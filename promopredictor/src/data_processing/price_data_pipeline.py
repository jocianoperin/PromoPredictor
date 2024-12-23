from workalendar.america import Brazil
import pandas as pd
import numpy as np
from pathlib import Path
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def load_raw_data(file_path: Path) -> pd.DataFrame:
    """
    Lê o CSV de dados brutos de vendas para um produto específico.
    """
    logger.info(f"Lendo dados brutos de {file_path}")
    df = pd.read_csv(file_path, parse_dates=['Data'])
    return df

def clean_data_for_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    Faz uma limpeza básica de dados para previsão de valor unitário.
    - Remove linhas com data nula
    - Ajusta colunas, se necessário
    """
    logger.info("Iniciando limpeza de dados para preço.")

    # Remover datas nulas (se tiver)
    df = df.dropna(subset=['Data'])

    # Ajustar colunas, ex: se Hora ainda tiver '0 days'
    if 'Hora' in df.columns:
        df['Hora'] = df['Hora'].apply(lambda x: str(x).split(" ")[-1] if pd.notnull(x) else None)

    # Converter para datetime a coluna 'DataHora'
    df['DataHora'] = pd.to_datetime(df['Data'].astype(str) + ' ' + df['Hora'], errors='coerce')
    df = df.dropna(subset=['DataHora'])

    # Exemplo: remover vendas canceladas (opcional)
    # df = df[df['VendaCancelada'] == 0]
    # df = df[df['ItemCancelado'] == 0]

    # Aqui você pode filtrar datas mais antigas, etc.
    df = df[df['Data'] >= pd.to_datetime('2019-01-01')]

    # Preencher NaN para colunas importantes
    df['ValorUnitario'] = df['ValorUnitario'].fillna(0)

    logger.info("Limpeza de dados para preço concluída.")
    return df

def aggregate_daily(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa o dataframe por DIA (df['Data']), calculando o valor médio unitário.
    """
    logger.info("Iniciando agregação diária para valor unitário.")

    # EXEMPLO de valor médio unitário: soma(ValorTotal) / soma(QuantidadeLiquida) 
    # ou simplesmente 'mean(ValorUnitario)'. Depende de qual faz mais sentido para você.

    # Antes, criar QuantidadeLiquida se precisar
    if 'Quantidade' in df.columns and 'QuantDevolvida' in df.columns:
        df['QuantidadeLiquida'] = df['Quantidade'] - df['QuantDevolvida']
    else:
        df['QuantidadeLiquida'] = df['Quantidade']

    df['QuantidadeLiquida'] = df['QuantidadeLiquida'].fillna(0)

    # Valor médio unitário: 
    # (Opcional) se você quiser uma média simples:
    #   ValorUnitarioMedio = mean(ValorUnitario)
    # (Recomendado) se quiser algo mais realista:
    #   ValorUnitarioMedio = soma(ValorTotal) / soma(QuantidadeLiquida)
    df['ValorTotal'] = df['ValorTotal'].fillna(0)
    df['ValorUnitarioMedioItem'] = df['ValorTotal'] / df['QuantidadeLiquida'].replace(0, np.nan)

    grouped = df.groupby(df['Data'].dt.date).agg({
        'ValorUnitarioMedioItem': 'mean',   # média do item do dia
        'QuantidadeLiquida': 'sum',
        #'Rentabilidade': 'mean',           # se existir
        'DescontoGeral': 'mean',           # ou DescontoAplicado etc.
        'AcrescimoGeral': 'mean',
        'PrecoemPromocao': 'max',
        'ValorCusto': 'mean',
    }).reset_index()

    # Renomear
    grouped.rename(columns={'ValorUnitarioMedioItem': 'ValorUnitarioMedio'}, inplace=True)

    # Converte a 'Data' de volta pra datetime
    grouped['Data'] = pd.to_datetime(grouped['Data'])
    
    df_2024 = df[df['Data'].dt.year == 2024]
    #print("Linhas 2024 antes de agrupar:\n", df_2024[['Data', 'QuantidadeLiquida', 'ValorTotal']].head(50))

    logger.info("Agregação diária concluída.")
    return grouped

def feature_engineering_for_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extrai variáveis de Data (dia, mês, dia da semana, etc.).
    Aplica transformações, se necessário.
    """
    logger.info("Iniciando engenharia de recursos para preço.")

    # Criar colunas de dia, mes, diaDaSemana
    df['Dia'] = df['Data'].dt.day
    df['Mes'] = df['Data'].dt.month
    df['DiaDaSemana'] = df['Data'].dt.dayofweek

    df = add_holiday_features(df)

    # (Opcional) Calcular algo como LogValorUnitarioMedio
    # Assim você pode treinar a rede pra prever log em vez de valor bruto.
    df['LogValorUnitarioMedio'] = np.log1p(df['ValorUnitarioMedio'].replace(0, np.nan).abs())

    df['ValorCusto'] = df['ValorCusto'].fillna(0)  # Preencher valores ausentes com 0

    logger.info("Engenharia de recursos para preço concluída.")
    return df

def save_price_dataset(df: pd.DataFrame, output_file: Path):
    """
    Salva o df final (diário, com features) para uso no modelo de valor unitário.
    """
    if 'ValorCusto' not in df.columns:
        logger.warning("A coluna 'ValorCusto' não está presente no dataset. Verifique o pipeline.")

    df.to_csv(output_file, index=False)
    logger.info(f"Dataset de valor unitário salvo em {output_file}")

def run_price_pipeline(raw_file_path: Path, output_file_path: Path):
    """
    Roda todo o fluxo: carrega dados brutos -> imprime pré-limpeza -> limpa -> agrega -> feature eng. -> salva CSV final.
    """
    df = load_raw_data(raw_file_path)
    
    # >>>>>> AQUI você imprime ou loga o DataFrame (ou parte dele) <<<<<<
    #logger.info("### DUMP DE DADOS ANTES DO CLEANING ###")
    # Se for um DataFrame grande, imprima apenas as primeiras linhas
    #logger.info("\n" + df.head(50).to_string())  
    
    df_zeros = df[(df['Data'] >= '2019-01-01') & (df['Quantidade'] == 0)]
    #print("Linhas >= 2019 com Quantidade=0:\n", df_zeros[['Data','Quantidade','ValorTotal','VendaCancelada','ItemCancelado']].head(50))

    # Agora segue o pipeline normal
    df_clean = clean_data_for_price(df)
    df_daily = aggregate_daily(df_clean)
    df_feats = feature_engineering_for_price(df_daily)
    save_price_dataset(df_feats, output_file_path)

def add_holiday_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona colunas indicando se o dia é feriado e se é véspera de feriado (1, 2 ou 3 dias antes).
    """
    cal = Brazil()  # Calendário do Brasil

    # Se quiser focar só em feriados nacionais, use `Brazil()`.
    # Se quiser feriados estaduais, existem classes específicas (ex. BrazilAcre, BrazilSaoPaulo, etc.)

    # Converter 'Data' para datetime só pra garantir
    df['Data'] = pd.to_datetime(df['Data'])

    # Criar as colunas "is_holiday", "is_eve1", "is_eve2", "is_eve3"
    df['is_holiday'] = df['Data'].apply(lambda d: 1 if cal.is_working_day(d) == False and cal.is_working_day(d + pd.Timedelta(1, 'D')) == True else 1 if d in cal.holidays(d.year) else 0)
    # A linha acima é só um exemplo; se "is_working_day(d) == False" significa não é dia útil, mas no Brasil, véspera de feriado pode requerer lógica diferente.
    # Melhor é simplesmente checkar "d in feriados_do_ano" e "d +1 in feriados_do_ano", etc.
    # Vamos criar uma abordagem mais simples:

    # 1) Obter todos os feriados para cada ano do DF
    #     -> cal.holidays(year) retorna lista de tuplas (data, nome).
    # 2) Criar um set de datas para lookup rápido.
    
    df_holidays = set()
    for year in df['Data'].dt.year.unique():
        feriados_ano = cal.holidays(year)
        # feriados_ano é algo como [(datetime(2024, 1, 1), 'Confraternização Universal'), ...]
        # Precisamos só da data
        for (dt, nome) in feriados_ano:
            df_holidays.add(dt)

    # Agora sim, definimos a função que verifica se d está em df_holidays
    def is_feriado(d):
        return 1 if d in df_holidays else 0

    df['is_holiday'] = df['Data'].apply(is_feriado)

    # Para "véspera de feriado", checamos se d+1 ou d+2 ou d+3 está em df_holidays
    def is_eve_of_holiday(d, delta):
        return 1 if (d + pd.Timedelta(delta, unit='D')) in df_holidays else 0

    df['is_eve1'] = df['Data'].apply(lambda d: is_eve_of_holiday(d, 1))
    df['is_eve2'] = df['Data'].apply(lambda d: is_eve_of_holiday(d, 2))
    df['is_eve3'] = df['Data'].apply(lambda d: is_eve_of_holiday(d, 3))

    return df