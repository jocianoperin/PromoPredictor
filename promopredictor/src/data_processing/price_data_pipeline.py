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
        # e por aí vai...
    }).reset_index()

    # Renomear
    grouped.rename(columns={'ValorUnitarioMedioItem': 'ValorUnitarioMedio'}, inplace=True)

    # Converte a 'Data' de volta pra datetime
    grouped['Data'] = pd.to_datetime(grouped['Data'])

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

    # (Opcional) Calcular algo como LogValorUnitarioMedio
    # Assim você pode treinar a rede pra prever log em vez de valor bruto.
    df['LogValorUnitarioMedio'] = np.log1p(df['ValorUnitarioMedio'].replace(0, np.nan).abs())

    logger.info("Engenharia de recursos para preço concluída.")
    return df

def save_price_dataset(df: pd.DataFrame, output_file: Path):
    """
    Salva o df final (diário, com features) para uso no modelo de valor unitário.
    """
    df.to_csv(output_file, index=False)
    logger.info(f"Dataset de valor unitário salvo em {output_file}")

def run_price_pipeline(raw_file_path: Path, output_file_path: Path):
    """
    Roda todo o fluxo: carrega dados brutos -> limpa -> agrega -> feature eng. -> salva CSV final.
    """
    df = load_raw_data(raw_file_path)
    df_clean = clean_data_for_price(df)
    df_daily = aggregate_daily(df_clean)
    df_feats = feature_engineering_for_price(df_daily)
    save_price_dataset(df_feats, output_file_path)
