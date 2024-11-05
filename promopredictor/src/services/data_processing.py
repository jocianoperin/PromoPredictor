# src/services/data_processing.py

import os
import pandas as pd
import numpy as np
from datetime import timedelta
from workalendar.america import Brazil
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def extract_raw_data(produto_especifico):
    query = """
    SELECT
        vp.CodigoVenda, v.Data, v.Hora, v.Status, v.Cancelada AS VendaCancelada, v.TotalPedido,
        v.DescontoGeral, v.AcrescimoGeral, v.TotalCusto, vp.CodigoProduto,
        vp.Quantidade, vp.ValorUnitario, vp.ValorTotal, vp.Desconto, vp.Acrescimo,
        vp.Cancelada AS ItemCancelado, vp.QuantDevolvida, vp.PrecoemPromocao, vp.CodigoSecao,
        vp.CodigoGrupo, vp.CodigoSubGrupo, vp.CodigoFabricante, vp.ValorCusto,
        vp.ValorCustoGerencial, vp.CodigoFornecedor, vp.CodigoKitPrincipal, vp.ValorKitPrincipal
    FROM vendasprodutos vp
    INNER JOIN vendas v ON vp.CodigoVenda = v.Codigo
    WHERE vp.CodigoProduto = :produto_especifico AND v.Status IN ('f', 'x')
    """
    try:
        result = db_manager.execute_query(query, params={'produto_especifico': produto_especifico})
        if 'data' in result and 'columns' in result:
            df = pd.DataFrame(result['data'], columns=result['columns'])
            logger.info(f"Dados brutos extraídos com sucesso para o produto {produto_especifico}.")
            return df
        else:
            logger.error(f"Erro ao extrair dados brutos para o produto {produto_especifico}.")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Erro ao extrair dados para o produto {produto_especifico}: {e}")
        return pd.DataFrame()

def clean_data(df):
    if 'Data' not in df.columns or 'Hora' not in df.columns:
        raise KeyError("'Data' ou 'Hora' não está presente no DataFrame.")

    # Converter a coluna 'Data' para datetime
    df['Data'] = pd.to_datetime(df['Data'], errors='coerce')

    # Remover linhas com 'Data' nula
    df = df.dropna(subset=['Data'])

    # Filtrar dados a partir de 01/01/2019
    df = df[df['Data'] >= pd.to_datetime('2019-01-01')]

    # Verificar o tipo da coluna 'Hora'
    if np.issubdtype(df['Hora'].dtype, np.number):
        # Se 'Hora' é numérica, assumimos que representa segundos e convertemos para timedelta
        df['Hora'] = pd.to_timedelta(df['Hora'], unit='s')
    elif np.issubdtype(df['Hora'].dtype, np.timedelta64):
        # Se 'Hora' já é timedelta, não fazemos nada
        pass
    else:
        # Se 'Hora' é string, convertemos para timedelta
        df['Hora'] = pd.to_timedelta(df['Hora'])

    # Criar a coluna 'DataHora'
    df['DataHora'] = df['Data'] + df['Hora']

    # Ajustar colunas para 1 ou 0
    df['VendaCancelada'] = df['VendaCancelada'].fillna(0).apply(lambda x: 1 if x == 1 else 0)
    df['ItemCancelado'] = df['ItemCancelado'].fillna(0).apply(lambda x: 1 if x == 1 else 0)

    # Preencher colunas numéricas com 0 onde houver valores nulos
    numeric_cols = ['DescontoGeral', 'AcrescimoGeral', 'Desconto', 'Acrescimo', 'PrecoemPromocao']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    # Garantir que 'PrecoemPromocao' contenha apenas 0 ou 1
    if 'PrecoemPromocao' in df.columns:
        df['PrecoemPromocao'] = df['PrecoemPromocao'].apply(lambda x: 1 if x == 1 else 0)
    else:
        df['PrecoemPromocao'] = 0

    # Adicionar 'EmPromocao' se não existir e ajustar valores
    df['EmPromocao'] = df['PrecoemPromocao']

    # Forçar 2 casas decimais para valores monetários
    cols_2_decimals = ['TotalPedido', 'TotalCusto', 'ValorUnitario', 'ValorTotal',
                       'ValorCusto', 'ValorCustoGerencial']
    for col in cols_2_decimals:
        if col in df.columns:
            df[col] = df[col].round(2)

    # Forçar 4 casas decimais para percentuais e descontos
    cols_4_decimals = ['DescontoGeral', 'AcrescimoGeral', 'Desconto', 'Acrescimo']
    for col in cols_4_decimals:
        if col in df.columns:
            df[col] = df[col].astype(float).round(4)

    # Substituir valores nulos em colunas numéricas com 0 e converter para int
    cols_fill_zeros = ['QuantDevolvida', 'CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo',
                       'CodigoFabricante', 'CodigoFornecedor', 'CodigoKitPrincipal', 'ValorKitPrincipal']
    for col in cols_fill_zeros:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)

    # Incluir 'Data' em cols_to_keep
    cols_to_keep = [
        'CodigoVenda', 'Data', 'DataHora', 'Status', 'VendaCancelada', 'TotalPedido',
        'DescontoGeral', 'AcrescimoGeral', 'TotalCusto', 'CodigoProduto',
        'Quantidade', 'ValorUnitario', 'ValorTotal', 'Desconto', 'Acrescimo',
        'ItemCancelado', 'QuantDevolvida', 'PrecoemPromocao', 'CodigoSecao',
        'CodigoGrupo', 'CodigoSubGrupo', 'CodigoFabricante', 'ValorCusto',
        'ValorCustoGerencial', 'CodigoFornecedor', 'CodigoKitPrincipal',
        'ValorKitPrincipal', 'EmPromocao'
    ]

    df = df[[col for col in cols_to_keep if col in df.columns]]

    return df

def save_transaction_data(df, produto_especifico, data_dir):
    # Salvar dados no nível de transação
    df.to_csv(os.path.join(data_dir, f'dados_transacao_{produto_especifico}.csv'), index=False, sep=',')
    logger.info(f'Dados de transação salvos em data/dados_transacao_{produto_especifico}.csv.')

def aggregate_data(df, produto_especifico):
    # Agrupar por Data e somar as quantidades e valores
    df_grouped = df.groupby('Data').agg({
        'CodigoVenda': 'nunique',
        'Quantidade': 'sum',
        'ValorTotal': 'sum',
        'Desconto': 'sum',
        'Acrescimo': 'sum',
        'QuantDevolvida': 'sum',
        'ValorCusto': 'sum',
        'ValorCustoGerencial': 'sum',
        'TotalCusto': 'sum',
        'DescontoGeral': 'sum',
        'AcrescimoGeral': 'sum',
        'TotalPedido': 'sum',
        'VendaCancelada': 'sum',
        'ItemCancelado': 'sum',
        'PrecoemPromocao': 'max',  # Se houve promoção no dia, marca como 1
        'EmPromocao': 'max',
        'CodigoSecao': 'first',
        'CodigoGrupo': 'first',
        'CodigoSubGrupo': 'first',
        'CodigoFabricante': 'first',
        'CodigoFornecedor': 'first',
        'CodigoKitPrincipal': 'first',
        'ValorKitPrincipal': 'sum',
        'ValorUnitario': 'mean',  # ValorUnitario médio no dia
        'ValorCusto': 'mean',
        'ValorCustoGerencial': 'mean',
    }).reset_index()

    # Incluir dias sem vendas
    df_complete = create_complete_date_range(df_grouped, produto_especifico)

    return df_complete

def create_complete_date_range(df, produto_especifico):
    # Criar um DataFrame com todas as datas no intervalo
    start_date = df['Data'].min()
    end_date = df['Data'].max()
    all_dates = pd.DataFrame({'Data': pd.date_range(start=start_date, end=end_date)})

    # Mesclar com o DataFrame original
    df_complete = all_dates.merge(df, on='Data', how='left')

    # Preencher valores nulos com zeros ou valores padrão
    numeric_cols = ['CodigoVenda', 'Quantidade', 'ValorTotal', 'Desconto', 'Acrescimo',
                    'QuantDevolvida', 'ValorCusto', 'ValorCustoGerencial', 'TotalCusto',
                    'DescontoGeral', 'AcrescimoGeral', 'TotalPedido', 'VendaCancelada',
                    'ItemCancelado', 'PrecoemPromocao', 'EmPromocao', 'ValorKitPrincipal',
                    'ValorUnitario']
    for col in numeric_cols:
        df_complete[col] = df_complete[col].fillna(0)

    # Preencher colunas categóricas com o valor mais frequente ou um valor padrão
    categorical_cols = ['CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo', 'CodigoFabricante',
                        'CodigoFornecedor', 'CodigoKitPrincipal']
    for col in categorical_cols:
        df_complete[col] = df_complete[col].fillna(method='ffill').fillna(method='bfill').fillna(0).astype(int)

    # Adicionar a coluna 'CodigoProduto'
    df_complete['CodigoProduto'] = produto_especifico

    return df_complete

def feature_engineering(df):
    if 'Data' not in df.columns:
        raise KeyError("'Data' não está presente no DataFrame.")

    # Configurar o calendário de feriados do Brasil
    cal = Brazil()

    # Variáveis temporais
    df['DiaDaSemana'] = df['Data'].dt.dayofweek
    df['Mes'] = df['Data'].dt.month
    df['Dia'] = df['Data'].dt.day

    # Marcar feriados
    df['Feriado'] = df['Data'].apply(lambda x: 1 if cal.is_holiday(x) else 0)

    # Marcar vésperas de feriado prolongado
    df['VesperaDeFeriado'] = df['Data'].apply(lambda x: 1 if is_feriado_prolongado(x, cal) else 0)

    # Rentabilidade média do dia
    if 'ValorUnitario' in df.columns and 'ValorCusto' in df.columns:
        df['Rentabilidade'] = ((df['ValorUnitario'] - df['ValorCusto']) / df['ValorCusto']).astype(float).round(4)
        df['Rentabilidade'].replace([np.inf, -np.inf], np.nan, inplace=True)
        df['Rentabilidade'].fillna(0, inplace=True)
    else:
        df['Rentabilidade'] = 0.0

    # Desconto e acréscimo aplicados totais do dia
    df['DescontoAplicado'] = df['DescontoGeral'] + df['Desconto']
    df['AcrescimoAplicado'] = df['AcrescimoGeral'] + df['Acrescimo']

    # Garantir que 'DescontoAplicado' e 'AcrescimoAplicado' não tenham valores nulos
    df['DescontoAplicado'] = df['DescontoAplicado'].fillna(0).astype(float).round(4)
    df['AcrescimoAplicado'] = df['AcrescimoAplicado'].fillna(0).astype(float).round(4)

    # Quantidade líquida total do dia
    df['QuantidadeLiquida'] = df['Quantidade'] - df['QuantDevolvida']
    df['QuantidadeLiquida'].fillna(df['Quantidade'], inplace=True)

    # Reordenar as colunas
    cols_order = [
        'Data', 'CodigoProduto', 'CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo',
        'CodigoFabricante', 'CodigoFornecedor', 'CodigoKitPrincipal', 'CodigoVenda',
        'Quantidade', 'QuantidadeLiquida', 'QuantDevolvida', 'ValorUnitario',
        'ValorTotal', 'Desconto', 'Acrescimo', 'DescontoGeral', 'AcrescimoGeral',
        'DescontoAplicado', 'AcrescimoAplicado', 'TotalPedido', 'ValorCusto',
        'ValorCustoGerencial', 'TotalCusto', 'Rentabilidade', 'ValorKitPrincipal',
        'VendaCancelada', 'ItemCancelado', 'PrecoemPromocao', 'EmPromocao',
        'DiaDaSemana', 'Mes', 'Dia', 'Feriado', 'VesperaDeFeriado'
    ]

    df = df[[col for col in cols_order if col in df.columns]]

    return df

def save_daily_data(df, produto_especifico, data_dir):
    # Salvar dados agregados por dia
    df.to_csv(os.path.join(data_dir, f'dados_agrupados_{produto_especifico}.csv'), index=False, sep=',')
    logger.info(f'Dados diários salvos em data/dados_agrupados_{produto_especifico}.csv.')

def is_feriado_prolongado(date, calendar):
    date = pd.to_datetime(date)
    if calendar.is_holiday(date + timedelta(days=1)):
        return True
    elif calendar.is_holiday(date + timedelta(days=3)) and date.weekday() == 4:
        return True
    elif calendar.is_holiday(date + timedelta(days=2)) and date.weekday() == 3:
        return True
    return False

def feature_engineering_transacao(df):
    if 'DataHora' not in df.columns:
        raise KeyError("'DataHora' não está presente no DataFrame.")

    # Configurar o calendário de feriados do Brasil
    cal = Brazil()

    # Variáveis temporais
    df['DiaDaSemana'] = df['DataHora'].dt.dayofweek
    df['Mes'] = df['DataHora'].dt.month
    df['Dia'] = df['DataHora'].dt.day

    # Marcar feriados
    df['Feriado'] = df['DataHora'].dt.date.apply(lambda x: 1 if cal.is_holiday(x) else 0)

    # Marcar vésperas de feriado prolongado
    df['VesperaDeFeriado'] = df['DataHora'].dt.date.apply(lambda x: 1 if is_feriado_prolongado(x, cal) else 0)

    return df