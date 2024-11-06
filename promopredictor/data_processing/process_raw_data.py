import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from workalendar.america import Brazil
from datetime import timedelta

def create_db_connection():
    try:
        engine = create_engine('mysql+mysqlconnector://root:123@localhost/ubialli')
        print('Conexão com o banco de dados estabelecida.')
        return engine
    except Exception as e:
        print(f'Erro ao conectar ao banco de dados: {e}')
        return None

def extract_raw_data(connection, produto_especifico):
    query = """
    SELECT
        vp.CodigoVenda, v.Data, v.Hora, v.Status, v.Cancelada AS VendaCancelada, v.TotalPedido, v.DescontoGeral, v.AcrescimoGeral, v.TotalCusto, vp.CodigoProduto,
        vp.Quantidade, vp.ValorUnitario, vp.ValorTotal, vp.Desconto, vp.Acrescimo, vp.Cancelada AS ItemCancelado, vp.QuantDevolvida, vp.PrecoemPromocao, vp.CodigoSecao,
        vp.CodigoGrupo, vp.CodigoSubGrupo, vp.CodigoFabricante, vp.ValorCusto, vp.ValorCustoGerencial, vp.CodigoFornecedor, vp.CodigoKitPrincipal, vp.ValorKitPrincipal
    FROM vendasprodutos vp INNER JOIN vendas v ON vp.CodigoVenda = v.Codigo WHERE vp.CodigoProduto = %(produto_especifico)s AND v.Status IN ('f', 'x')
    """
    df = pd.read_sql(query, connection, params={'produto_especifico': produto_especifico})
    return df

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
            df[col].fillna(0, inplace=True)

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
    cols_4_decimals = ['DescontoGeral', 'AcrescimoGeral', 'Desconto', 'Acrescimo',
                       'Rentabilidade', 'DescontoAplicado', 'AcrescimoAplicado']
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
        'ValorKitPrincipal', 'DiaDaSemana', 'Mes', 'Dia', 'Feriado', 'VesperaDeFeriado',
        'EmPromocao', 'Rentabilidade', 'DescontoAplicado', 'AcrescimoAplicado',
        'QuantidadeLiquida'
    ]

    return df[[col for col in cols_to_keep if col in df.columns]]

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

    # Rentabilidade
    if 'ValorCusto' in df.columns and 'ValorUnitario' in df.columns:
        df['Rentabilidade'] = ((df['ValorUnitario'] - df['ValorCusto']) / df['ValorCusto']).astype(float).round(4)
        df['Rentabilidade'].replace([np.inf, -np.inf], np.nan, inplace=True)
        df['Rentabilidade'].fillna(0, inplace=True)

    # Desconto e acréscimo aplicados
    if 'DescontoGeral' in df.columns and 'Desconto' in df.columns:
        df['DescontoAplicado'] = df['DescontoGeral'] + df['Desconto']
    else:
        df['DescontoAplicado'] = 0

    if 'AcrescimoGeral' in df.columns and 'Acrescimo' in df.columns:
        df['AcrescimoAplicado'] = df['AcrescimoGeral'] + df['Acrescimo']
    else:
        df['AcrescimoAplicado'] = 0

    # Garantir que 'DescontoAplicado' e 'AcrescimoAplicado' não tenham valores nulos
    df['DescontoAplicado'] = df['DescontoAplicado'].fillna(0).astype(float).round(4)
    df['AcrescimoAplicado'] = df['AcrescimoAplicado'].fillna(0).astype(float).round(4)

    # Quantidade líquida
    if 'Quantidade' in df.columns and 'QuantDevolvida' in df.columns:
        df['QuantidadeLiquida'] = df['Quantidade'] - df['QuantDevolvida']
        df['QuantidadeLiquida'].fillna(df['Quantidade'], inplace=True)
    else:
        df['QuantidadeLiquida'] = df['Quantidade']

    return df

def is_feriado_prolongado(date, calendar):
    if calendar.is_holiday(date + timedelta(days=1)):
        return True
    elif calendar.is_holiday(date + timedelta(days=3)) and date.weekday() == 4:
        return True
    elif calendar.is_holiday(date + timedelta(days=2)) and date.weekday() == 3:
        return True
    return False

def main():
    connection = create_db_connection()
    produto_especifico = 26173

    if connection:
        df_raw = extract_raw_data(connection, produto_especifico)
        connection.dispose()
        print('Dados brutos extraídos com sucesso.')

        df_cleaned = clean_data(df_raw)
        df_processed = feature_engineering(df_cleaned)

        # Criar o diretório 'data' se não existir
        os.makedirs('data', exist_ok=True)

        # Salvar o dataframe processado em um arquivo CSV
        df_processed.to_csv('data/dados_processados.csv', index=False, sep=',')
        print('Dados processados salvos em data/dados_processados.csv.')

        # Verificar se há valores nulos
        print("Valores nulos por coluna:")
        print(df_processed.isnull().sum())

        # Exibir as primeiras linhas do DataFrame com as colunas
        print("Primeiras linhas do DataFrame:")
        print(df_processed.head())

if __name__ == "__main__":
    main()
