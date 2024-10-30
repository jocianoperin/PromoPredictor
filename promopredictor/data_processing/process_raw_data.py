# process_raw_data.py

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import holidays

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
    # Remover vendas canceladas
    df = df[df['VendaCancelada'] != 1]
    # Remover itens cancelados
    df = df[df['ItemCancelado'] != 1]
    # Remover registros com quantidade <= 0
    df = df[df['Quantidade'] > 0]
    # Remover registros com ValorTotal <= 0
    df = df[df['ValorTotal'] > 0]
    # Remover registros com TotalPedido <= 0
    df = df[df['TotalPedido'] > 0]

    # Remover registros com valores nulos em campos críticos
    campos_criticos = ['Data', 'CodigoProduto', 'Quantidade', 'ValorTotal']
    df.dropna(subset=campos_criticos, inplace=True)

    # Converter tipos de dados
    df['Data'] = pd.to_datetime(df['Data'])

    # Ajuste na conversão da coluna 'Hora'
    df['Hora'] = df['Hora'].apply(lambda x: (pd.Timestamp('1900-01-01') + x).time())

    campos_numericos = ['Quantidade', 'ValorUnitario', 'ValorTotal', 'Desconto', 'Acrescimo', 'ValorCusto']
    df[campos_numericos] = df[campos_numericos].astype(float)
    return df

def feature_engineering(df):
    # Variáveis temporais
    df['DiaDaSemana'] = df['Data'].dt.dayofweek
    df['Mes'] = df['Data'].dt.month
    df['Dia'] = df['Data'].dt.day
    # Feriados
    br_holidays = holidays.Brazil()
    df['Feriado'] = df['Data'].apply(lambda x: 1 if x in br_holidays else 0)
    # Promoção
    df['EmPromocao'] = df['PrecoemPromocao']
    # Rentabilidade
    df['Rentabilidade'] = (df['ValorUnitario'] - df['ValorCusto']) / df['ValorCusto']
    df['Rentabilidade'] = df['Rentabilidade'].replace([np.inf, -np.inf], np.nan)
    df['Rentabilidade'] = df['Rentabilidade'].fillna(0)
    # Desconto e acréscimo aplicados
    df['DescontoAplicado'] = df['DescontoGeral'] + df['Desconto']
    df['AcrescimoAplicado'] = df['AcrescimoGeral'] + df['Acrescimo']
    # Quantidade líquida
    df['QuantidadeLiquida'] = df['Quantidade'] - df['QuantDevolvida']
    df['QuantidadeLiquida'] = df['QuantidadeLiquida'].fillna(df['Quantidade'])
    return df

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
        df_processed.to_csv('../data/dados_processados.csv', index=False, sep=',')
        print('Dados processados salvos em data/dados_processados.csv.')

        # Verificar se há valores nulos
        print("Valores nulos por coluna:")
        print(df_processed.isnull().sum())

        # Exibir as primeiras linhas do DataFrame com as colunas
        print("Primeiras linhas do DataFrame:")
        print(df_processed.head())

if __name__ == "__main__":
    main()
