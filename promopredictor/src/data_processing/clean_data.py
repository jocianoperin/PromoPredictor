import os
import pandas as pd
import numpy as np
from workalendar.america import Brazil
from datetime import timedelta
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def clean_data(df):
    """
    Realiza a limpeza de dados brutos.

    Parâmetros:
        df (pandas.DataFrame): Dados brutos.

    Retorna:
        pandas.DataFrame: Dados limpos.
    """
    logger.info("Iniciando limpeza de dados.")

    # Converter a coluna 'Data' para datetime
    df['Data'] = pd.to_datetime(df['Data'], errors='coerce')

    # Remover linhas com 'Data' nula
    df = df.dropna(subset=['Data'])

    # Filtrar dados a partir de 01/01/2019
    df = df[df['Data'] >= pd.to_datetime('2019-01-01')]

    # Ajustar a coluna 'Hora' para remover "0 days"
    if 'Hora' in df.columns:
        df['Hora'] = df['Hora'].apply(lambda x: str(x).split(" ")[-1] if pd.notnull(x) else None)

    # Criar a coluna 'DataHora' combinando 'Data' e 'Hora'
    df['DataHora'] = pd.to_datetime(df['Data'].astype(str) + ' ' + df['Hora'], errors='coerce')

    # Remover linhas com 'DataHora' nula
    df = df.dropna(subset=['DataHora'])

    # Ajustar colunas binárias
    binary_cols = ['VendaCancelada', 'ItemCancelado', 'PrecoemPromocao']
    for col in binary_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)

    df['EmPromocao'] = df['PrecoemPromocao']
    df.fillna(0, inplace=True)

    logger.info("Limpeza de dados concluída.")
    return df

def feature_engineering(df):
    """
    Adiciona colunas derivadas aos dados limpos.

    Parâmetros:
        df (pandas.DataFrame): Dados limpos.

    Retorna:
        pandas.DataFrame: Dados com colunas derivadas.
    """
    logger.info("Iniciando engenharia de recursos.")

    # Variáveis temporais
    df['Dia'] = df['Data'].dt.day
    df['DiaDaSemana'] = df['Data'].dt.dayofweek
    df['Mes'] = df['Data'].dt.month

    # Quantidade líquida
    if 'Quantidade' in df.columns and 'QuantDevolvida' in df.columns:
        df['QuantidadeLiquida'] = df['Quantidade'] - df['QuantDevolvida']
        df['QuantidadeLiquida'] = df['QuantidadeLiquida'].fillna(0)

    # Rentabilidade
    if 'ValorCusto' in df.columns and 'ValorUnitario' in df.columns:
        df['Rentabilidade'] = ((df['ValorUnitario'] - df['ValorCusto']) / df['ValorCusto']).fillna(0)

    # Descontos e acréscimos aplicados
    df['DescontoAplicado'] = df['DescontoGeral'] + df['Desconto']
    df['AcrescimoAplicado'] = df['AcrescimoGeral'] + df['Acrescimo']

    logger.info("Engenharia de recursos concluída.")
    return df

def process_clean_data(produto_especifico, base_dir):
    """
    Lê dados brutos, limpa e realiza engenharia de recursos, salvando o resultado.

    Parâmetros:
        produto_especifico (int): Código do produto.
        base_dir (Path): Diretório base contendo os subdiretórios `raw` e `cleaned`.
    """
    raw_file_path = base_dir / "raw" / f'produto_{produto_especifico}.csv'
    cleaned_dir = base_dir / "cleaned"

    df = pd.read_csv(raw_file_path)

    df_clean = clean_data(df)
    df_processed = feature_engineering(df_clean)

    # Salva o dataset “normal” (por venda) para o modelo de quantidade:
    output_path = cleaned_dir / f'produto_{produto_especifico}_clean.csv'
    df_processed.to_csv(output_path, index=False, sep=',')
    logger.info(f"Dados processados salvos em {output_path}.")

if __name__ == "__main__":
    process_clean_data(26173)