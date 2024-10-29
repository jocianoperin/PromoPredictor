import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def create_sequences(data, seq_length, target_cols):
    """
    Cria sequências de dados para modelos sequenciais.

    :param data: DataFrame com os dados.
    :param seq_length: Comprimento da sequência (número de timesteps).
    :param target_cols: Lista com os nomes das colunas de alvo (targets).
    :return: Arrays numpy para X e y.
    """
    X = []
    y = {col: [] for col in target_cols}

    for i in range(len(data) - seq_length):
        X.append(data[i:i+seq_length, :])
        for col in target_cols:
            col_idx = target_cols.index(col)
            y[col].append(data[i+seq_length, col_idx])

    X = np.array(X)
    y = {col: np.array(y[col]) for col in target_cols}
    return X, y

def scale_data(df, feature_range=(0, 1)):
    """
    Normaliza os dados usando MinMaxScaler.

    :param df: DataFrame com os dados.
    :param feature_range: Tupla indicando o range para a normalização.
    :return: Dados normalizados e o scaler ajustado.
    """
    scaler = MinMaxScaler(feature_range=feature_range)
    data_scaled = scaler.fit_transform(df)
    return data_scaled, scaler
