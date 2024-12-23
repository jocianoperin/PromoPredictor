import pandas as pd

def add_rolling_features(df, columns, window_size=7):
    """
    Adiciona variáveis de janela flutuante ao DataFrame.

    Args:
        df (pd.DataFrame): DataFrame com os dados.
        columns (list): Lista de colunas alvo para calcular as estatísticas.
        window_size (int): Tamanho da janela deslizante.

    Returns:
        pd.DataFrame: DataFrame com novas colunas baseadas em janelas.
    """
    for column in columns:
        df[f'{column}_rolling_mean_{window_size}'] = df[column].rolling(window=window_size).mean()
        df[f'{column}_rolling_std_{window_size}'] = df[column].rolling(window=window_size).std()
        df[f'{column}_rolling_sum_{window_size}'] = df[column].rolling(window=window_size).sum()
    return df
