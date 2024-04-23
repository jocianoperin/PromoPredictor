from src.data.promotion_processor import insert_forecast
import numpy as np
import pandas as pd
from keras._tf_keras.keras.models import Sequential
from keras._tf_keras.keras.layers import LSTM, Dense
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def train_rnn_model(series: pd.Series, window_size=30, lstm_units=64, dense_units=32):
    """
    Treina um modelo RNN (LSTM) para a série de preços fornecida.
    Args:
        series (pd.Series): Série temporal de preços.
        window_size (int): Tamanho da janela deslizante para criar os exemplos de treinamento.
        lstm_units (int): Número de unidades LSTM na camada LSTM.
        dense_units (int): Número de unidades na camada densa.
    Returns:
        Modelo RNN treinado, ou None se falhar.
    """
    try:
        # Preparar os dados para o modelo RNN
        data = series.values.reshape(-1, 1)
        X_train, y_train = prepare_data(data, window_size)

        # Criar o modelo RNN
        model = Sequential()
        model.add(LSTM(lstm_units, input_shape=(window_size, 1)))
        model.add(Dense(dense_units))
        model.add(Dense(1))
        model.compile(optimizer='adam', loss='mse')

        # Treinar o modelo RNN
        model.fit(X_train, y_train, epochs=100, batch_size=32, verbose=0)
        logger.info(f"Modelo RNN treinado com sucesso. Tamanho da Série: {len(series)}, Window Size: {window_size}, LSTM Units: {lstm_units}, Dense Units: {dense_units}")
        return model
    except Exception as e:
        logger.error(f"Erro ao treinar o modelo RNN: {e}")
        return None

def prepare_data(data, window_size):
    """
    Prepara os dados para o treinamento do modelo RNN.
    Args:
        data (numpy.ndarray): Dados de série temporal.
        window_size (int): Tamanho da janela deslizante.
    Returns:
        X_train (numpy.ndarray): Exemplos de entrada para o treinamento.
        y_train (numpy.ndarray): Rótulos de saída para o treinamento.
    """
    X_train, y_train = [], []
    for i in range(window_size, len(data)):
        X_train.append(data[i - window_size:i, 0])
        y_train.append(data[i, 0])
    X_train, y_train = np.array(X_train), np.array(y_train)
    X_train = np.reshape(X_train, (X_train.shape[0], X_train.shape[1], 1))
    return X_train, y_train

def forecast_price(model, row: pd.Series, window_size):
    """
    Realiza uma previsão de preço usando o modelo RNN fornecido.
    Args:
        model: Modelo RNN treinado.
        row (pd.Series): Linha do DataFrame contendo os dados do produto.
        window_size (int): Tamanho da janela deslizante.
    Returns:
        float: Preço previsto, ou None se falhar.
    """
    try:
        input_data = row['ValorUnitario'].values[-window_size:].reshape((1, window_size, 1))
        forecast = model.predict(input_data)
        predicted_price = forecast[0, 0]
        insert_forecast(row['CodigoProduto'], row['Data'], row['ValorUnitario'], None, predicted_price)
        logger.info(f"Preço previsto (RNN): {predicted_price}")
        return predicted_price
    except Exception as e:
        logger.error(f"Erro ao fazer previsão com RNN: {e}")
        return None