from src.data.promotion_processor_commom import insert_forecast
from statsmodels.tsa.arima.model import ARIMA
import pandas as pd
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def train_arima_model(series: pd.Series, order=(1, 1, 1)):
    """
    Treina um modelo ARIMA para a série de preços fornecida.
    
    Args:
        series (pd.Series): Série temporal de preços.
        order (tuple): Ordem do modelo ARIMA (p, d, q).
        
    Returns:
        ARIMA: Modelo ARIMA treinado, ou None se falhar.
    """
    if len(series.dropna()) < 30:  # Requisito mínimo de pontos de dados
        logger.error("Insuficiente quantidade de dados para treinar ARIMA: " + str(len(series.dropna())))
        return None

    try:
        # Definir frequência se não estiver presente
        if series.index.freq is None:
            series.index.freq = 'D'  # Supõe-se diário se não especificado

        # Criação e treinamento do modelo ARIMA
        model = ARIMA(series.dropna(), order=order)
        model_fit = model.fit()
        logger.info(f"Modelo ARIMA treinado com sucesso. Ordem: {order}, Tamanho da Série: {len(series.dropna())}, AIC: {model_fit.aic}, BIC: {model_fit.bic}")
        return model_fit
    except Exception as e:
        logger.error(f"Erro ao ajustar o modelo ARIMA: {e}, Tamanho da Série: {len(series.dropna())}")
        return None

def forecast_price(model, row: pd.Series, steps=1):
    """
    Realiza uma previsão de preço usando o modelo ARIMA fornecido.
    Args:
        model (ARIMA): Modelo ARIMA treinado.
        row (pd.Series): Linha do DataFrame contendo os dados do produto.
        steps (int): Número de passos à frente para prever.
    Returns:
        float: Preço previsto, ou None se falhar.
    """
    try:
        if model is not None:
            forecast = model.forecast(steps=steps)
            predicted_price = forecast.iloc[0]
            insert_forecast(row['CodigoProduto'], row['Data'], row['ValorUnitario'], predicted_price, None)
            logger.info(f"Preço previsto (ARIMA): {predicted_price}")
            return predicted_price
        else:
            return None
    except Exception as e:
        logger.error(f"Erro ao fazer previsão com ARIMA: {e}")
        return None

def fill_missing_values(series: pd.Series, method='linear'):
    """
    Preenche os valores ausentes (NaN) na série temporal usando um método de interpolação.

    Args:
        series (pd.Series): Série temporal com valores ausentes.
        method (str): Método de interpolação a ser utilizado. Opções: 'linear', 'time', 'index', 'values', etc.

    Returns:
        pd.Series: Série temporal com os valores ausentes preenchidos.
    """
    try:
        filled_series = series.interpolate(method=method)
        logger.info(f"Valores ausentes preenchidos usando o método '{method}'.")
        return filled_series
    except Exception as e:
        logger.error(f"Erro ao preencher valores ausentes: {e}")
        return series