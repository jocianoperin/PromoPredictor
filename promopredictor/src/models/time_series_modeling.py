from statsmodels.tsa.arima.model import ARIMA
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger
import pandas as pd

logger = get_logger(__name__)

def train_arima_model(series: pd.Series, order=(1,1,1)):
    """
    Treina um modelo ARIMA para a série de preços fornecida.
    
    Args:
        series (pd.Series): Série temporal de preços.
        order (tuple): Ordem do modelo ARIMA (p, d, q).
        
    Returns:
        ARIMA: Modelo ARIMA treinado.
    """
    try:
        model = ARIMA(series.dropna(), order=order)  # Garante que NaNs sejam removidos
        model_fit = model.fit()
        return model_fit
    except ValueError as e:
        logger.error(f"Erro ao ajustar o modelo ARIMA: {e}")
        return None

def forecast_price(model, steps=1):
    """
    Realiza uma previsão de preço usando o modelo ARIMA fornecido.
    
    Args:
        model (ARIMA): Modelo ARIMA treinado.
        steps (int): Número de passos à frente para prever.
        
    Returns:
        float: Preço previsto.
    """
    if model is not None:
        forecast = model.forecast(steps=steps)
        return forecast[-1]
    else:
        return None

