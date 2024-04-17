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
    try:
        # Certifique-se de que a série tem um índice de data com frequência especificada
        if series.index.freq is None:
            series.index.freq = 'D'  # Supõe-se diário se não especificado

        model = ARIMA(series.dropna(), order=order)
        model_fit = model.fit()
        return model_fit
    except Exception as e:
        logger.error(f"Erro ao ajustar o modelo ARIMA: {e}")
        return None

def forecast_price(model, steps=1):
    """
    Realiza uma previsão de preço usando o modelo ARIMA fornecido.
    
    Args:
        model (ARIMA): Modelo ARIMA treinado.
        steps (int): Número de passos à frente para prever.
        
    Returns:
        float: Preço previsto, ou None se falhar.
    """
    try:
        if model is not None:
            forecast = model.forecast(steps=steps)
            return forecast.iloc[0]
        else:
            return None
    except Exception as e:
        logger.error(f"Erro ao fazer previsão com ARIMA: {e}")
        return None
