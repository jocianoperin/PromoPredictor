# src/models/arima_model.py

import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def train_arima(series):
    """
    Treina um modelo ARIMA com os dados de série temporal fornecidos.
    Args:
        series (pd.Series): Série temporal de dados para treinamento do modelo.
    Returns:
        ARIMA ResultsWrapper: Um objeto de resultado treinado do modelo ARIMA.
    """
    try:
        model = ARIMA(series, order=(1, 1, 1))  # Ajuste estes parâmetros conforme necessário
        arima_result = model.fit()
        logger.info("Modelo ARIMA treinado com sucesso.")
        return arima_result
    except Exception as e:
        logger.error(f"Erro ao treinar o modelo ARIMA: {e}")
        return None

def forecast_arima(model, steps=1):
    """
    Realiza previsões futuras usando o modelo ARIMA treinado.
    Args:
        model (ARIMA ResultsWrapper): Modelo ARIMA treinado.
        steps (int): Número de passos de tempo a prever para frente.
    Returns:
        np.ndarray: Um array de previsões.
    """
    try:
        forecast = model.forecast(steps=steps)
        logger.info("Previsões ARIMA realizadas com sucesso.")
        return forecast
    except Exception as e:
        logger.error(f"Erro ao realizar previsões com o modelo ARIMA: {e}")
        return None

def impute_values(table, product_column, date_column, value_column, product_id, forecasted_value):
    """
    Imputa um valor previsto em uma tabela específica para uma entrada que possui um valor nulo.
    Args:
        table (str): Nome da tabela onde o valor será imputado.
        product_column (str): Coluna que identifica o produto.
        date_column (str): Coluna que identifica a data.
        value_column (str): Coluna que receberá o valor imputado.
        product_id (int): ID do produto.
        forecasted_value (float): Valor previsto para ser imputado.
    """
    update_query = f"""
    UPDATE {table}
    SET {value_column} = %s
    WHERE {product_column} = %s AND {value_column} IS NULL
    ORDER BY {date_column} DESC
    LIMIT 1;  # Isso assume que você deseja imputar o valor mais recente nulo.
    """
    try:
        db_manager.execute_query(update_query, [forecasted_value, product_id])
        logger.info(f"Valor imputado com sucesso para o produto {product_id} na coluna {value_column}.")
    except Exception as e:
        logger.error(f"Erro ao imputar valor para o produto {product_id}: {e}")
