# src/models/arima_model.py

import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def train_arima(series, product_id, value_column, p=1, d=1, q=1):
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
        save_arima_config(product_id, value_column, arima_result, p, d, q)
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

def save_arima_config(product_id, value_column, model, p, d, q):
    """
    Salva a configuração do modelo ARIMA no banco de dados.
    Args:
        product_id (int): ID do produto para o qual o modelo foi treinado.
        value_column (str): Coluna para a qual o modelo foi aplicado.
        model (ARIMA ResultsWrapper): Modelo ARIMA treinado.
        p (int): Ordem do componente AR do modelo.
        d (int): Ordem do componente de diferenciação.
        q (int): Ordem do componente MA do modelo.
    """
    insert_query = """
    INSERT INTO arima_model_config (product_id, value_column, p, d, q, aic, bic, date_executed)
    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW());
    """
    params = (product_id, value_column, p, d, q, model.aic, model.bic)
    try:
        db_manager.execute_query(insert_query, params)
        logger.info("Configuração do modelo ARIMA salva com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao salvar configuração do modelo ARIMA: {e}")