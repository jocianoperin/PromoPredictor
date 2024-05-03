from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def store_arima_results(data):
    """
    Armazena os resultados das previsões ARIMA no banco de dados.
    """
    insert_query = """
    INSERT INTO price_forecasts (Data, PrevisaoARIMA)
    VALUES (%(data)s, %(previsao)s)
    """
    params = [{'data': date, 'previsao': forecast} for date, forecast in data]

    try:
        db_manager.execute_query(insert_query, params)
        logger.info("Resultados ARIMA armazenados com sucesso no banco de dados.")
    except Exception as e:
        logger.error(f"Erro ao armazenar resultados ARIMA: {e}")
