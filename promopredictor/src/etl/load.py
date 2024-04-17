import pandas as pd
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def load_vendasexport(vendasexport):
    """
    Carrega os dados transformados para a tabela vendasexport no banco de dados.

    Args:
        vendasexport (pd.DataFrame): DataFrame contendo os dados transformados da tabela vendasexport.
    """
    connection = get_db_connection()
    if connection:
        try:
            vendasexport.to_sql("vendasexport", connection, if_exists="replace", index=False)
            logger.info("Dados transformados carregados na tabela vendasexport com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao carregar dados na tabela vendasexport: {e}")
        finally:
            connection.close()

def load_vendasprodutosexport(vendasprodutosexport):
    """
    Carrega os dados transformados para a tabela vendasprodutosexport no banco de dados.

    Args:
        vendasprodutosexport (pd.DataFrame): DataFrame contendo os dados transformados da tabela vendasprodutosexport.
    """
    connection = get_db_connection()
    if connection:
        try:
            vendasprodutosexport.to_sql("vendasprodutosexport", connection, if_exists="replace", index=False)
            logger.info("Dados transformados carregados na tabela vendasprodutosexport com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao carregar dados na tabela vendasprodutosexport: {e}")
        finally:
            connection.close()