import pandas as pd
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def extract_vendasexport():
    """
    Extrai os dados da tabela vendasexport do banco de dados.

    Returns:
        pd.DataFrame: DataFrame contendo os dados da tabela vendasexport.
    """
    connection = get_db_connection()
    if connection:
        try:
            query = "SELECT * FROM vendasexport"
            vendasexport = pd.read_sql_query(query, connection)
            logger.info("Dados da tabela vendasexport extraídos com sucesso.")
            return vendasexport
        except Exception as e:
            logger.error(f"Erro ao extrair dados da tabela vendasexport: {e}")
        finally:
            connection.close()
    return None

def extract_vendasprodutosexport():
    """
    Extrai os dados da tabela vendasprodutosexport do banco de dados.

    Returns:
        pd.DataFrame: DataFrame contendo os dados da tabela vendasprodutosexport.
    """
    connection = get_db_connection()
    if connection:
        try:
            query = "SELECT * FROM vendasprodutosexport"
            vendasprodutosexport = pd.read_sql_query(query, connection)
            logger.info("Dados da tabela vendasprodutosexport extraídos com sucesso.")
            return vendasprodutosexport
        except Exception as e:
            logger.error(f"Erro ao extrair dados da tabela vendasprodutosexport: {e}")
        finally:
            connection.close()
    return None