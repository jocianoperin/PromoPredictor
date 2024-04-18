from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def explore_data(table_name):
    """
    Realiza uma exploração básica dos dados em uma tabela.
    Args:
        table_name (str): Nome da tabela a ser explorada.
    """
    try:
        # Obter o número de linhas
        count_query = f"SELECT COUNT(*) FROM {table_name}"
        row_count = db_manager.execute_query(count_query)[0][0]
        logger.info(f"Número de linhas na tabela '{table_name}': {row_count}")

        # Obter as estatísticas básicas para colunas numéricas
        numeric_columns = get_numeric_columns(table_name)
        for column in numeric_columns:
            stats_query = f"""
                SELECT
                    MIN({column}) AS minimum,
                    MAX({column}) AS maximum,
                    AVG({column}) AS mean,
                    STDDEV({column}) AS std_dev
                FROM {table_name}
            """
            stats = db_manager.execute_query(stats_query)[0]
            logger.info(f"Estatísticas para a coluna '{column}' na tabela '{table_name}': {stats}")

    except Exception as e:
        logger.error(f"Erro ao explorar a tabela '{table_name}': {e}")

def get_numeric_columns(table_name):
    """
    Obtém os nomes das colunas numéricas de uma tabela.
    Args:
        table_name (str): Nome da tabela.
    Returns:
        list: Lista com os nomes das colunas numéricas.
    """
    query = f"SHOW COLUMNS FROM {table_name} WHERE Type LIKE '%int%' OR Type LIKE '%float%' OR Type LIKE '%decimal%'"
    result = db_manager.execute_query(query)
    numeric_columns = [row[0] for row in result]
    return numeric_columns