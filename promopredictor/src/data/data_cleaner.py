# src/data/data_cleaner.py
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def execute_query(query):
    connection = get_db_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                affected_rows = cursor.rowcount
                connection.commit()
                return affected_rows
        except Exception as e:
            logger.error(f"Erro durante a execução da query: {e}")
            connection.rollback()
        finally:
            connection.close()
    return 0

def delete_data(table_name, condition):
    delete_query = f"DELETE FROM {table_name} WHERE {condition}"
    affected_rows = execute_query(delete_query)
    logger.info(f"DELETE na tabela '{table_name}': {affected_rows} linhas removidas sob a condição '{condition}'.")

def update_data(table_name, updates, condition):
    update_query = f"UPDATE {table_name} SET {updates} WHERE {condition}"
    affected_rows = execute_query(update_query)
    logger.info(f"UPDATE na tabela '{table_name}': {affected_rows} linhas atualizadas sob a condição '{condition}'.")
