# src/data/index_manager.py
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def create_indexes(indexes_info):
    for index_name, table_name, columns in indexes_info:
        try:
            connection = get_db_connection()
            if connection:
                with connection.cursor() as cursor:
                    # Tentamos criar o índice diretamente, já que a operação é segura e idempotente
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({columns});")
                    connection.commit()
                    logger.debug(f"Tentativa de criação do índice '{index_name}' em '{table_name}'.")
        except Exception as e:
            logger.error(f"Erro ao criar índice '{index_name}' em '{table_name}': {e}")
        finally:
            if connection:
                connection.close()
    logger.info("Processo de criação de índices concluído.")