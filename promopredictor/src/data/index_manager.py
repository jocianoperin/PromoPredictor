# src/data/index_manager.py
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def index_exists(index_name, table_name):
    query = """
        SELECT COUNT(*)
        FROM information_schema.statistics
        WHERE table_schema = (SELECT DATABASE()) AND table_name = %s AND index_name = %s;
        """
    connection = get_db_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                query = """
                SELECT COUNT(*)
                FROM information_schema.statistics
                WHERE table_schema = (SELECT DATABASE()) AND table_name = %s AND index_name = %s;
                """
                cursor.execute(query, (table_name, index_name))
                result = cursor.fetchone()
                if isinstance(result, tuple):  # Verificando se result é uma tupla
                    count = result[0]
                    if isinstance(count, int) and count > 0:  # Certificamo-nos de que count é um int e é maior que 0
                        return True
                return False
        except Exception as e:
            logger.error(f"Erro ao verificar existência do índice '{index_name}' na tabela '{table_name}': {e}")
        finally:
            connection.close()
    return False

def create_indexes(indexes_info):
    for index_name, table_name, columns in indexes_info:
        if not index_exists(index_name, table_name):
            connection = get_db_connection()
            if connection:
                try:
                    with connection.cursor() as cursor:
                        command = f"CREATE INDEX {index_name} ON {table_name} ({columns});"
                        cursor.execute(command)
                        connection.commit()
                        logger.info(f"Índice '{index_name}' criado com sucesso em '{table_name}'.")
                except Exception as e:
                    logger.error(f"Erro ao criar índice '{index_name}': {e}")
                    connection.rollback()
                finally:
                    connection.close()
        else:
            logger.info(f"Índice '{index_name}' já existe em '{table_name}' e não será criado.")
    logger.info("Processo de criação de índices concluído.")
