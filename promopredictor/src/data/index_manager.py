from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger
from sqlalchemy.exc import SQLAlchemyError

logger = get_logger(__name__)

def create_indexes(indexes_info):
    engine = get_db_connection()  # Assume que get_db_connection retorna um engine SQLAlchemy.
    for index_name, table_name, columns in indexes_info:
        try:
            with engine.connect() as connection:
                # Tentativa de criação do índice, diretamente, pois é uma operação segura e idempotente
                connection.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({columns});")
                connection.commit()
                logger.debug(f"Tentativa de criação do índice '{index_name}' em '{table_name}'.")
        except SQLAlchemyError as e:
            logger.error(f"Erro ao criar índice '{index_name}' em '{table_name}': {e}")
        finally:
            engine.dispose()
    logger.info("Processo de criação de índices concluído.")
