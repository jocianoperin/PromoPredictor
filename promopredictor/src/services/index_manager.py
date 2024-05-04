from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def create_indexes(indexes):
    """
    Cria índices nas tabelas e colunas especificadas.
    Args:
        indexes (list of tuples): Lista de tuplas contendo o nome do índice, nome da tabela e colunas.
    """
    logger.info("Início da criação de indexes.")
    try:
        for index_name, table_name, columns in indexes:
            columns_str = ', '.join(columns.split(', '))
            create_index_query = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({columns_str})"
            db_manager.execute_query(create_index_query)

            logger.info(f"Índice {index_name} criado na tabela {table_name}, coluna {columns_str}.")
        logger.info("Processo de criação de índices concluído.")
    except Exception as e:
        logger.error(f"Erro ao criar índices: {e}")