from ..db.db_config import get_db_connection
from ..db.db_operations import DatabaseOptimizer
from ..logging_config import get_logger

logger = get_logger(__name__)

def main():
    logger.info("Iniciando processo de otimização do banco de dados.")
    conn = get_db_connection()
    try:
        optimizer = DatabaseOptimizer(conn)
        optimizer.create_indexes()
        logger.info("Índices criados com sucesso.")
    except Exception as e:
        logger.error(f"Erro durante o processo de otimização: {e}")
    finally:
        if conn.is_connected():
            conn.close()

if __name__ == "__main__":
    main()