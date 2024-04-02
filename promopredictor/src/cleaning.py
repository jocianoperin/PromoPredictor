from ..db.db_config import get_db_connection
from ..db.db_operations import DatabaseCleaner
from ..logging_config import get_logger

logger = get_logger(__name__)

def perform_cleaning():
    logger.info("Iniciando processo de limpeza de dados.")
    conn = get_db_connection()
    try:
        cleaner = DatabaseCleaner(conn)
        cleaner.clean_data()
        logger.info("Processo de limpeza concluído com sucesso.")
    except Exception as e:
        logger.error(f"Erro durante o processo de limpeza: {e}")
    finally:
        if conn.is_connected():
            conn.close()

def main():
    perform_cleaning()

if __name__ == "__main__":
    main()