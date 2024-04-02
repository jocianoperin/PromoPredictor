from ..db.db_config import get_db_connection
from ..db.db_operations import PromotionsDB
from ..logging_config import get_logger

logger = get_logger(__name__)

def process_promotions():
    logger.info("Iniciando processo de identificação e inserção de promoções.")
    conn = get_db_connection()
    try:
        promotions_db = PromotionsDB(conn)

        promotions_db.identify_and_insert_promotions()
        
        logger.info("Processamento de promoções concluído com sucesso.")
    except Exception as e:
        logger.error(f"Erro durante o processamento de promoções: {e}")
    finally:
        if conn.is_connected():
            conn.close()

def main():
    process_promotions()

if __name__ == "__main__":
    main()