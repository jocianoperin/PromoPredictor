from concurrent.futures import ThreadPoolExecutor
from ..db.db_config import get_db_connection
from ..db.db_operations import PromotionsDB
from ..utils.logging_config import get_logger

logger = get_logger(__name__)

def perform_promotion_analysis():
    logger.info("Iniciando a análise de sucesso das promoções.")
    conn = get_db_connection()
    try:
        promotions_db = PromotionsDB(conn)
        # Chamada direta para o método que analisa o sucesso das promoções e insere os resultados.
        promotions_db.analyze_promotion_success()
        logger.info("Análise de sucesso das promoções concluída com sucesso.")
    except Exception as e:
        logger.error(f"Erro durante a análise de sucesso das promoções: {e}")
    finally:
        if conn.is_connected():
            conn.close()

def main():
    perform_promotion_analysis()

if __name__ == "__main__":
    main()