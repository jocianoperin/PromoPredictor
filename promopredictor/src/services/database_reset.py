from src.utils.logging_config import get_logger
from src.services.database import db_manager

logger = get_logger(__name__)

def drop_tables():  # Adicione db_manager como argumento
    """
    Exclui todas as tabelas no banco de dados.
    """
    try:
        db_manager.execute_query("DROP TABLE IF EXISTS sales_indicators")
        logger.info("Tabela sales_indicators excluída com sucesso.")
        db_manager.execute_query("DROP TABLE IF EXISTS promotions_identified")
        logger.info("Tabela promotions_identified excluída com sucesso.")
        # Adicione outras tabelas que precisam ser excluídas aqui
    except Exception as e:
        logger.error(f"Erro ao excluir tabelas: {e}")