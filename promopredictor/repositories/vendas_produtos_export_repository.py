from db.db_config import get_db_connection
from repositories.interfaces.ivendas_produtos_export_repository import IVendasProdutosExportRepository
from utils.logging_config import get_logger

logger = get_logger(__name__)

class VendasProdutosExportRepository(IVendasProdutosExportRepository):
    def __init__(self):
        self.conn = get_db_connection()

    def clean_data(self):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("DELETE FROM vendasprodutosexport WHERE ValorTotal <= 0 OR Quantidade <= 0")
                affected_rows = cursor.rowcount
                self.conn.commit()
                logger.info(f"Limpeza na tabela 'vendasprodutosexport': {affected_rows} linhas removidas.")
        except Exception as e:
            logger.error(f"Erro durante a limpeza da tabela 'vendasprodutosexport': {e}")
            self.conn.rollback()
        finally:
            self.conn.close()
