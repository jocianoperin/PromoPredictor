from db.db_config import get_db_connection
from repositories.interfaces.ivendas_export_repository import IVendasExportRepository
from utils.logging_config import get_logger

logger = get_logger(__name__)

class VendasExportRepository(IVendasExportRepository):
    def __init__(self):
        self.conn = get_db_connection()

    def clean_data(self):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("DELETE FROM vendasexport WHERE TotalPedido <= 0")
                affected_rows = cursor.rowcount
                self.conn.commit()
                logger.info(f"Limpeza na tabela 'vendasexport': {affected_rows} linhas removidas.")
        except Exception as e:
            logger.error(f"Erro durante a limpeza da tabela 'vendasexport': {e}")
            self.conn.rollback()
        finally:
            self.conn.close()
