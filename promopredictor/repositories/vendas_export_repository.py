from db.db_config import get_db_connection
from repositories.interfaces.ivendas_export_repository import IVendasExportRepository
from utils.logging_config import get_logger
from typing import Optional, Tuple, Any

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

    def index_exists(self, index_name, table_name):
        query = """
        SELECT COUNT(*)
        FROM information_schema.statistics
        WHERE table_schema = (SELECT DATABASE()) AND table_name = %s AND index_name = %s;
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query, (table_name, index_name))
            result = cursor.fetchone()
            if result:  # Aqui estamos verificando se result não é None
                count = result[0]  # Podemos confiar que result[0] está acessível, pois result não é None.
                if isinstance(count, int) and count > 0:  # Certificamo-nos de que count é um int e é maior que 0.
                    return True
            return False

    def create_indexes(self):
        indexes_info = [
            ("idx_codigo", "vendasexport", "Codigo"),
            ("idx_data", "vendasexport", "Data"),
            ("idx_codigocliente", "vendasexport", "CodigoCliente"),
            ("idx_data_codigocliente", "vendasexport", "Data, CodigoCliente"),
            ("idx_totalpedido", "vendasexport", "TotalPedido"),
        ]
        for index_name, table_name, columns in indexes_info:
            if not self.index_exists(index_name, table_name):
                try:
                    command = f"CREATE INDEX {index_name} ON {table_name} ({columns});"
                    with self.conn.cursor() as cursor:
                        cursor.execute(command)
                        self.conn.commit()
                        logger.info(f"Índice '{index_name}' criado com sucesso em '{table_name}'.")
                except Exception as e:
                    logger.error(f"Erro ao criar índice '{index_name}': {e}")
                    self.conn.rollback()
            else:
                logger.info(f"Índice '{index_name}' já existe em '{table_name}' e não será criado.")
        logger.info("Processo de criação de índices concluído.")
