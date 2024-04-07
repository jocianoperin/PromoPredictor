from db.db_config import get_db_connection
from repositories.interfaces.ivendas_produtos_export_repository import IVendasProdutosExportRepository
from utils.logging_config import get_logger

logger = get_logger(__name__)

class VendasProdutosExportRepository(IVendasProdutosExportRepository):
    def __init__(self):
        self.conn = get_db_connection()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()

    def close_connection(self):
        if self.conn.is_connected():
            self.conn.close()
            logger.info("Conexão com o banco de dados fechada.")

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

    def index_exists(self, index_name, table_name):
        query = """
        SELECT COUNT(*)
        FROM information_schema.statistics
        WHERE table_schema = (SELECT DATABASE()) AND table_name = %s AND index_name = %s;
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query, (table_name, index_name))
            result = cursor.fetchone()
            if isinstance(result, tuple):  # Verificando se result é uma tupla
                count = result[0]
                if isinstance(count, int) and count > 0:  # Certificamo-nos de que count é um int e é maior que 0
                    return True
            return False

    def create_indexes(self):
        indexes_info = [
            ("idx_vendasprodutosexport_codigovenda", "vendasprodutosexport", "CodigoVenda"),
            ("idx_vendasprodutosexport_codigoproduto", "vendasprodutosexport", "CodigoProduto"),
            ("idx_vendasprodutosexport_codigosecao", "vendasprodutosexport", "CodigoSecao"),
            ("idx_vendasprodutosexport_codigogrupo", "vendasprodutosexport", "CodigoGrupo"),
            ("idx_vendasprodutosexport_codigosubgrupo", "vendasprodutosexport", "CodigoSubGrupo"),
            ("idx_vendasprodutosexport_secaogrupo", "vendasprodutosexport", "CodigoSecao, CodigoGrupo"),
            ("idx_vendasprodutosexport_valorunitario", "vendasprodutosexport", "ValorUnitario"),
            ("idx_vendasprodutosexport_quantidade", "vendasprodutosexport", "Quantidade"),
            ("idx_vendasprodutosexport_desconto", "vendasprodutosexport", "Desconto"),
            ("idx_vendasprodutosexport_precoempromocao", "vendasprodutosexport", "PrecoemPromocao"),
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