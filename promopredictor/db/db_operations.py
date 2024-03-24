import mysql.connector
from ..logging_config import get_logger

logger = get_logger(__name__)

class PromotionsDB:
    def __init__(self, conn):
        self.conn = conn

    def create_promotions_table_if_not_exists(self):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS promotions_identified (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        CodigoProduto INT NOT NULL,
                        Data DATE NOT NULL,
                        ValorUnitario DECIMAL(10, 2) NOT NULL,
                        ValorTabela DECIMAL(10, 2) NOT NULL,
                        UNIQUE KEY unique_promocao (CodigoProduto, Data)
                    );
                """)
                self.conn.commit()
                logger.info("Tabela 'promotions_identified' verificada/criada com sucesso.")
        except mysql.connector.Error as e:
            logger.error("Erro ao criar a tabela 'promotions_identified': %s", e)
            self.conn.rollback()

    def insert_promotion(self, promo):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO promotions_identified (CodigoProduto, Data, ValorUnitario, ValorTabela)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE ValorUnitario = VALUES(ValorUnitario), ValorTabela = VALUES(ValorTabela);
                """, (promo['CodigoProduto'], promo['Data'], promo['ValorUnitario'], promo['ValorTabela']))
                self.conn.commit()
                logger.info("Promoção inserida/atualizada com sucesso.")
        except mysql.connector.Error as e:
            logger.error("Erro ao inserir/atualizar promoção: %s", e)
            self.conn.rollback()

    def get_all_promotions(self):
        """
        Recupera todas as promoções armazenadas na tabela de promoções.
        """
        promotions = []
        try:
            with self.conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                    SELECT CodigoProduto, Data AS DataPromocao, ValorUnitario, ValorTabela
                    FROM promotions_identified;
                """)
                promotions = cursor.fetchall()
                logger.info(f"{len(promotions)} promoções recuperadas.")
        except mysql.connector.Error as e:
            logger.error("Erro ao recuperar promoções: %s", e)
        return promotions

class DatabaseCleaner:
    def __init__(self, conn):
        self.conn = conn

    def clean_data(self):
        self._clean_vendas()
        self._clean_vendas_produtos()

    def _clean_vendas(self):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("DELETE FROM vendasexport WHERE TotalPedido <= 0")
                affected_rows = cursor.rowcount
                self.conn.commit()
                logger.info(f"Limpeza na tabela 'vendas': {affected_rows} linhas removidas.")
        except mysql.connector.Error as e:
            logger.error(f"Erro durante a limpeza da tabela 'vendas': {e}")
            self.conn.rollback()

    def _clean_vendas_produtos(self):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("DELETE FROM vendasprodutosexport WHERE ValorTotal <= 0 OR Quantidade <= 0")
                affected_rows = cursor.rowcount
                self.conn.commit()
                logger.info(f"Limpeza na tabela 'vendas_produtos': {affected_rows} linhas removidas.")
        except mysql.connector.Error as e:
            logger.error(f"Erro durante a limpeza da tabela 'vendas_produtos': {e}")
            self.conn.rollback()