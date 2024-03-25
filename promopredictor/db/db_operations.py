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

    def identify_and_insert_promotions(self):
        """
        Identifica promoções com base em critérios específicos e insere na tabela promotions_identified.
        """
        logger.info("Iniciando a identificação e inserção de promoções.")
        try:
            with self.conn.cursor(dictionary=True) as cursor:
                logger.info("Recuperando dados necessários para análise do banco de dados.")
                cursor.execute("""
                SELECT 
                    vp.CodigoProduto, 
                    v.Data, 
                    vp.ValorUnitario, 
                    vp.ValorCusto
                FROM vendasprodutosexport vp
                JOIN vendasexport v ON vp.CodigoVenda = v.Codigo
                ORDER BY vp.CodigoProduto, v.Data;
                """)
                products_data = cursor.fetchall()
                logger.info(f"Dados recuperados para {len(products_data)} registros.")

            product_history = {}
            for row in products_data:
                if row['CodigoProduto'] not in product_history:
                    product_history[row['CodigoProduto']] = []
                product_history[row['CodigoProduto']].append(row)

            total_promotions = 0
            for codigo, entries in product_history.items():
                logger.info(f"Processando produto {codigo} com {len(entries)} entradas.")
                promo_count = 0
                for i in range(1, len(entries)):
                    avg_cost = sum(e['ValorCusto'] for e in entries[:i]) / i
                    avg_sale_price = sum(e['ValorUnitario'] for e in entries[:i]) / i

                    current_entry = entries[i]
                    if (current_entry['ValorUnitario'] < avg_sale_price * 0.95 and 
                        abs(current_entry['ValorCusto'] - avg_cost) < avg_cost * 0.05):
                        self.insert_promotion({
                            'CodigoProduto': current_entry['CodigoProduto'], 
                            'Data': current_entry['Data'], 
                            'ValorUnitario': current_entry['ValorUnitario'], 
                            'ValorTabela': avg_sale_price
                        })
                        promo_count += 1
                        total_promotions += 1
                        logger.info(f"Promoção identificada e inserida para o produto {codigo} na data {current_entry['Data']}.")
                
                if promo_count > 0:
                    logger.info(f"Identificadas e inseridas {promo_count} promoções para o produto {codigo}.")
                else:
                    logger.info(f"Nenhuma promoção identificada para o produto {codigo}.")

            logger.info(f"Processamento concluído. Total de promoções identificadas e inseridas: {total_promotions}.")

        except mysql.connector.Error as e:
            logger.error(f"Erro ao identificar e inserir promoções: {e}")
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