from db.db_config import get_db_connection
from repositories.interfaces.ipromotions_repository import IPromotionsRepository
from utils.logging_config import get_logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import cast, Dict, List, Any

logger = get_logger(__name__)

class PromotionsRepository(IPromotionsRepository):
    def __init__(self):
        self.conn = get_db_connection()
        logger.info("PromotionsRepository inicializado e conexão com o banco de dados estabelecida.")

    def __enter__(self):
        logger.debug("PromotionsRepository entrando no contexto.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()
        logger.debug("PromotionsRepository saindo do contexto.")

    def close_connection(self):
        if self.conn.is_connected():
            self.conn.close()
            logger.info("Conexão com o banco de dados fechada com sucesso.")

    def create_promotions_table_if_not_exists(self):
        logger.debug("Verificando se a tabela 'promotions_identified' existe e criando-a se necessário.")
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

    def insert_promotion(self, promo: Dict[str, Any]):
        logger.debug(f"Inserindo ou atualizando promoção: {promo}")
        with self.conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO promotions_identified (CodigoProduto, Data, ValorUnitario, ValorTabela)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE ValorUnitario = VALUES(ValorUnitario), ValorTabela = VALUES(ValorTabela);
            """, (promo['CodigoProduto'], promo['Data'], promo['ValorUnitario'], promo['ValorTabela']))
            self.conn.commit()
            logger.info(f"Promoção para o produto {promo['CodigoProduto']} na data {promo['Data']} inserida/atualizada com sucesso.")

    def fetch_all_products(self) -> List[Dict[str, Any]]:
        logger.debug("Buscando todos os produtos para processamento de promoções.")
        with self.conn.cursor(dictionary=True) as cursor:
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
            products_raw = cursor.fetchall()
            products: List[Dict[str, Any]] = [
                cast(Dict, row)  # Use cast to assert the row type for the type checker
                for row in products_raw
            ]
            logger.info(f"{len(products)} produtos encontrados para processamento.")
            return products

    def process_product_chunk(self, product_chunk: Dict) -> int:
        promotions_identified = 0
        codigo = product_chunk['CodigoProduto']
        entries = product_chunk['Entries']
        logger.debug(f"Processando chunk para o produto {codigo} com {len(entries)} entradas.")

        with self.conn.cursor() as cursor:
            for i in range(1, len(entries)):
                avg_cost = sum(e['ValorCusto'] for e in entries[:i]) / i
                avg_sale_price = sum(e['ValorUnitario'] for e in entries[:i]) / i
                
                current_entry = entries[i]
                if current_entry['ValorUnitario'] < avg_sale_price * 0.95 and abs(current_entry['ValorCusto'] - avg_cost) < avg_cost * 0.05:
                    data_to_insert = {
                        'CodigoProduto': current_entry['CodigoProduto'],
                        'Data': current_entry['Data'],
                        'ValorUnitario': current_entry['ValorUnitario'],
                        'ValorTabela': avg_sale_price
                    }
                    self.insert_promotion(data_to_insert)
                    promotions_identified += 1

            logger.info(f"{promotions_identified} promoções identificadas e inseridas para o produto {codigo}.")

        return promotions_identified

    def process_chunks(self, product_chunks: List[Dict], chunk_size: int = 10):
        logger.debug("Iniciando o processamento de chunks para identificação de promoções.")
        total_promotions_identified = 0
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(self.process_product_chunk, chunk) for chunk in product_chunks]
            for future in as_completed(futures):
                total_promotions_identified += future.result()

        logger.info(f"Total de {total_promotions_identified} promoções identificadas em todos os chunks.")