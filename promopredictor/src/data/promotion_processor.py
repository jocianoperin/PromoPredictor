# src/data/promotion_processor.py
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, cast

logger = get_logger(__name__)

def insert_promotion(promo: Dict[str, Any]):
    connection = get_db_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO promotions_identified (CodigoProduto, Data, ValorUnitario, ValorTabela)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE ValorUnitario = VALUES(ValorUnitario), ValorTabela = VALUES(ValorTabela);
                """, (promo['CodigoProduto'], promo['Data'], promo['ValorUnitario'], promo['ValorTabela']))
                connection.commit()
                logger.info(f"Promoção para o produto {promo['CodigoProduto']} na data {promo['Data']} inserida/atualizada com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao inserir/atualizar promoção: {e}")
            connection.rollback()
        finally:
            connection.close()

def fetch_all_products() -> List[Dict[str, Any]]:
    connection = get_db_connection()
    products = []
    if connection:
        try:
            with connection.cursor(dictionary=True) as cursor:
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
        except Exception as e:
            logger.error(f"Erro ao buscar produtos: {e}")
        finally:
            connection.close()
    return products

def process_product_chunk(product_chunk: Dict) -> int:
    promotions_identified = 0
    codigo = product_chunk['CodigoProduto']
    entries = product_chunk['Entries']
    logger.info(f"Iniciando processamento do chunk para o produto {codigo}.")

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
            insert_promotion(data_to_insert)
            promotions_identified += 1
    
    logger.debug(f"{promotions_identified} promoções identificadas e inseridas para o produto {codigo}.")
    return promotions_identified

def process_chunks(product_chunks: List[Dict], chunk_size: int = 10):
    logger.info("Iniciando o processamento paralelo de chunks para identificação de promoções.")
    
    total_promotions_identified = 0
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_product_chunk, chunk) for chunk in product_chunks]
        for future in as_completed(futures):
            total_promotions_identified += future.result()

    logger.info(f"Total de {total_promotions_identified} promoções identificadas em todos os chunks.")