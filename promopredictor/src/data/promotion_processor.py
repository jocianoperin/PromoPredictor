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

def process_product_chunk(product_data: List[Dict[str, Any]]) -> int:
    promotions_identified = 0

    if product_data:
        codigo = product_data[0]['CodigoProduto']
        logger.debug(f"Processando {len(product_data)} entradas de vendas para o produto {codigo}.")

    for data in product_data:
        # Processamento para identificar se existe uma promoção para cada entrada de venda.
        # Supõe-se que 'data' contém os dados de vendas de um único produto.
        avg_cost = sum(d['ValorCusto'] for d in product_data) / len(product_data)
        avg_sale_price = sum(d['ValorUnitario'] for d in product_data) / len(product_data)
        
        # Detalhes do cálculo para a promoção
        logger.debug(
            f"Calculando promoção para o produto {data['CodigoProduto']} na data {data['Data']}. "
            f"Custo médio: {avg_cost}, Preço de venda médio: {avg_sale_price}"
        )

        if data['ValorUnitario'] < avg_sale_price * 0.95 and abs(data['ValorCusto'] - avg_cost) < avg_cost * 0.05:
            # Log antes de inserir a promoção
            logger.debug(
                f"Promoção identificada para o produto {data['CodigoProduto']} na data {data['Data']}. "
                f"Valor Unitário: {data['ValorUnitario']}, Valor Tabela: {avg_sale_price}"
            )

            insert_promotion({
                'CodigoProduto': data['CodigoProduto'],
                'Data': data['Data'],
                'ValorUnitario': data['ValorUnitario'],
                'ValorTabela': avg_sale_price
            })
            promotions_identified += 1
            logger.info(f"Promoção inserida para o produto {data['CodigoProduto']} na data {data['Data']}.")
    
    logger.debug(f"Total de {promotions_identified} promoções identificadas e inseridas para o produto {codigo}.")
    return promotions_identified

def organize_sales_by_product(products: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    logger.debug("Organizando vendas por código do produto.")
    product_sales = {}
    for product in products:
        if product['CodigoProduto'] not in product_sales:
            product_sales[product['CodigoProduto']] = []
        product_sales[product['CodigoProduto']].append(product)
    logger.debug("Organização concluída.")
    return product_sales

def process_chunks(products: List[Dict[str, Any]], chunk_size: int = 10):
    logger.info("Iniciando o processamento paralelo de chunks para identificação de promoções.")
    
    product_sales = organize_sales_by_product(products)
    
    # Processa os dados de vendas por produto
    total_promotions_identified = 0

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_product = {executor.submit(process_product_chunk, data): code for code, data in product_sales.items()}
        for future in as_completed(future_to_product):
            product_code = future_to_product[future]
            logger.debug(f"Iniciando o processamento assíncrono para o produto {product_code}.")
            
            try:
                promotions_identified = future.result()
                total_promotions_identified += promotions_identified
                logger.info(f"Promoções identificadas para o produto {product_code}: {promotions_identified}")
            except Exception as e:
                logger.error(f"Erro ao processar o produto {product_code}: {e}")

    logger.info(f"Processamento paralelo concluído. Total de {total_promotions_identified} promoções identificadas em todos os produtos.")