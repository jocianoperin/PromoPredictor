# src/data/promotion_processor.py
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta, date
from typing import List, Dict, Any, cast

logger = get_logger(__name__)

def insert_promotion(promo: Dict[str, Any]):
    connection = get_db_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO promotions_identified (CodigoProduto, DataInicioPromocao, DataFimPromocao, ValorUnitario, ValorTabela)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE ValorUnitario = VALUES(ValorUnitario), ValorTabela = VALUES(ValorTabela), DataInicioPromocao = VALUES(DataInicioPromocao), DataFimPromocao = VALUES(DataFimPromocao);
                """, (promo['CodigoProduto'], promo['DataInicioPromocao'], promo['DataFimPromocao'], promo['ValorUnitario'], promo['ValorTabela']))
                connection.commit()
        except Exception as e:
            logger.error(f"Erro ao inserir/atualizar período promocional: {e}")
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
        except Exception as e:
            logger.error(f"Erro ao buscar produtos: {e}")
        finally:
            connection.close()
    return products

def process_product_chunk(product_data: List[Dict[str, Any]]) -> int:
    promotions_identified = 0
    if product_data:
        codigo = product_data[0]['CodigoProduto']

        # Calculando o custo médio e o preço de venda médio para os critérios de promoção
        avg_cost = sum(d['ValorCusto'] for d in product_data) / len(product_data)
        avg_sale_price = sum(d['ValorUnitario'] for d in product_data) / len(product_data)
                
        sorted_data = sorted(product_data, key=lambda x: x['Data'])
        promo_start = None
        promo_end = None
        previous_date = None

        for data in sorted_data:
            current_date = data['Data']
            is_promotion = data['ValorUnitario'] < avg_sale_price * 0.95 and abs(data['ValorCusto'] - avg_cost) < avg_cost * 0.05

            if is_promotion:
                if previous_date and current_date - timedelta(days=1) == previous_date:
                    # Condição de promoção se mantém, atualizar fim do período
                    promo_end = current_date
                else:
                    # Novo período promocional, processar o anterior se existir
                    if promo_start and promo_end:
                        insert_promotion({
                            'CodigoProduto': codigo,
                            'DataInicioPromocao': promo_start,
                            'DataFimPromocao': promo_end,
                            'ValorUnitario': data['ValorUnitario'],
                            'ValorTabela': avg_sale_price,
                        })
                        promotions_identified += 1
                    # Iniciar novo período
                    promo_start = current_date
                    promo_end = current_date

            else:
                # Se o registro atual não atende aos critérios de promoção mas existem datas de início e fim registradas, 
                # finaliza a promoção anterior antes de limpar as variáveis de controle
                if promo_start and promo_end:
                    insert_promotion({
                        'CodigoProduto': codigo,
                        'DataInicioPromocao': promo_start,
                        'DataFimPromocao': promo_end,
                        'ValorUnitario': data['ValorUnitario'],
                        'ValorTabela': avg_sale_price,
                    })
                    promotions_identified += 1
                    promo_start = None
                    promo_end = None
            
            previous_date = current_date

        # Processar a última promoção identificada, se aplicável
        if promo_start and promo_end:
            insert_promotion({
                'CodigoProduto': codigo,
                'DataInicioPromocao': promo_start,
                'DataFimPromocao': promo_end,
                'ValorUnitario': data['ValorUnitario'],
                'ValorTabela': avg_sale_price,
            })
            promotions_identified += 1

    if promotions_identified > 0:
        logger.info(f"Produto {codigo}: {promotions_identified} períodos promocionais identificados.")
    return promotions_identified

def organize_sales_by_product(products: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    product_sales = {}
    for product in products:
        product_sales.setdefault(product['CodigoProduto'], []).append(product)
    return product_sales

def process_chunks(products: List[Dict[str, Any]], chunk_size: int = 20):
    product_sales = organize_sales_by_product(products)
    total_promotions_identified = 0

    if product_sales:
        logger.info("Iniciando o processamento paralelo para identificação de promoções.")

        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_product = {executor.submit(process_product_chunk, data): code for code, data in product_sales.items()}
            for future in as_completed(future_to_product):
                promotions_identified = future.result()
                total_promotions_identified += promotions_identified

        logger.info(f"Processamento paralelo concluído. {total_promotions_identified} promoções identificadas.")
    else:
        logger.debug("Nenhum produto para processar em chunks.")
