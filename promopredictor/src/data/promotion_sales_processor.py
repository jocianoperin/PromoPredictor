from concurrent.futures import ThreadPoolExecutor, as_completed
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger
from typing import cast, List, Dict, Any

logger = get_logger(__name__)

def calculate_sales_indicators_for_promotion(promo: Dict[str, Any]) -> Dict[str, Any]:
    logger.info(f"Calculando indicadores de vendas para o período promocional do produto {promo['CodigoProduto']} de {promo['DataInicioPromocao']} até {promo['DataFimPromocao']}")
    connection = get_db_connection()
    indicators = {}

    if connection:
        try:
            with connection.cursor(dictionary=True) as cursor:
                # Consulta para todas as vendas no período promocional
                cursor.execute("""
                    SELECT 
                        COUNT(*) AS TotalVendas,
                        SUM(v.TotalPedido) AS ValorTotalVendas
                    FROM vendasexport v
                    WHERE v.Data BETWEEN %s AND %s
                """, (promo['DataInicioPromocao'], promo['DataFimPromocao']))
                all_sales = cursor.fetchone()
                
                # Consulta para vendas que envolvem o produto promovido
                cursor.execute("""
                    SELECT 
                        COUNT(*) AS TotalVendasProduto,
                        SUM(v.TotalPedido) AS ValorTotalVendasProduto
                    FROM vendasprodutosexport vp
                    JOIN vendasexport v ON vp.CodigoVenda = v.Codigo
                    WHERE vp.CodigoProduto = %s AND v.Data BETWEEN %s AND %s
                """, (promo['CodigoProduto'], promo['DataInicioPromocao'], promo['DataFimPromocao']))
                product_sales = cursor.fetchone()

                if all_sales and product_sales:
                    indicators = {
                        "CodigoProduto": promo['CodigoProduto'],
                        "DataInicioPromocao": promo['DataInicioPromocao'],
                        "DataFimPromocao": promo['DataFimPromocao'],
                        "TotalVendasPeriodo": all_sales['TotalVendas'],
                        "ValorTotalVendasPeriodo": all_sales['ValorTotalVendas'],
                        "TotalVendasProduto": product_sales['TotalVendasProduto'],
                        "ValorTotalVendasProduto": product_sales['ValorTotalVendasProduto']
                    }
        except Exception as e:
            logger.error(f"Erro ao calcular indicadores de vendas: {e}")
        finally:
            connection.close()

    return indicators


def insert_sales_indicators(indicators: Dict[str, Any]):
    if indicators:
        connection = get_db_connection()
        if connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO sales_indicators (CodigoProduto, DataInicioPromocao, DataFimPromocao, QuantidadeTotal, ValorTotalVendido)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE 
                            QuantidadeTotal = VALUES(QuantidadeTotal), 
                            ValorTotalVendido = VALUES(ValorTotalVendido);
                    """, (indicators['CodigoProduto'], indicators['DataInicioPromocao'], indicators['DataFimPromocao'], indicators['QuantidadeTotal'], indicators['ValorTotalVendido']))
                    connection.commit()
            except Exception as e:
                logger.error(f"Erro ao inserir indicadores de vendas para o produto {indicators['CodigoProduto']}: {e}")
                connection.rollback()
            finally:
                connection.close()

def fetch_promotions() -> List[Dict[str, Any]]:
    connection = get_db_connection()
    promotions = []
    if connection:
        try:
            with connection.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT * FROM promotions_identified")
                promotions_raw = cursor.fetchall()
                promotions = [cast(Dict[str, Any], promo) for promo in promotions_raw]
        except Exception as e:
            logger.error(f"Erro ao buscar períodos promocionais: {e}")
        finally:
            connection.close()

    if promotions:
        logger.info(f"{len(promotions)} períodos promocionais encontrados para processamento.")
    else:
        logger.debug("Nenhum período promocional encontrado para processamento.")

    return promotions

def process_promotions_in_chunks():
    promotions = fetch_promotions()
    if promotions:
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(calculate_sales_indicators_for_promotion, promo) for promo in promotions}
            for future in as_completed(futures):
                indicators = future.result()
                if indicators:
                    insert_sales_indicators(indicators)
        logger.info("Todos os indicadores de vendas para os períodos promocionais foram processados.")
    else:
        logger.debug("Nenhum período promocional para processar.")
