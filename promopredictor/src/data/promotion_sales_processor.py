from concurrent.futures import ThreadPoolExecutor, as_completed
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger
from typing import cast, List, Dict, Any

logger = get_logger(__name__)

def calculate_sales_indicators_for_promotion(promo: Dict[str, Any]) -> Dict[str, Any]:
    logger.debug(f"Calculando indicadores de vendas para a promoção do produto {promo['CodigoProduto']}")
    connection = get_db_connection()

    if connection:
        try:
            with connection.cursor(dictionary=True) as cursor:  # Garante que o resultado seja um dicionário
                cursor.execute("""
                    SELECT 
                        SUM(vp.Quantidade) AS QuantidadeTotal, 
                        SUM(vp.ValorTotal) AS ValorTotalVendido
                    FROM vendasprodutosexport vp
                    JOIN vendasexport v ON vp.CodigoVenda = v.Codigo
                    WHERE vp.CodigoProduto = %s AND v.Data BETWEEN %s AND %s
                """, (promo['CodigoProduto'], promo['Data']))
                result = cast(Dict[str, Any], cursor.fetchone())
                if result:
                    logger.debug(f"Indicadores para o produto {promo['CodigoProduto']} foram calculados com sucesso.")
                    
                    return {
                        "CodigoProduto": promo['CodigoProduto'],
                        "DataInicioPromocao": promo['Data'],
                        "DataFimPromocao": promo['Data'],
                        "QuantidadeTotal": result["QuantidadeTotal"],
                        "ValorTotalVendido": result["ValorTotalVendido"],
                    }
                else:
                    logger.debug(f"Nenhum dado de vendas encontrado para a promoção do produto {promo['CodigoProduto']} em {promo['Data']}")
        except Exception as e:
            logger.error(f"Erro ao calcular indicadores de vendas para a promoção: {e}")
        finally:
            connection.close()
    else:
        logger.error("Não foi possível estabelecer conexão com o banco de dados para calcular indicadores de vendas.")
    return {}

def insert_sales_indicators(indicators):
    connection = get_db_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO sales_indicators (CodigoProduto, DataInicioPromocao, DataFimPromocao, QuantidadeTotal, ValorTotalVendido)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE QuantidadeTotal = VALUES(QuantidadeTotal), ValorTotalVendido = VALUES(ValorTotalVendido);
                """, (indicators['CodigoProduto'], indicators['DataInicioPromocao'], indicators['DataFimPromocao'], indicators['QuantidadeTotal'], indicators['ValorTotalVendido']))
                connection.commit()
                logger.info(f"Indicadores de vendas inseridos/atualizados com sucesso para a promoção do produto {indicators['CodigoProduto']}.")
        except Exception as e:
            logger.error(f"Erro ao inserir indicadores de vendas: {e}")
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
                promotions = cast(List[Dict[str, Any]], promotions_raw)
                logger.info(f"{len(promotions)} promoções encontradas para processamento.")
        except Exception as e:
            logger.error(f"Erro ao buscar promoções: {e}")
        finally:
            connection.close()
    return promotions

def process_promotions_in_chunks():
    promotions = fetch_promotions()
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(calculate_sales_indicators_for_promotion, promo) for promo in promotions]
        for future in as_completed(futures):
            indicators = future.result()
            if indicators:
                insert_sales_indicators(indicators)
    logger.info("Todos os indicadores de vendas foram processados e inseridos.")