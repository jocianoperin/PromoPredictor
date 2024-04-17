from concurrent.futures import ThreadPoolExecutor, as_completed
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger
from typing import cast, List, Dict, Any
from sqlalchemy.exc import SQLAlchemyError

logger = get_logger(__name__)

def calculate_sales_indicators_for_promotion(promo: Dict[str, Any]) -> Dict[str, Any]:
    logger.info(f"Calculando indicadores de vendas para o período promocional do produto {promo['CodigoProduto']} de {promo['DataInicioPromocao']} até {promo['DataFimPromocao']}")
    engine = get_db_connection()
    indicators = {}

    if engine:
        try:
            with engine.connect() as connection:
                result = connection.execute("""
                    SELECT 
                        SUM(vp.Quantidade) AS QuantidadeTotal, 
                        SUM(v.TotalPedido) AS ValorTotalVendido
                    FROM vendasprodutosexport vp
                    JOIN vendasexport v ON vp.CodigoVenda = v.Codigo
                    WHERE vp.CodigoProduto = :codigo AND v.Data BETWEEN :inicio AND :fim
                """, {'codigo': promo['CodigoProduto'], 'inicio': promo['DataInicioPromocao'], 'fim': promo['DataFimPromocao']}).fetchone()
                if result:
                    indicators = {
                        "CodigoProduto": promo['CodigoProduto'],
                        "DataInicioPromocao": promo['DataInicioPromocao'],
                        "DataFimPromocao": promo['DataFimPromocao'],
                        "QuantidadeTotal": result.get("QuantidadeTotal", 0),
                        "ValorTotalVendido": result.get("ValorTotalVendido", 0.0),
                    }
        except SQLAlchemyError as e:
            logger.error(f"Erro ao calcular indicadores de vendas: {e}")
        finally:
            engine.dispose()
    return indicators

def insert_sales_indicators(indicators: Dict[str, Any]):
    if indicators:
        engine = get_db_connection()
        if engine:
            try:
                with engine.connect() as connection:
                    connection.execute("""
                        INSERT INTO sales_indicators (CodigoProduto, DataInicioPromocao, DataFimPromocao, QuantidadeTotal, ValorTotalVendido)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE 
                            QuantidadeTotal = VALUES(QuantidadeTotal), 
                            ValorTotalVendido = VALUES(ValorTotalVendido);
                    """, (indicators['CodigoProduto'], indicators['DataInicioPromocao'], indicators['DataFimPromocao'], indicators['QuantidadeTotal'], indicators['ValorTotalVendido']))
                    connection.commit()
            except SQLAlchemyError as e:
                logger.error(f"Erro ao inserir indicadores de vendas para o produto {indicators['CodigoProduto']}: {e}")
                connection.rollback()
            finally:
                engine.dispose()

def fetch_promotions() -> List[Dict[str, Any]]:
    engine = get_db_connection()
    promotions = []
    if engine:
        try:
            with engine.connect() as connection:
                result = connection.execute("SELECT * FROM promotions_identified")
                promotions_raw = result.fetchall()
                promotions = [cast(Dict[str, Any], promo) for promo in promotions_raw]
        except SQLAlchemyError as e:
            logger.error(f"Erro ao buscar períodos promocionais: {e}")
        finally:
            engine.dispose()

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
