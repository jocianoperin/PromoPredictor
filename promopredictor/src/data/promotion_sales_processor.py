from concurrent.futures import ThreadPoolExecutor, as_completed
from src.services.database import db_manager
from src.utils.logging_config import get_logger
from typing import List, Dict, Any

logger = get_logger(__name__)

def calculate_sales_indicators_for_promotion(promo: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calcula os indicadores de vendas para um determinado período promocional de um produto.
    Args:
        promo (Dict[str, Any]): Dicionário contendo informações do produto e do período promocional.
    Returns:
        Dict[str, Any]: Dicionário contendo os indicadores calculados para o produto.
    """
    logger.info(f"Calculando indicadores de vendas para o período promocional do produto {promo['CodigoProduto']} de {promo['DataInicioPromocao']} até {promo['DataFimPromocao']}")
    
    indicators = {}
    
    query = """
        SELECT 
            SUM(vp.Quantidade) AS QuantidadeTotal, 
            SUM(v.TotalPedido) AS ValorTotalVendido
        FROM vendasprodutosexport vp
        JOIN vendasexport v ON vp.CodigoVenda = v.Codigo
        WHERE vp.CodigoProduto = %s AND v.Data BETWEEN %s AND %s
    """
    params = (promo['CodigoProduto'], promo['DataInicioPromocao'], promo['DataFimPromocao'])
    result = db_manager.execute_query(query, params)
    if result and result[0]:
        indicators = {
            "CodigoProduto": promo['CodigoProduto'],
            "DataInicioPromocao": promo['DataInicioPromocao'],
            "DataFimPromocao": promo['DataFimPromocao'],
            "QuantidadeTotal": result[0].get("QuantidadeTotal", 0),
            "ValorTotalVendido": result[0].get("ValorTotalVendido", 0.0),
        }

    return indicators

def insert_sales_indicators(indicators: Dict[str, Any]):
    """
    Insere os indicadores de vendas calculados para um produto no banco de dados.
    Args:
        indicators (Dict[str, Any]): Dicionário contendo os indicadores de vendas do produto.
    """
    if indicators:
        query = """
            INSERT INTO sales_indicators (CodigoProduto, DataInicioPromocao, DataFimPromocao, QuantidadeTotal, ValorTotalVendido)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                QuantidadeTotal = VALUES(QuantidadeTotal), 
                ValorTotalVendido = VALUES(ValorTotalVendido);
        """
        params = (indicators['CodigoProduto'], indicators['DataInicioPromocao'], indicators['DataFimPromocao'], indicators['QuantidadeTotal'], indicators['ValorTotalVendido'])
        db_manager.execute_query(query, params)

def fetch_promotions() -> List[Dict[str, Any]]:
    """
    Busca todos os períodos promocionais identificados no banco de dados.
    Returns:
        List[Dict[str, Any]]: Lista de dicionários contendo informações sobre os períodos promocionais.
    """
    query = "SELECT * FROM promotions_identified"
    result = db_manager.execute_query(query)
    if result:
        promotions = [dict(promo) for promo in result]
        logger.info(f"{len(promotions)} períodos promocionais encontrados para processamento.")
        return promotions
    else:
        logger.debug("Nenhum período promocional encontrado para processamento.")
        return []

def process_promotions_in_chunks():
    """
    Processa os períodos promocionais em partes, calculando e inserindo os indicadores de vendas.
    """
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
