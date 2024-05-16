from src.services.database import db_manager
from src.utils.logging_config import get_logger
import pandas as pd

logger = get_logger(__name__)

def prepare_time_series_data(table, product_column, date_column, value_column, product_id):
    """
    Prepara dados para análise de séries temporais por produto, filtrando apenas registros com valores nulos na coluna especificada.
    Args:
        table (str): Nome da tabela de produtos exportados.
        product_column (str): Nome da coluna que identifica o produto.
        date_column (str): Nome da coluna que identifica a data.
        value_column (str): Nome da coluna cujos valores nulos serão analisados.
        product_id (int): ID do produto para o qual os dados serão preparados.
    Returns:
        DataFrame: Contendo dados de série temporal para análise.
    """
    query = f"""
    SELECT vendasprodutosexport.ExportID, {date_column}, {table}.{product_column}, {value_column}
    FROM {table}
    JOIN vendasexport ON {table}.CodigoVenda = vendasexport.Codigo
    WHERE {product_column} = %s AND {value_column} IS NULL
    ORDER BY {date_column};
    """
    try:
        result = db_manager.execute_query(query, [product_id])
        if result and 'data' in result:
            # Convertendo os dados para DataFrame com os nomes corretos das colunas
            df = pd.DataFrame(result['data'], columns=['ExportID', date_column, 'CodigoProduto', value_column])
            logger.info(f"Dados de série temporal com valores nulos preparados com sucesso para o produto {product_id}.")
            return df
        else:
            logger.warning("Dados preparados, mas nenhum valor nulo encontrado.")
            return pd.DataFrame()  # Retorna um DataFrame vazio se não houver dados
    except Exception as e:
        logger.error(f"Erro na preparação dos dados da série temporal para o produto {product_id}: {e}")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro
    
def get_non_null_data(table, product_column, date_column, value_column, product_id):
    """
    Obtém dados sem valores nulos para treinar o modelo ARIMA.

    Args:
        table (str): Nome da tabela de produtos exportados.
        product_column (str): Nome da coluna que identifica o produto.
        date_column (str): Nome da coluna que identifica a data.
        value_column (str): Nome da coluna cujos valores não nulos serão usados para treinar o modelo.
        product_id (int): ID do produto para o qual os dados serão recuperados.

    Returns:
        DataFrame: Contendo dados de série temporal sem valores nulos para o produto especificado.
    """
    query = f"""
    SELECT vendasprodutosexport.ExportID, {date_column}, {table}.{product_column}, {value_column}
    FROM {table}
    JOIN vendasexport ON {table}.CodigoVenda = vendasexport.Codigo
    WHERE {product_column} = %s AND {value_column} IS NOT NULL
    ORDER BY {date_column};
    """

    try:
        result = db_manager.execute_query(query, [product_id])
        if result and 'data' in result:
            df = pd.DataFrame(result['data'], columns=['ExportID', date_column, 'CodigoProduto', value_column])
            logger.info(f"Dados sem valores nulos recuperados com sucesso para o produto {product_id}.")
            return df
        else:
            logger.warning("Dados recuperados, mas nenhum valor não nulo encontrado.")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Erro na obtenção de dados sem valores nulos para o produto {product_id}: {e}")
        return pd.DataFrame()