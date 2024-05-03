from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def prepare_time_series_data(table, product_column, date_column, value_column, product_id):
    """
    Prepara dados para análise de séries temporais por produto, filtrando apenas registros com valores nulos na coluna especificada.
    """
    query = f"""
    SELECT {date_column}, {value_column}
    FROM {table}
    WHERE {product_column} = %s AND {value_column} IS NULL
    ORDER BY {date_column};
    """
    try:
        data = db_manager.execute_query(query, [product_id])
        logger.info(f"Dados de série temporal com valores nulos preparados com sucesso para o produto {product_id}.")
        return data
    except Exception as e:
        logger.error(f"Erro na preparação dos dados da série temporal para o produto {product_id}: {e}")
        return None