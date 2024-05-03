from concurrent.futures import ThreadPoolExecutor
from src.data.time_series_preparation import prepare_time_series_data
from src.models.arima_model import train_arima, forecast_arima, impute_values
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def process_column(product_id, table, product_column, date_column, value_column):
    """
    Processa uma coluna específica para imputação de valores nulos usando o modelo ARIMA.
    Args:
        product_id (int): ID do produto.
        table (str): Nome da tabela.
        product_column (str): Nome da coluna que identifica o produto.
        date_column (str): Nome da coluna que identifica a data.
        value_column (str): Nome da coluna cujos valores nulos serão imputados.
    """
    data = prepare_time_series_data(table, product_column, date_column, value_column, product_id)
    if data:
        model = train_arima(data)
        if model:
            forecast = forecast_arima(model, steps=1)
            impute_values(table, product_column, date_column, value_column, product_id, forecast)
            logger.info(f"Valores nulos imputados com sucesso para o produto {product_id} na coluna {value_column}.")
        else:
            logger.error(f"Falha ao treinar o modelo ARIMA para o produto {product_id} na coluna {value_column}.")
    else:
        logger.error(f"Nenhum dado disponível para imputação para o produto {product_id} na coluna {value_column}.")

def imput_null_values(table, product_column, date_column, value_columns):
    """
    Identifica produtos com valores nulos e executa a imputação usando ARIMA para cada coluna especificada de forma paralela.
    Args:
        table (str): Nome da tabela.
        product_column (str): Nome da coluna do produto.
        date_column (str): Nome da coluna da data.
        value_columns (list): Lista de colunas para as quais os valores nulos serão imputados.
    """
    product_ids = get_product_ids_with_nulls('vendasprodutosexport', 'CodigoProduto', 'Data', ['ValorCusto', 'ValorUnitario'])
    with ThreadPoolExecutor(max_workers=4) as executor:
        for product_id in product_ids:
            for value_column in value_columns:
                executor.submit(process_column, product_id, table, product_column, date_column, value_column)

def get_product_ids_with_nulls(table, product_column, date_column, value_columns):
    """
    Recupera os IDs de produtos que possuem valores nulos em qualquer das colunas especificadas,
    fazendo um join com a tabela 'vendasexport' para acessar a coluna 'Data'.
    """
    value_columns_condition = ' OR '.join([f"{table}.{col} IS NULL" for col in value_columns])
    query = f"""
    SELECT DISTINCT {table}.{product_column}
    FROM {table}
    JOIN vendasexport ON {table}.CodigoVenda = vendasexport.Codigo
    WHERE {value_columns_condition} OR vendasexport.{date_column} IS NULL;
    """
    
    try:
        result = db_manager.execute_query(query)
        if result and 'data' in result:
            product_ids = [row[0] for row in result['data']]
            logger.info(f"IDs de produtos com valores nulos recuperados com sucesso na tabela {table}.")
            return product_ids
        else:
            logger.warning("Consulta bem-sucedida, mas nenhum dado encontrado.")
            return []
    except Exception as e:
        logger.error(f"Erro ao recuperar IDs de produtos com valores nulos na tabela {table}: {e}")
        return []