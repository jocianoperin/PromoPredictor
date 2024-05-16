from concurrent.futures import ThreadPoolExecutor
from src.data.time_series_preparation import prepare_time_series_data, get_non_null_data
from src.models.arima_model import train_arima, forecast_arima, impute_values
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def save_model_config(product_id, value_column, model, model_type, additional_info=None):
    """
    Salva a configuração do modelo no banco de dados.
    Args:
        product_id (int): ID do produto para o qual o modelo foi treinado.
        value_column (str): Coluna para a qual o modelo foi aplicado.
        model (object): Modelo treinado.
        model_type (str): Tipo do modelo ('ARIMA', 'RNN', etc.).
        additional_info (dict): Informações adicionais como parâmetros do modelo.
    """
    insert_query = f"""
    INSERT INTO model_config (product_id, value_column, model_type, parameters, aic, bic, date_executed)
    VALUES (%s, %s, %s, %s, %s, %s, NOW());
    """
    params = (product_id, value_column, model_type, str(additional_info), model.aic, model.bic)
    try:
        db_manager.execute_query(insert_query, params)
        logger.info(f"Configuração do modelo {model_type} salva com sucesso para o produto {product_id}.")
    except Exception as e:
        logger.error(f"Erro ao salvar configuração do modelo: {e}")

def process_column(product_id, table, product_column, date_column, value_column):
    """
    Processa uma coluna específica para imputação de valores nulos usando o modelo ARIMA.

    Args:
        product_id (int): ID do produto.
        table (str): Nome da tabela.
        product_column (str): Nome da coluna que identifica o produto.
        date_column (str): Nome da coluna que identifica a data.
        value_column (str): Nome da coluna cujos valores nulos serão imputados.

    Etapas:
        1. Obter os dados sem valores nulos para treinar o modelo ARIMA.
        2. Obter os dados com valores nulos para imputação.
        3. Treinar o modelo ARIMA com os dados sem valores nulos.
        4. Realizar previsões com o modelo ARIMA para os dados com valores nulos.
        5. Imputar os valores previstos nos dados com valores nulos.
        6. Salvar a configuração do modelo ARIMA no banco de dados.
    """
    non_null_data = get_non_null_data(table, product_column, date_column, value_column, product_id)
    null_data = prepare_time_series_data(table, product_column, date_column, value_column, product_id)

    if non_null_data.empty or null_data.empty:
        logger.warning(f"Não há dados suficientes para treinar o modelo ARIMA para o produto {product_id} na coluna {value_column}.")
        return

    model = train_arima(non_null_data)
    if model:
        forecast = forecast_arima(model, steps=len(null_data))
        impute_values(table, product_column, date_column, value_column, product_id, null_data, forecast)
        save_model_config(product_id, value_column, model, 'ARIMA', {'p': 1, 'd': 1, 'q': 1})
        logger.info(f"Valores nulos imputados com sucesso para o produto {product_id} na coluna {value_column}.")
    else:
        logger.error(f"Falha ao treinar o modelo ARIMA para o produto {product_id} na coluna {value_column}.")

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
    WHERE {value_columns_condition};
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