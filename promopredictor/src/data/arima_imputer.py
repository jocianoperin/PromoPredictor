from concurrent.futures import ThreadPoolExecutor
from src.models.arima_model import train_arima, forecast_arima, insert_arima_predictions
from src.services.database import db_manager
from src.utils.logging_config import get_logger
import pandas as pd

logger = get_logger(__name__)

def save_model_config(product_id, export_id, value_column, model, model_type, additional_info=None):
    """
    Salva a configuração do modelo no banco de dados.

    Args:
        product_id (int): ID do produto para o qual o modelo foi treinado.
        export_id (int): ID da exportação relacionada.
        value_column (str): Coluna para a qual o modelo foi aplicado.
        model (object): Modelo treinado.
        model_type (str): Tipo do modelo ('ARIMA', 'RNN', etc.).
        additional_info (dict): Informações adicionais como parâmetros do modelo.
    """
    insert_query = """
    INSERT INTO model_config (product_id, export_id, value_column, model_type, parameters, aic, bic, date_executed)
    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW());
    """
    params = (product_id, export_id, value_column, model_type, str(additional_info), model.aic, model.bic)
    try:
        db_manager.execute_query(insert_query, params)
        logger.info(f"Configuração do modelo {model_type} salva com sucesso para o produto {product_id} na coluna {value_column}.")
    except Exception as e:
        logger.error(f"Erro ao salvar configuração do modelo: {e}")

def get_data_for_product(table, product_column, date_column, value_column, product_id):
    """
    Obtém todos os dados (nulos e não nulos) para um determinado produto e coluna de valor.

    Args:
        table (str): Nome da tabela.
        product_column (str): Nome da coluna que identifica o produto.
        date_column (str): Nome da coluna que identifica a data.
        value_column (str): Nome da coluna cujos valores serão obtidos.
        product_id (int): ID do produto.

    Returns:
        DataFrame: Dados da série temporal para o produto e coluna especificados.
    """
    query = f"""
    SELECT vendasprodutosexport.ExportID, {date_column}, {table}.{product_column}, {value_column}
    FROM {table}
    JOIN vendasexport ON {table}.CodigoVenda = vendasexport.Codigo
    WHERE {product_column} = %s
    ORDER BY {date_column};
    """
    try:
        result = db_manager.execute_query(query, [product_id])
        if result and 'data' in result:
            df = pd.DataFrame(result['data'], columns=['ExportID', date_column, 'CodigoProduto', value_column])
            return df
        else:
            logger.warning(f"Nenhum dado encontrado para o produto {product_id} na coluna {value_column}.")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Erro ao obter dados para o produto {product_id} na coluna {value_column}: {e}")
        return pd.DataFrame()

def process_column(table, product_column, date_column, value_column, product_id):
    """
    Processa uma coluna específica para imputação de valores nulos usando o modelo ARIMA.

    Args:
        table (str): Nome da tabela.
        product_column (str): Nome da coluna do produto.
        date_column (str): Nome da coluna da data.
        value_column (str): Nome da coluna cujos valores nulos serão imputados.
        product_id (int): ID do produto.
    """
    data = get_data_for_product(table, product_column, date_column, value_column, product_id)

    if data.empty:
        logger.warning(f"Não há dados suficientes para treinar o modelo ARIMA para o produto {product_id} na coluna {value_column}.")
        return

    # Separar dados nulos e não nulos
    non_null_data = data[data[value_column].notnull()]
    null_data = data[data[value_column].isnull()]

    if non_null_data.empty or null_data.empty:
        logger.warning(f"Não há dados suficientes para treinar o modelo ARIMA para o produto {product_id} na coluna {value_column}.")
        return

    model = train_arima(non_null_data)

    if model:
        forecast = forecast_arima(model, steps=len(null_data))
        if forecast is not None:
            insert_arima_predictions(table, product_column, date_column, value_column, product_id, null_data, forecast)

            # Obter o export_id (opcional)
            export_id = null_data['ExportID'].iloc[0] if not null_data.empty else None

            save_model_config(product_id, export_id, value_column, model, 'ARIMA', {'p': 1, 'd': 1, 'q': 1})
            logger.info(f"Valores nulos imputados com sucesso para o produto {product_id} na coluna {value_column}.")
        else:
            logger.error(f"Falha ao realizar previsões para o produto {product_id} na coluna {value_column}.")
    else:
        logger.error(f"Falha ao treinar o modelo ARIMA para o produto {product_id} na coluna {value_column}.")


def impute_null_values(table, product_column, date_column, value_columns):
    """
    Identifica produtos com valores nulos e executa a imputação usando ARIMA para cada coluna especificada de forma paralela.

    Args:
        table (str): Nome da tabela.
        product_column (str): Nome da coluna do produto.
        date_column (str): Nome da coluna da data.
        value_columns (list): Lista de colunas para as quais os valores nulos serão imputados.
    """
    products_with_nulls = get_products_with_nulls(table, product_column, value_columns)

    with ThreadPoolExecutor(max_workers=4) as executor:
        for value_column, product_ids in products_with_nulls.items():
            for product_id in product_ids:
                executor.submit(process_column, table, product_column, date_column, value_column, product_id)


def get_products_with_nulls(table, product_column, value_columns):
    """
    Recupera os IDs de produtos que possuem valores nulos em qualquer uma das colunas especificadas.

    Args:
        table (str): Nome da tabela que contém os dados.
        product_column (str): Nome da coluna que identifica os produtos.
        value_columns (list): Lista das colunas que contêm os valores a serem imputados.

    Returns:
        dict: Dicionário com os IDs de produtos agrupados por coluna com valores nulos.
    """
    column_placeholders = ' OR '.join([f"{value_column} IS NULL" for value_column in value_columns])
    query = f"""
    SELECT DISTINCT {table}.{product_column}, {', '.join([f"{value_column} IS NULL AS {value_column}_null" for value_column in value_columns])}
    FROM {table}
    JOIN vendasexport ON {table}.CodigoVenda = vendasexport.Codigo
    WHERE {column_placeholders};
    """
    logger.info(f"IDs de produtos que possuem valores nulos em qualquer uma das colunas especificadas: {query}")
    try:
        result = db_manager.execute_query(query)
        if result and 'data' in result:
            products_with_nulls = {}
            for value_column in value_columns:
                products_with_nulls[value_column] = [row[0] for row in result['data'] if row[value_columns.index(value_column) + 1]]
            return products_with_nulls
        else:
            logger.warning("Consulta bem-sucedida, mas nenhum dado encontrado.")
            return {value_column: [] for value_column in value_columns}
    except Exception as e:
        logger.error(f"Erro ao recuperar IDs de produtos com valores nulos nas colunas {', '.join(value_columns)} da tabela {table}: {e}")
        return {value_column: [] for value_column in value_columns}