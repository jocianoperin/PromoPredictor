from decimal import Decimal
import pandas as pd
from src.services.database import db_manager
from src.utils.logging_config import get_logger
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = get_logger(__name__)

def treat_column_outliers(data, column_name):
    """Função para tratar outliers em uma coluna específica de um DataFrame."""
    data[column_name] = data[column_name].apply(lambda x: float(x) if isinstance(x, Decimal) else x)
    Q1 = data[column_name].quantile(0.25)
    Q3 = data[column_name].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return data[~((data[column_name] > upper_bound) | (data[column_name] < lower_bound))]

def update_database(data, column_list, table_name):
    """Função para atualizar o banco de dados com dados tratados."""
    updated_data = data.to_dict('records')
    for record in updated_data:
        update_values = ', '.join([f"{col} = %s" for col in column_list])
        update_params = [record[col] for col in column_list]
        update_query = f"UPDATE {table_name} SET {update_values} WHERE ExportID = %s"
        update_params.append(record['ExportID'])
        db_manager.execute_query(update_query, update_params)

def identify_and_treat_outliers(table_name, column_names):
    """Identifica e trata outliers em múltiplas colunas de uma tabela."""
    logger.info(f"Iniciando a trativa de outliers nas colunas {column_names} da tabela {table_name}.")
    try:
        column_list = [column.strip() for column in column_names.split(',')]
        query = f"SELECT * FROM {table_name}"
        result = db_manager.execute_query(query)

        if not result['data']:
            logger.info(f"Nenhum dado retornado para a tabela {table_name}.")
            return

        data = pd.DataFrame(result['data'], columns=result['columns'])

        with ThreadPoolExecutor(max_workers=len(column_list)) as executor:
            futures = {executor.submit(treat_column_outliers, data.copy(), column): column for column in column_list}
            for future in as_completed(futures):
                column = futures[future]
                data = future.result()
                logger.info(f"Outliers tratados para a coluna {column}.")

        update_database(data, column_list, table_name)

        logger.info(f"Outliers identificados e tratados nas colunas '{column_names}' da tabela '{table_name}'.")
    except Exception as e:
        logger.error(f"Erro ao identificar e tratar outliers nas colunas '{column_names}' da tabela '{table_name}': {e}")
