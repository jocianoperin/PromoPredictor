from decimal import Decimal
import pandas as pd
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def identify_and_treat_outliers(table_name, column_names):
    """
    Identifica e trata outliers em múltiplas colunas de uma tabela com base em técnicas estatísticas.
    Args:
        table_name (str): Nome da tabela na qual os outliers serão identificados e tratados.
        column_names (str): String com os nomes das colunas separadas por vírgulas, nas quais os outliers serão identificados e tratados.
    """
    logger.info(f"Iniciando a trativa de outliers nas colunas {column_names} da tabela {table_name}.")
    try:
        column_list = column_names.split(',')
        query = f"SELECT * FROM {table_name}"
        result = db_manager.execute_query(query)

        if not result['data']:
            logger.info(f"Nenhum dado retornado para a tabela {table_name}.")
            return

        # Converter a lista de tuplas em DataFrame
        data = pd.DataFrame(result['data'], columns=result['columns'])

        for column_name in column_list:
            column_name = column_name.strip()
            # Convertendo Decimal para float
            data[column_name] = data[column_name].apply(lambda x: float(x) if isinstance(x, Decimal) else x)

            # Identificar e tratar outliers
            Q1 = data[column_name].quantile(0.25)
            Q3 = data[column_name].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            data = data[~((data[column_name] > upper_bound) | (data[column_name] < lower_bound))]

        # Salvando os dados tratados de volta ao banco de dados
        updated_data = data.to_dict('records')
        for record in updated_data:
            update_values = ', '.join([f"{col} = %s" for col in column_list])
            update_params = [record[col] for col in column_list]
            update_query = f"UPDATE {table_name} SET {update_values} WHERE ExportID = %s"
            update_params.append(record['ExportID'])
            db_manager.execute_query(update_query, update_params)

        logger.info(f"Outliers identificados e tratados nas colunas '{column_names}' da tabela '{table_name}'.")
    except Exception as e:
        logger.error(f"Erro ao identificar e tratar outliers nas colunas '{column_names}' da tabela '{table_name}': {e}")