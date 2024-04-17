import pandas as pd
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def identify_and_treat_outliers(table_name):
    """
    Identifica e trata outliers em uma tabela com base em técnicas estatísticas.
    Args:
        table_name (str): Nome da tabela na qual os outliers serão identificados e tratados.
    """
    connection = get_db_connection()
    if connection:
        try:
            query = f"SELECT * FROM {table_name}"
            data = pd.read_sql_query(query, connection)
            
            # Identificar outliers usando o método dos quartis
            Q1 = data['Quantidade'].quantile(0.25)
            Q3 = data['Quantidade'].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            # Tratar outliers removendo-os do DataFrame
            data = data.drop(data[(data['Quantidade'] > upper_bound) | (data['Quantidade'] < lower_bound)].index)
            
            # Atualizar a tabela com os dados tratados
            data.to_sql(table_name, connection, if_exists='replace', index=False)
            connection.commit()
            logger.info(f"Outliers identificados e tratados na tabela '{table_name}'.")
        except Exception as e:
            logger.error(f"Erro ao identificar e tratar outliers na tabela '{table_name}': {e}")
            connection.rollback()
        finally:
            connection.close()