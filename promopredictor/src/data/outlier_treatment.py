import pandas as pd
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def identify_and_treat_outliers(table_name):
    """
    Identifica e trata outliers em uma tabela com base em técnicas estatísticas.
    Args:
        table_name (str): Nome da tabela na qual os outliers serão identificados e tratados.
    """
    try:
        # Utiliza o execute_query para obter os dados
        query = f"SELECT * FROM {table_name}"
        data = pd.read_sql_query(query, db_manager.engine)  # Alterado para usar diretamente pd.read_sql_query
        
        # Identificar e tratar outliers
        Q1 = data['ValorUnitario'].quantile(0.25)
        Q3 = data['ValorUnitario'].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        filtered_data = data[~((data['ValorUnitario'] > upper_bound) | (data['ValorUnitario'] < lower_bound))]
        
        # Converter DataFrame para SQL
        filtered_data.to_sql(table_name, db_manager.engine, if_exists='replace', index=False)
        logger.info(f"Outliers identificados e tratados na tabela '{table_name}'.")
    except Exception as e:
        logger.error(f"Erro ao identificar e tratar outliers na tabela '{table_name}': {e}")
    finally:
        if db_manager.use_sqlalchemy:
            db_manager.engine.dispose()
