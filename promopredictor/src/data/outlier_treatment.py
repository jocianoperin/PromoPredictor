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
        # Utiliza o engine do SQLAlchemy com pandas para ler a consulta SQL.
        query = f"SELECT * FROM {table_name}"
        data = pd.read_sql_query(query, db_manager.engine)
        
        # Identificar outliers usando o método dos quartis
        Q1 = data['ValorUnitario'].quantile(0.25)
        Q3 = data['ValorUnitario'].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        # Tratar outliers removendo-os do DataFrame
        filtered_data = data[~((data['ValorUnitario'] > upper_bound) | (data['ValorUnitario'] < lower_bound))]
        
        # Atualizar a tabela com os dados tratados
        filtered_data.to_sql(table_name, db_manager.engine, if_exists='replace', index=False)
        logger.info(f"Outliers identificados e tratados na tabela '{table_name}'.")
    except Exception as e:
        logger.error(f"Erro ao identificar e tratar outliers na tabela '{table_name}': {e}")
    finally:
        db_manager.engine.dispose()
