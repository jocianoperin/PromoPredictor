import pandas as pd
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger
from sqlalchemy.exc import SQLAlchemyError

logger = get_logger(__name__)

def identify_and_treat_outliers(table_name):
    """
    Identifica e trata outliers em uma tabela com base em técnicas estatísticas.
    Args:
        table_name (str): Nome da tabela na qual os outliers serão identificados e tratados.
    """
    engine = get_db_connection()  # Assume que get_db_connection retorna um engine SQLAlchemy.
    try:
        # Utiliza o engine do SQLAlchemy com pandas para ler a consulta SQL.
        data = pd.read_sql_table(table_name, engine)
        
        # Identificar outliers usando o método dos quartis
        Q1 = data['ValorUnitario'].quantile(0.25)
        Q3 = data['ValorUnitario'].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        # Tratar outliers removendo-os do DataFrame
        data = data.drop(data[(data['ValorUnitario'] > upper_bound) | (data['ValorUnitario'] < lower_bound)].index)
        
        # Atualizar a tabela com os dados tratados
        data.to_sql(table_name, engine, if_exists='replace', index=False)
        logger.info(f"Outliers identificados e tratados na tabela '{table_name}'.")
    except SQLAlchemyError as e:
        logger.error(f"Erro ao identificar e tratar outliers na tabela '{table_name}': {e}")
    finally:
        engine.dispose()
