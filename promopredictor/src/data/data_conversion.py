import pandas as pd
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger
from sqlalchemy.exc import SQLAlchemyError

logger = get_logger(__name__)

def convert_data_types(table_name, type_conversions):
    """
    Converte os tipos de dados de colunas específicas em uma tabela.
    Args:
        table_name (str): Nome da tabela na qual os tipos de dados serão convertidos.
        type_conversions (dict): Dicionário contendo as conversões de tipo para cada coluna.
    """
    engine = get_db_connection() 
    if engine:
        try:
        
            data = pd.read_sql_table(table_name, engine)
            
            for column, conversion in type_conversions.items():
                data[column] = data[column].astype(conversion)
            
        
            data.to_sql(table_name, engine, if_exists='replace', index=False)
            logger.info(f"Tipos de dados convertidos na tabela '{table_name}'.")
        except SQLAlchemyError as e:
            logger.error(f"Erro ao converter tipos de dados na tabela '{table_name}': {e}")
        finally:
            engine.dispose() 
