import pandas as pd
import matplotlib.pyplot as plt
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def explore_data(table_name):
    """
    Realiza análises exploratórias em uma tabela.
    Args:
        table_name (str): Nome da tabela na qual as análises exploratórias serão realizadas.
    """
    connection = get_db_connection()
    if connection:
        try:
            query = f"SELECT * FROM {table_name}"
            data = pd.read_sql_query(query, connection)
            
            # Análise de dados ausentes
            missing_data = data.isnull().sum()
            logger.info(f"Dados ausentes na tabela '{table_name}':\n{missing_data}")
            
            # Estatísticas descritivas
            descriptive_stats = data.describe()
            logger.info(f"Estatísticas descritivas da tabela '{table_name}':\n{descriptive_stats}")
            
            # Visualizações (histogramas, gráficos de dispersão, etc.)
            # Por exemplo:
            data.hist(bins=30, figsize=(12, 8))
            plt.tight_layout()
            plt.show()
            
            # Adicione outras análises exploratórias conforme necessário
            
        except Exception as e:
            logger.error(f"Erro ao realizar análises exploratórias na tabela '{table_name}': {e}")
        finally:
            connection.close()