import pandas as pd
import matplotlib.pyplot as plt
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger
from sqlalchemy.exc import SQLAlchemyError

logger = get_logger(__name__)

def explore_data(table_name):
    """
    Realiza análises exploratórias em uma tabela.
    Args:
        table_name (str): Nome da tabela na qual as análises exploratórias serão realizadas.
    """
    engine = get_db_connection()  # Supõe-se que get_db_connection retorna um engine do SQLAlchemy.
    if engine:
        try:
            # Utilizando engine do SQLAlchemy com pandas para ler a consulta SQL.
            data = pd.read_sql_table(table_name, engine)
            
            # Análise de dados ausentes
            missing_data = data.isnull().sum()
            logger.info(f"Dados ausentes na tabela '{table_name}':\n{missing_data}")
            
            # Estatísticas descritivas
            descriptive_stats = data.describe()
            logger.info(f"Estatísticas descritivas da tabela '{table_name}':\n{descriptive_stats}")
            
            # Visualizações (histogramas, gráficos de dispersão, etc.)
            # Exemplo de histograma:
            data.hist(bins=30, figsize=(12, 8))
            plt.tight_layout()
            plt.show()
            
            # Adicionar outras análises exploratórias conforme necessário

        except SQLAlchemyError as e:
            logger.error(f"Erro ao realizar análises exploratórias na tabela '{table_name}': {e}")
        finally:
            engine.dispose()  # Corretamente descartando o engine.

