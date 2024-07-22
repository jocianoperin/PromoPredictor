import pandas as pd
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def detect_and_remove_outliers(table_name, columns):
    """
    Detecta e remove outliers em uma tabela para as colunas especificadas.
    Args:
        table_name (str): Nome da tabela onde os outliers serão verificados.
        columns (list of str): Lista de colunas onde os outliers devem ser verificados.
    """
    try:
        for column in columns:
            query = f"SELECT {column} FROM {table_name}"
            result = db_manager.execute_query(query)

            if 'data' in result and 'columns' in result:
                data = pd.DataFrame(result['data'], columns=result['columns'])
                data[column] = data[column].astype(float)  # Converte os valores para float

                Q1 = data[column].quantile(0.25)
                Q3 = data[column].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR

                delete_query = f"DELETE FROM {table_name} WHERE {column} < {lower_bound} OR {column} > {upper_bound}"
                affected_rows = db_manager.execute_query(delete_query)
                logger.info(f"DELETE na tabela '{table_name}': {affected_rows['rows_affected']} linhas removidas por serem outliers na coluna '{column}'.")
            else:
                logger.error(f"Erro ao executar consulta SQL: {result}")
    except Exception as e:
        logger.error(f"Erro ao detectar e remover outliers na tabela '{table_name}': {e}")

# Exemplo de uso:
# detect_and_remove_outliers('vendasexport', ['totalpedido', 'totalcusto'])
# detect_and_remove_outliers('vendasprodutosexport', ['valortabela', 'valorunitario', 'valorcusto'])
