import pandas as pd
from src.services.database import db_manager
from src.utils.logging_config import get_logger
import json

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
            # Ajustar a consulta para selecionar a coluna de identificação correta
            query = f"SELECT ExportID, {column} FROM {table_name}"
            result = db_manager.execute_query(query)

            if 'data' in result and 'columns' in result:
                data = pd.DataFrame(result['data'], columns=result['columns'])
                data[column] = data[column].astype(float)  # Converte os valores para float

                Q1 = data[column].quantile(0.25)
                Q3 = data[column].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR

                # Identificar outliers
                outliers = data[(data[column] < lower_bound) | (data[column] > upper_bound)]
                
                # Inserir outliers na tabela 'outliers'
                for index, row in outliers.iterrows():
                    insert_query = f"""
                    INSERT INTO outliers (original_table, column_name, outlier_value) 
                    VALUES ('{table_name}', '{column}', {row[column]})
                    """
                    db_manager.execute_query(insert_query)

                # Deletar outliers da tabela original
                outlier_ids = outliers['ExportID'].tolist()
                if outlier_ids:
                    delete_query = f"DELETE FROM {table_name} WHERE ExportID IN ({', '.join(map(str, outlier_ids))})"
                    affected_rows = db_manager.execute_query(delete_query)
                    logger.info(f"DELETE na tabela '{table_name}': {affected_rows['rows_affected']} linhas removidas por serem outliers na coluna '{column}'.")
            else:
                logger.error(f"Erro ao executar consulta SQL: {result}")
    except Exception as e:
        logger.error(f"Erro ao detectar e remover outliers na tabela '{table_name}': {e}")