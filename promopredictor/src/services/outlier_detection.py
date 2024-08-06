import pandas as pd
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def detect_and_remove_outliers(table_name, columns, window_size=10, threshold=3):
    """
    Detecta e remove outliers em uma tabela para as colunas especificadas, considerando variações temporais.
    Args:
        table_name (str): Nome da tabela onde os outliers serão verificados.
        columns (list of str): Lista de colunas onde os outliers devem ser verificados.
        window_size (int): Tamanho da janela deslizante para cálculo das estatísticas móveis.
        threshold (float): Número de desvios padrão para determinar um outlier.
    """
    try:
        for column in columns:
            # Ajustar a consulta para selecionar a coluna de identificação correta
            query = f"SELECT ExportID, {column} FROM {table_name} ORDER BY ExportID"
            result = db_manager.execute_query(query)

            if 'data' in result and 'columns' in result:
                data = pd.DataFrame(result['data'], columns=result['columns'])
                data[column] = data[column].astype(float)  # Converte os valores para float

                # Calcula a média móvel e o desvio padrão móvel
                rolling_mean = data[column].rolling(window=window_size).mean()
                rolling_std = data[column].rolling(window=window_size).std()

                # Identifica outliers como pontos que estão fora do limite de threshold * std
                outliers = data[(data[column] > rolling_mean + threshold * rolling_std) |
                                (data[column] < rolling_mean - threshold * rolling_std)]

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
                    logger.info(f"DELETE na tabela '{table_name}': {affected_rows['rows_affected']} linhas foram removidas da coluna '{column}' porque foram identificadas como outliers com base na análise de média móvel e desvio padrão móvel.")
            else:
                logger.error(f"Erro ao executar consulta SQL: {result}")
    except Exception as e:
        logger.error(f"Erro ao detectar e remover outliers na tabela '{table_name}': {e}")