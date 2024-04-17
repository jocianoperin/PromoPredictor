# src/data/data_cleaner.py
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def execute_query(query):
    connection = get_db_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                affected_rows = cursor.rowcount
                connection.commit()
                return affected_rows
        except Exception as e:
            logger.error(f"Erro durante a execução da query: {e}")
            connection.rollback()
        finally:
            connection.close()
    return 0

def delete_data(table_name, condition):
    delete_query = f"DELETE FROM {table_name} WHERE {condition}"
    affected_rows = execute_query(delete_query)
    logger.info(f"DELETE na tabela '{table_name}': {affected_rows} linhas removidas sob a condição '{condition}'.")

def update_data(table_name, updates, condition):
    update_query = f"UPDATE {table_name} SET {updates} WHERE {condition}"
    affected_rows = execute_query(update_query)
    logger.info(f"UPDATE na tabela '{table_name}': {affected_rows} linhas atualizadas sob a condição '{condition}'.")

def clean_null_values(table_name, columns):
    """
    Atualiza valores NULL para 0 para as colunas especificadas de uma tabela.
    
    Args:
    - table_name (str): Nome da tabela a ser atualizada.
    - columns (list): Lista de colunas para as quais os valores NULL serão atualizados.
    """
    for column in columns:
        condition = f"{column} IS NULL"
        updates = f"{column} = 0"
        affected_rows = update_data(table_name, updates, condition)
        logger.info(f"Limpeza na tabela '{table_name}': {affected_rows} linhas atualizadas onde {column} era NULL.")

def remove_duplicates(table_name):
    """
    Remove registros duplicados de uma tabela.
    Args:
        table_name (str): Nome da tabela da qual serão removidos os duplicados.
    """
    connection = get_db_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                query = f"CREATE TEMPORARY TABLE temp_{table_name} SELECT DISTINCT * FROM {table_name}"
                cursor.execute(query)
                query = f"TRUNCATE TABLE {table_name}"
                cursor.execute(query)
                query = f"INSERT INTO {table_name} SELECT * FROM temp_{table_name}"
                cursor.execute(query)
                connection.commit()
            logger.info(f"Registros duplicados removidos da tabela '{table_name}'.")
        except Exception as e:
            logger.error(f"Erro ao remover registros duplicados da tabela '{table_name}': {e}")
            connection.rollback()
        finally:
            connection.close()

def remove_invalid_records(table_name, conditions):
    """
    Remove registros inválidos de uma tabela com base em condições específicas.
    Args:
        table_name (str): Nome da tabela da qual serão removidos os registros inválidos.
        conditions (list): Lista de condições para identificar registros inválidos.
    """
    connection = get_db_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                for condition in conditions:
                    delete_query = f"DELETE FROM {table_name} WHERE {condition}"
                    cursor.execute(delete_query)
                    affected_rows = cursor.rowcount
                    logger.info(f"DELETE na tabela '{table_name}': {affected_rows} linhas removidas sob a condição '{condition}'.")
                connection.commit()
        except Exception as e:
            logger.error(f"Erro ao remover registros inválidos da tabela '{table_name}': {e}")
            connection.rollback()
        finally:
            connection.close()

def standardize_formatting(table_name, formatting_rules):
    """
    Padroniza a formatação de registros em uma tabela com base em regras específicas.
    Args:
        table_name (str): Nome da tabela na qual a formatação será padronizada.
        formatting_rules (dict): Dicionário contendo as regras de formatação para cada coluna.
    """
    connection = get_db_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                for column, rule in formatting_rules.items():
                    update_query = f"UPDATE {table_name} SET {column} = {rule}({column})"
                    cursor.execute(update_query)
                    affected_rows = cursor.rowcount
                    logger.info(f"UPDATE na tabela '{table_name}': {affected_rows} linhas atualizadas para padronizar a formatação da coluna '{column}'.")
                connection.commit()
        except Exception as e:
            logger.error(f"Erro ao padronizar a formatação de registros na tabela '{table_name}': {e}")
            connection.rollback()
        finally:
            connection.close()