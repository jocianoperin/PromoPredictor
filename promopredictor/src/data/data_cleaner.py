from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text

logger = get_logger(__name__)

def execute_query(query):
    connection = get_db_connection()
    if connection:
        try:
            with connection.begin() as transaction:
                result = transaction.execute(text(query))
                affected_rows = result.rowcount
                return affected_rows
        except SQLAlchemyError as e:
            logger.error(f"Erro durante a execução da query: {e}")
            if connection:
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
    """
    for column in columns:
        condition = f"{column} IS NULL"
        updates = f"{column} = 0"
        affected_rows = update_data(table_name, updates, condition)
        logger.info(f"Limpeza na tabela '{table_name}': {affected_rows} linhas atualizadas onde {column} era NULL.")

def remove_duplicates(table_name):
    """
    Remove registros duplicados de uma tabela.
    """
    connection = get_db_connection()
    if connection:
        try:
            with connection.begin() as transaction:
                transaction.execute(text(f"CREATE TEMPORARY TABLE temp_{table_name} SELECT DISTINCT * FROM {table_name}"))
                transaction.execute(text(f"TRUNCATE TABLE {table_name}"))
                transaction.execute(text(f"INSERT INTO {table_name} SELECT * FROM temp_{table_name}"))
            logger.info(f"Registros duplicados removidos da tabela '{table_name}'.")
        except SQLAlchemyError as e:
            logger.error(f"Erro ao remover registros duplicados da tabela '{table_name}': {e}")
        finally:
            connection.close()

def remove_invalid_records(table_name, conditions):
    """
    Remove registros inválidos de uma tabela com base em condições específicas.
    """
    for condition in conditions:
        delete_data(table_name, condition)

def standardize_formatting(table_name, formatting_rules):
    """
    Padroniza a formatação de registros em uma tabela com base em regras específicas.
    """
    for column, rule in formatting_rules.items():
        update_query = f"UPDATE {table_name} SET {column} = {rule}({column})"
        affected_rows = execute_query(update_query)
        logger.info(f"UPDATE na tabela '{table_name}': {affected_rows} linhas atualizadas para padronizar a formatação da coluna '{column}'.")
