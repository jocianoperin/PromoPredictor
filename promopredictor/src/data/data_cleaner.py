from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def delete_data(table_name, condition):
    """
    Deleta registros de uma tabela baseado em uma condição especificada.
    Args:
        table_name (str): Nome da tabela onde os registros serão deletados.
        condition (str): Condição para identificar quais registros devem ser deletados.
    """
    delete_query = f"DELETE FROM {table_name} WHERE {condition}"
    affected_rows = db_manager.execute_query(delete_query)
    logger.info(f"DELETE na tabela '{table_name}': {affected_rows} linhas removidas sob a condição '{condition}'.")

def update_data(table_name, updates, condition):
    """
    Atualiza registros em uma tabela baseado em uma condição especificada.
    Args:
        table_name (str): Nome da tabela a ser atualizada.
        updates (str): Instruções SQL de atualização.
        condition (str): Condição para identificar quais registros devem ser atualizados.
    """
    update_query = f"UPDATE {table_name} SET {updates} WHERE {condition}"
    affected_rows = db_manager.execute_query(update_query)
    logger.info(f"UPDATE na tabela '{table_name}': {affected_rows} linhas atualizadas sob a condição '{condition}'.")

def clean_null_values(table_name, columns):
    """
    Atualiza valores NULL para 0 nas colunas especificadas de uma tabela.
    Args:
        table_name (str): Nome da tabela a ser atualizada.
        columns (list of str): Lista de colunas onde os valores NULL devem ser atualizados para 0.
    """
    for column in columns:
        condition = f"{column} IS NULL"
        updates = f"{column} = 0"
        affected_rows = update_data(table_name, updates, condition)
        logger.info(f"Limpeza na tabela '{table_name}': {affected_rows} linhas atualizadas onde {column} era NULL.")

def remove_duplicates(table_name):
    """Remove registros duplicados de uma tabela.

    Args:
        table_name (str): Nome da tabela da qual os registros duplicados serão removidos.
    """
    primary_key_columns = db_manager.get_primary_key_columns(table_name)
    if primary_key_columns:
        primary_key_clause = ', '.join(primary_key_columns)
        query = f"""
            DELETE FROM {table_name}
            WHERE ({primary_key_clause}) NOT IN (
                SELECT {primary_key_clause}
                FROM (
                    SELECT {primary_key_clause}, ROW_NUMBER() OVER (PARTITION BY {primary_key_clause} ORDER BY {primary_key_clause}) AS row_num
                    FROM {table_name}
                ) AS deduped
                WHERE row_num = 1
            );
        """
    else:
        all_columns = db_manager.get_all_columns(table_name)
        query = f"""
            DELETE FROM {table_name}
            WHERE ({', '.join(all_columns)}) NOT IN (
                SELECT {', '.join(all_columns)}
                FROM (
                    SELECT {', '.join(all_columns)}, ROW_NUMBER() OVER (PARTITION BY {', '.join(all_columns)} ORDER BY {', '.join(all_columns)}) AS row_num
                    FROM {table_name}
                ) AS deduped
                WHERE row_num = 1
            );
        """

    success = db_manager.execute_query(query)
    if success:
        logger.info(f"Registros duplicados removidos da tabela '{table_name}'.")
    else:
        logger.error(f"Erro ao remover registros duplicados da tabela '{table_name}'.")

def remove_invalid_records(table_name, conditions):
    """
    Remove registros inválidos de uma tabela com base em condições específicas.
    Args:
        table_name (str): Nome da tabela da qual os registros inválidos serão removidos.
        conditions (list of str): Condições para identificar quais registros são considerados inválidos.
    """
    for condition in conditions:
        delete_data(table_name, condition)

def get_primary_key_columns(self, table_name):
    """
    Obtém os nomes das colunas que compõem a chave primária de uma tabela.

    Args:
        table_name (str): Nome da tabela.

    Returns:
        list: Lista com os nomes das colunas que compõem a chave primária.
    """
    if self.use_sqlalchemy:
        # Implementação para SQLAlchemy
        pass
    else:
        query = f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'"
        result = self.execute_query(query)
        primary_key_columns = [row[4] for row in result]
        return primary_key_columns

def get_all_columns(self, table_name):
    """
    Obtém os nomes de todas as colunas de uma tabela.

    Args:
        table_name (str): Nome da tabela.

    Returns:
        list: Lista com os nomes de todas as colunas da tabela.
    """
    if self.use_sqlalchemy:
        # Implementação para SQLAlchemy
        pass
    else:
        query = f"DESCRIBE {table_name}"
        result = self.execute_query(query)
        column_names = [row[0] for row in result]
        return column_names

def standardize_formatting(table_name, formatting_rules):
    """
    Padroniza a formatação de registros em uma tabela com base em regras específicas.
    Args:
        table_name (str): Nome da tabela onde a formatação será padronizada.
        formatting_rules (dict): Dicionário contendo as regras de formatação para cada coluna.
    """
    for column, rule in formatting_rules.items():
        update_query = f"UPDATE {table_name} SET {column} = {rule}({column})"
        affected_rows = db_manager.execute_query(update_query)
        logger.info(f"UPDATE na tabela '{table_name}': {affected_rows} linhas atualizadas para padronizar a formatação da coluna '{column}'.")
