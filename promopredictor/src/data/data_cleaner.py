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
    """
    Remove registros duplicados de uma tabela. Utiliza chaves primárias para identificar duplicatas, caso disponíveis.
    Caso contrário, usa todas as colunas da tabela.

    Args:
        table_name (str): Nome da tabela da qual os registros duplicados serão removidos.
    """
    primary_key_columns = get_primary_key_columns(table_name)
    if primary_key_columns:
        primary_key_clause = ', '.join(primary_key_columns)
        if not primary_key_clause:  # Se a cláusula de chaves primárias estiver vazia, evita executar uma query malformada
            logger.error(f"Erro ao formar cláusula de chave primária para a tabela {table_name}.")
            return
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
        logger.info(f"Nenhuma chave primária encontrada, usando todas as colunas para a tabela {table_name}.")
        all_columns = get_all_columns(table_name)
        if not all_columns:  # Se não houver colunas, evita executar uma query malformada
            logger.error(f"Nenhuma coluna encontrada para a tabela {table_name}. Impossível remover duplicatas.")
            return
        columns_clause = ', '.join(all_columns)
        query = f"""
            DELETE FROM {table_name}
            WHERE ({columns_clause}) NOT IN (
                SELECT {columns_clause}
                FROM (
                    SELECT {columns_clause}, ROW_NUMBER() OVER (PARTITION BY {columns_clause} ORDER BY {columns_clause}) AS row_num
                    FROM {table_name}
                ) AS deduped
                WHERE row_num = 1
            );
        """

    # Executando a query e logando o resultado
    try:
        success = db_manager.execute_query(query)
        if success:
            logger.info(f"Registros duplicados removidos da tabela '{table_name}'.")
        else:
            logger.error(f"Erro ao remover registros duplicados da tabela '{table_name}'. A query pode ter sido executada, mas sem sucesso na remoção.")
    except Exception as e:
        logger.error(f"Erro ao executar a remoção de duplicatas na tabela {table_name}: {e}")

def remove_invalid_records(table_name, conditions):
    """
    Remove registros inválidos de uma tabela com base em condições específicas.
    Args:
        table_name (str): Nome da tabela da qual os registros inválidos serão removidos.
        conditions (list of str): Condições para identificar quais registros são considerados inválidos.
    """
    for condition in conditions:
        delete_data(table_name, condition)

def get_primary_key_columns(table_name):
    """
    Obtém os nomes das colunas que compõem a chave primária de uma tabela.
    Args:
        table_name (str): Nome da tabela.
    Returns:
        list: Lista com os nomes das colunas que compõem a chave primária.
    """
    logger.info(f"Tentando obter chaves primárias da tabela {table_name}")
    query = f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'"
    try:
        result = db_manager.execute_query(query)
        if result is None or len(result) == 0:
            logger.error(f"Erro ao obter chaves primárias para a tabela {table_name} ou nenhuma chave primária encontrada.")
            return []
        primary_key_columns = [row[4] for row in result]  # Ajustar o índice conforme a estrutura real do resultado.
        return primary_key_columns
    except Exception as e:
        logger.error(f"Erro ao processar chaves primárias para a tabela {table_name}: {e}")
        return []

def get_all_columns(table_name):
    """
    Obtém os nomes de todas as colunas de uma tabela.
    Args:
        table_name (str): Nome da tabela.
    Returns:
        list: Lista com os nomes de todas as colunas da tabela.
    """
    logger.info(f"Tentando obter todas as colunas da tabela {table_name}")
    query = f"DESCRIBE {table_name}"
    try:
        result = db_manager.execute_query(query)
        if result is None or len(result) == 0:
            logger.error(f"Erro ao descrever a tabela {table_name} ou nenhuma coluna encontrada.")
            return []
        column_names = [row[0] for row in result]
        return column_names
    except Exception as e:
        logger.error(f"Erro ao tentar obter colunas da tabela {table_name}: {e}")
        return []

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