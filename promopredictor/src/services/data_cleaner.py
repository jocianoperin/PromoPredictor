from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def delete_data(table_name, condition):
    """
    Deleta registros de uma tabela baseado em uma condição especificada.

    Args:
        table_name (str): Nome da tabela onde os registros serão deletados.
        condition (str): Condição para identificar quais registros devem ser deletados.

    Retorna:
        None: A função não retorna valores, mas remove os registros da tabela com base na condição especificada.
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

    Retorna:
        None: A função não retorna valores, mas atualiza os registros da tabela com base na condição especificada.
    """

    update_query = f"UPDATE {table_name} SET {updates} WHERE {condition}"
    affected_rows = db_manager.execute_query(update_query)
    logger.info(f"UPDATE na tabela '{table_name}': {affected_rows} linhas atualizadas sob a condição '{condition}'.")

def clean_null_values(table_name, columns):
    """
    Remove registros com valores NULL nas colunas especificadas de uma tabela.

    Args:
        table_name (str): Nome da tabela de onde os registros serão removidos.
        columns (list of str): Lista de colunas onde os valores NULL devem ser removidos.

    Retorna:
        None: A função não retorna valores, mas remove os registros que contêm valores NULL nas colunas especificadas.
    """

    for column in columns:
        condition = f"{column} IS NULL"
        delete_data(table_name, condition)
        logger.info(f"Registros removidos da tabela '{table_name}' onde {column} era NULL.")

def remove_invalid_records(table_name, conditions):
    """
    Remove registros inválidos de uma tabela com base em condições específicas.

    Args:
        table_name (str): Nome da tabela da qual os registros inválidos serão removidos.
        conditions (list of str): Condições para identificar quais registros são considerados inválidos.

    Retorna:
        None: A função não retorna valores, mas remove os registros que correspondem às condições inválidas especificadas.
    """

    for condition in conditions:
        delete_data(table_name, condition)

def remove_duplicates(table_name):
    """
    Remove registros duplicados de uma tabela, considerando todas as colunas exceto a chave primária.

    Args:
        table_name (str): Nome da tabela da qual os registros duplicados serão removidos.

    Retorna:
        None: A função não retorna valores, mas remove os registros duplicados da tabela especificada.
    """

    primary_key_columns = get_primary_key_columns(table_name)
    all_columns = get_all_columns(table_name)
    
    if not all_columns:
        logger.error(f"Nenhuma coluna encontrada para a tabela {table_name}. Impossível remover duplicatas.")
        return

    if primary_key_columns:
        non_pk_columns = [col for col in all_columns if col not in primary_key_columns]
    else:
        logger.info(f"Nenhuma chave primária encontrada, usando todas as colunas para a tabela {table_name}.")
        non_pk_columns = all_columns

    if not non_pk_columns:
        logger.error(f"Todas as colunas são chaves primárias, não existem colunas não primárias para verificar duplicatas.")
        return

    non_pk_clause = ', '.join(non_pk_columns)

    # Usando ROW_NUMBER para identificar e manter apenas uma linha de cada grupo de duplicatas
    query = f"""
        DELETE t1 FROM {table_name} t1
        JOIN (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {non_pk_clause} ORDER BY {non_pk_clause}) as row_num
            FROM {table_name}
        ) t2 ON t1.{primary_key_columns[0]} = t2.{primary_key_columns[0]}
        WHERE t2.row_num > 1;
    """

    logger.debug(f"A query a ser executada é: {query}")
    try:
        success = db_manager.execute_query(query)
        if success:
            logger.info(f"Registros duplicados removidos da tabela '{table_name}'.")
        else:
            logger.error(f"Erro ao remover registros duplicados da tabela '{table_name}'. A query pode ter sido executada, mas sem sucesso na remoção.")
    except Exception as e:
        logger.error(f"Erro ao executar a remoção de duplicatas na tabela {table_name}: {e}")

def get_primary_key_columns(table_name):
    """
    Obtém os nomes das colunas que compõem a chave primária de uma tabela.

    Args:
        table_name (str): Nome da tabela.

    Retorna:
        list: Lista com os nomes das colunas que compõem a chave primária da tabela especificada.
    """

    logger.info(f"Obtendo chaves primárias da tabela {table_name}")

    query = f"SELECT column_name FROM information_schema.key_column_usage WHERE table_name = '{table_name}' AND constraint_name = 'PRIMARY'"
    
    try:
        result = db_manager.execute_query(query)
        if not result:
            logger.error(f"Nenhuma chave primária encontrada para a tabela {table_name}.")
            return []
        primary_key_columns = [row[0] for row in result]
        return primary_key_columns
    except Exception as e:
        logger.error(f"Erro ao processar chaves primárias para a tabela {table_name}: {e}")
        return []

def get_all_columns(table_name):
    """
    Obtém os nomes de todas as colunas de uma tabela.

    Args:
        table_name (str): Nome da tabela.

    Retorna:
        list: Lista com os nomes de todas as colunas da tabela especificada.
    """

    logger.info(f"Obtendo todas as colunas da tabela {table_name}")
    
    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
    try:
        result = db_manager.execute_query(query)
        if not result:
            logger.error(f"Nenhuma coluna encontrada para a tabela {table_name}.")
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

    Retorna:
        None: A função não retorna valores, mas atualiza a formatação dos registros na tabela especificada.
    """
    
    for column, rule in formatting_rules.items():
        update_query = f"UPDATE {table_name} SET {column} = {rule}({column})"
        affected_rows = db_manager.execute_query(update_query)
        logger.info(f"UPDATE na tabela '{table_name}': {affected_rows} linhas atualizadas para padronizar a formatação da coluna '{column}'.")