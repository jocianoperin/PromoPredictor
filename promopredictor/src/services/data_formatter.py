from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

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

def check_data_types(table_name, column_types):
    """
    Verifica os tipos de dados em uma tabela e corrige se necessário.

    Args:
        table_name (str): Nome da tabela onde os tipos de dados serão verificados.
        column_types (dict): Dicionário contendo os tipos de dados esperados para cada coluna.

    Retorna:
        None: A função não retorna valores, mas ajusta os tipos de dados das colunas conforme especificado.
    """
    
    for column, expected_type in column_types.items():
        alter_query = f"ALTER TABLE {table_name} MODIFY COLUMN {column} {expected_type}"
        db_manager.execute_query(alter_query)
        logger.info(f"ALTER TABLE '{table_name}': Tipo de dados da coluna '{column}' atualizado para '{expected_type}'.")