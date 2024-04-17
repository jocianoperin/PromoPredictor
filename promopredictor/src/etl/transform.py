import pandas as pd
from src.data.data_cleaner import delete_data, update_data, clean_null_values, remove_duplicates, remove_invalid_records, standardize_formatting
from src.data.outlier_treatment import identify_and_treat_outliers
from src.data.data_conversion import convert_data_types
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def transform_vendasexport(vendasexport):
    """
    Realiza transformações nos dados da tabela vendasexport.

    Args:
        vendasexport (pd.DataFrame): DataFrame contendo os dados da tabela vendasexport.

    Returns:
        pd.DataFrame: DataFrame com os dados transformados da tabela vendasexport.
    """
    try:
        remove_duplicates(vendasexport, "vendasexport")
        remove_invalid_records("vendasexport", ["TotalPedido <= 0"])
        standardize_formatting("vendasexport", {})
        identify_and_treat_outliers(vendasexport, "Quantidade")
        convert_data_types(vendasexport, {})
        logger.info("Transformações nos dados da tabela vendasexport realizadas com sucesso.")
        return vendasexport
    except Exception as e:
        logger.error(f"Erro ao transformar os dados da tabela vendasexport: {e}")
    return None

def transform_vendasprodutosexport(vendasprodutosexport):
    """
    Realiza transformações nos dados da tabela vendasprodutosexport.

    Args:
        vendasprodutosexport (pd.DataFrame): DataFrame contendo os dados da tabela vendasprodutosexport.

    Returns:
        pd.DataFrame: DataFrame com os dados transformados da tabela vendasprodutosexport.
    """
    try:
        remove_duplicates(vendasprodutosexport, "vendasprodutosexport")
        remove_invalid_records("vendasprodutosexport", ["ValorTotal <= 0", "Quantidade <= 0"])
        standardize_formatting("vendasprodutosexport", {})
        identify_and_treat_outliers(vendasprodutosexport, "Quantidade")
        convert_data_types(vendasprodutosexport, {})
        logger.info("Transformações nos dados da tabela vendasprodutosexport realizadas com sucesso.")
        return vendasprodutosexport
    except Exception as e:
        logger.error(f"Erro ao transformar os dados da tabela vendasprodutosexport: {e}")
    return None