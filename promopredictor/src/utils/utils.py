from src.services.database import db_manager
from src.utils.logging_config import get_logger

# Inicializar o logger
logger = get_logger(__name__)

def clear_predictions_table():
    """
    Limpa a tabela de previsões antes de inserir novas previsões.
    """
    try:
        delete_query = "TRUNCATE indicadores_vendas_produtos_previsoes"
        db_manager.execute_query(delete_query)
        logger.info("Tabela de previsões limpa com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao limpar a tabela de previsões: {e}")

def insert_predictions(df_pred):
    """
    Insere as previsões na tabela indicadores_vendas_produtos_previsoes.
    """
    try:
        # Garantir que 'CodigoProduto' contenha o valor original
        df_pred = df_pred.copy()
        values = df_pred.to_dict(orient='records')
        insert_query = """
        INSERT INTO indicadores_vendas_produtos_previsoes (DATA, CodigoProduto, TotalUNVendidas, ValorTotalVendido, Promocao)
        VALUES (:DATA, :CodigoProduto, :TotalUNVendidas, :ValorTotalVendido, :Promocao)
        ON DUPLICATE KEY UPDATE
            TotalUNVendidas = VALUES(TotalUNVendidas),
            ValorTotalVendido = VALUES(ValorTotalVendido),
            Promocao = VALUES(Promocao)
        """
        db_manager.execute_query(insert_query, params=values)
        #logger.info("Previsões inseridas com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao inserir previsões: {e}")
