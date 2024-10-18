from src.services.database import db_manager
from src.utils.logging_config import get_logger
import pandas as pd

# Inicializar o logger
logger = get_logger(__name__)

def clear_predictions_table():
    """
    Limpa a tabela de previsões antes de inserir novas previsões.
    """
    try:
        delete_query = "TRUNCATE indicadores_vendas_produtos_previsoes;"
        db_manager.execute_query(delete_query)
        logger.info("Tabela de previsões limpa com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao limpar a tabela de previsões: {e}")

def insert_predictions(df_pred):
    """
    Insere as previsões na tabela indicadores_vendas_produtos_previsoes com logs detalhados para depuração.
    """
    try:
        # Garantir que 'CodigoProduto' contenha o valor original
        df_pred = df_pred.copy()
        
        # Verificar e logar o conteúdo e tipos do DataFrame antes da inserção
        logger.debug(f"DataFrame a ser inserido (primeiras 5 linhas):\n{df_pred.head()}")
        logger.debug(f"Tipos de dados no DataFrame:\n{df_pred.dtypes}")

        # Verificar se a coluna 'DATA' está no formato correto
        if not pd.api.types.is_datetime64_any_dtype(df_pred['DATA']):
            logger.debug("Coluna 'DATA' não está no formato datetime, convertendo para o formato correto.")
            df_pred['DATA'] = pd.to_datetime(df_pred['DATA'], errors='coerce')
            logger.debug(f"Após conversão, tipos de dados:\n{df_pred.dtypes}")

        # Verificar se há valores nulos na coluna 'DATA'
        if df_pred['DATA'].isnull().any():
            logger.error("Existem valores nulos ou inválidos na coluna 'DATA' após a conversão.")
            logger.debug(df_pred[df_pred['DATA'].isnull()])
            return

        # Converter DataFrame para lista de dicionários para inserção
        values = df_pred.to_dict(orient='records')
        logger.debug(f"Registros prontos para inserção (primeiros 5 registros): {values[:5]}")

        # Verificar se há registros para inserir
        if not values:
            logger.warning("Não há registros disponíveis para inserção.")
            return
        
        for idx, record in enumerate(values):
            try:
                logger.debug(f"Tentando inserir registro {idx + 1}/{len(values)}: {record}")
                insert_query = """
                INSERT INTO indicadores_vendas_produtos_previsoes (DATA, CodigoProduto, TotalUNVendidas, ValorTotalVendido, Promocao)
                VALUES (:DATA, :CodigoProduto, :TotalUNVendidas, :ValorTotalVendido, :Promocao)
                ON DUPLICATE KEY UPDATE
                    TotalUNVendidas = VALUES(TotalUNVendidas),
                    ValorTotalVendido = VALUES(ValorTotalVendido),
                    Promocao = VALUES(Promocao)
                """
                # Log da query que está sendo executada e os parâmetros
                logger.debug(f"Query: {insert_query}")
                logger.debug(f"Parâmetros: {record}")
                
                # Executa a query com o registro específico e captura a resposta
                response = db_manager.execute_query(insert_query, params=record)
                logger.debug(f"Resposta do banco de dados para o registro {idx + 1}: {response}")
                
                logger.debug(f"Registro {idx + 1} inserido com sucesso: {record}")
            except Exception as e:
                logger.error(f"Erro ao inserir registro {idx + 1} ({record}): {e}")
                continue  # Continuar tentando com outros registros em caso de erro

        logger.info("Previsões inseridas com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao inserir previsões: {e}")
