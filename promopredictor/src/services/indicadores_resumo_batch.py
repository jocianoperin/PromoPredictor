import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text
from src.services.database import db_manager
from src.utils.logging_config import get_logger
import numpy as np

# Carregar as variáveis do arquivo .env
load_dotenv()

logger = get_logger(__name__)

def fetch_data_in_batches(query, batch_size=100000):
    """
    Busca os dados em lotes usando SQLAlchemy.

    Args:
        query (str): A consulta SQL a ser executada.
        batch_size (int): Tamanho do lote.

    Yields:
        pd.DataFrame: DataFrame com o lote de dados.
    """
    offset = 0
    while True:
        paginated_query = f"{query} LIMIT {batch_size} OFFSET {offset};"
        try:
            result = db_manager.execute_query(paginated_query)
        except Exception as e:
            logger.error(f"Erro ao executar query no batch: {e}")
            raise

        if 'data' in result and 'columns' in result:
            if len(result['data']) == 0:
                break
            df = pd.DataFrame(result['data'], columns=result['columns'])
            logger.info(f"Batch com {len(df)} registros retornado.")
            yield df
        else:
            logger.warning("Nenhum dado foi retornado pela query.")
            break
        offset += batch_size

def insert_data_in_batches(df, table_name, batch_size=1000):
    """
    Insere os dados em lotes na tabela especificada usando SQLAlchemy.

    Args:
        df (pd.DataFrame): DataFrame com os dados a serem inseridos.
        table_name (str): Nome da tabela de destino.
        batch_size (int): Tamanho do lote para inserção.
    """
    try:
        logger.info(f"Iniciando inserção de dados.")

        batches = [df[i:i + batch_size] for i in range(0, df.shape[0], batch_size)]

        for batch in batches:
            # Preparar os dados para inserção
            batch = batch.replace({np.nan: None})
            values = batch.to_dict(orient='records')

            insert_query = f"""
            INSERT INTO {table_name} (DATA, CodigoProduto, CodigoSecao, CodigoGrupo,
                                       CodigoSubGrupo, TotalUNVendidas, ValorTotalVendido, Promocao)
            VALUES (:DATA, :CodigoProduto, :CodigoSecao, :CodigoGrupo,
                    :CodigoSubGrupo, :TotalUNVendidas, :ValorTotalVendido, :Promocao)
            ON DUPLICATE KEY UPDATE 
                TotalUNVendidas = VALUES(TotalUNVendidas),
                ValorTotalVendido = VALUES(ValorTotalVendido),
                Promocao = VALUES(Promocao);
            """

            connection = db_manager.get_connection()
            try:
                logger.debug(f"Inserindo batch de tamanho {len(batch)}.")
                connection.execute(text(insert_query), values)
                connection.commit()  # Comitar após cada batch
            except Exception as e:
                logger.error(f"Erro ao inserir lote de dados: {e}")
                connection.rollback()
                raise
            finally:
                connection.close()

        logger.info(f"Lote de {len(df)} registros inserido com sucesso na tabela {table_name}.")
    except Exception as e:
        logger.error(f"Erro ao inserir dados: {e}")
        raise

def process_data_and_insert():
    """
    Processa os dados de resumo em blocos e insere na tabela indicada.
    """
    query = """
        SELECT cal.DATA, p.CodigoProduto, IFNULL(p.CodigoSecao, 0) as CodigoSecao, IFNULL(p.CodigoGrupo, 0) as CodigoGrupo, IFNULL(p.CodigoSubGrupo, 0) as CodigoSubGrupo, 
               IFNULL(SUM(vp.Quantidade), 0) AS TotalUNVendidas, 
               IFNULL(SUM(vp.ValorTotal), 0) AS ValorTotalVendido, 
               IFNULL(MAX(vp.Promocao), 0) AS Promocao
        FROM calendario cal
        CROSS JOIN produtosmaisvendidos p
        LEFT JOIN indicadores_vendas_produtos vp
            ON cal.DATA = vp.DATA
            AND p.CodigoProduto = vp.CodigoProduto
        GROUP BY cal.DATA, p.CodigoProduto
        ORDER BY cal.DATA, p.CodigoProduto
    """

    logger.info("Iniciando processamento dos dados de resumo de vendas...")

    try:
        for df_batch in fetch_data_in_batches(query):
            insert_data_in_batches(df_batch, 'indicadores_vendas_produtos_resumo')
    except Exception as e:
        logger.error(f"Erro durante o processamento e inserção de dados: {e}")
        raise
