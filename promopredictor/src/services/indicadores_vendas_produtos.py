import pandas as pd
from src.services.database import db_manager
from src.utils.logging_config import get_logger
import numpy as np
import concurrent.futures

logger = get_logger(__name__)

def fetch_data_in_batches(query, batch_size=100000):
    """
    Busca dados em blocos do banco de dados a partir da query.
    """
    try:
        result = db_manager.execute_query(query)
        if 'data' in result and 'columns' in result:
            logger.info(f"Quantidade de registros retornados: {len(result['data'])}")
            df = pd.DataFrame(result['data'], columns=result['columns'])
            return [df]  # Retorna uma lista de DataFrames como batches
        else:
            logger.warning("Nenhum dado foi retornado pela query.")
            return None
    except Exception as e:
        logger.error(f"Erro ao buscar dados: {e}")
        return None

def insert_data_in_batches(df, table_name):
    """
    Insere os dados de vendas processados em lotes na tabela indicadores_vendas_produtos.
    """
    try:
        for _, row in df.iterrows():
            row = row.replace({np.nan: 'NULL'})
            insert_query = f"""
            INSERT INTO {table_name} (DATA, CodigoVenda, CodigoProduto, CodigoSecao, CodigoGrupo, 
                                       CodigoSubGrupo, CodigoSupermercado, Quantidade, ValorTotal, Promocao)
            VALUES ('{row['DATA']}', {row['CodigoVenda']}, {row['CodigoProduto']}, {row['CodigoSecao']}, 
                    {row['CodigoGrupo']}, {row['CodigoSubGrupo']}, {row['CodigoSupermercado']}, 
                    {row['Quantidade']}, {row['ValorTotal']}, {row['Promocao']})
            ON DUPLICATE KEY UPDATE 
                Quantidade = VALUES(Quantidade),
                ValorTotal = VALUES(ValorTotal),
                Promocao = VALUES(Promocao);
            """
            insert_query = insert_query.replace("'NULL'", "NULL")
            db_manager.execute_query(insert_query)
        logger.info(f"Lote de {len(df)} registros inserido com sucesso na tabela {table_name}.")
    except Exception as e:
        logger.error(f"Erro ao inserir lote de dados: {e}")

def process_data_and_insert():
    """
    Processa os dados em blocos e insere na tabela indicadores_vendas_produtos.
    """
    query = """
        SELECT v.DATA, v.Codigo AS CodigoVenda, vp.CodigoProduto, vp.CodigoSecao, 
               vp.CodigoGrupo, vp.CodigoSubGrupo, 1 AS CodigoSupermercado, 
               vp.Quantidade, vp.ValorTotal, vp.Promocao
        FROM vendas_auxiliar v
        INNER JOIN vendasprodutos_auxiliar vp ON v.Codigo = vp.CodigoVenda
        WHERE v.DATA IS NOT NULL;
    """

    logger.info("Iniciando processamento dos dados de vendas detalhadas...")

    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(insert_data_in_batches, df_batch, 'indicadores_vendas_produtos') for df_batch in fetch_data_in_batches(query)]
            for future in concurrent.futures.as_completed(futures):
                future.result()
    except Exception as e:
        logger.error(f"Erro durante o processamento e inserção de dados: {e}")
