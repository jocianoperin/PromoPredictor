import pandas as pd
from src.services.database import db_manager
from src.utils.logging_config import get_logger
import numpy as np
import concurrent.futures

logger = get_logger(__name__)

def fetch_data_in_batches(query, batch_size=100000):
    """
    Busca dados em blocos do banco de dados a partir da query, usando limite e deslocamento.
    """
    offset = 0
    try:
        while True:
            paginated_query = f"{query} LIMIT {batch_size} OFFSET {offset};"
            result = db_manager.execute_query(paginated_query)
            if 'data' in result and 'columns' in result:
                if len(result['data']) == 0:
                    break
                df = pd.DataFrame(result['data'], columns=result['columns'])
                logger.info(f"Batch com {len(df)} registros retornado.")
                yield df  # Retorna um DataFrame como batch
            else:
                logger.warning("Nenhum dado foi retornado pela query.")
                break
            offset += batch_size
    except Exception as e:
        logger.error(f"Erro ao buscar dados: {e}")

def insert_data_in_batches(df, table_name, batch_size=1000):
    """
    Insere os dados de vendas processados em lotes na tabela indicadores_vendas_produtos.
    """
    try:
        # Preparando lote de inserções
        batches = [df[i:i + batch_size] for i in range(0, df.shape[0], batch_size)]
        
        for batch in batches:
            values = []
            for _, row in batch.iterrows():
                row = row.replace({np.nan: 'NULL'})
                values.append(f"('{row['DATA']}', {row['CodigoVenda']}, {row['CodigoProduto']}, "
                              f"{row['CodigoSecao']}, {row['CodigoGrupo']}, {row['CodigoSubGrupo']}, "
                              f"{row['CodigoSupermercado']}, {row['Quantidade']}, {row['ValorTotal']}, {row['Promocao']})")
            
            insert_query = f"""
            INSERT INTO {table_name} (DATA, CodigoVenda, CodigoProduto, CodigoSecao, CodigoGrupo, 
                                       CodigoSubGrupo, CodigoSupermercado, Quantidade, ValorTotal, Promocao)
            VALUES {', '.join(values)}
            ON DUPLICATE KEY UPDATE 
                Quantidade = VALUES(Quantidade),
                ValorTotal = VALUES(ValorTotal),
                Promocao = VALUES(Promocao);
            """
            
            # Substitui 'NULL' string por NULL literal em SQL
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
        WHERE v.DATA >= '2019-01-01';
    """

    logger.info("Iniciando processamento dos dados de vendas detalhadas...")

    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for df_batch in fetch_data_in_batches(query):
                futures.append(executor.submit(insert_data_in_batches, df_batch, 'indicadores_vendas_produtos'))
            
            for future in concurrent.futures.as_completed(futures):
                future.result()
    except Exception as e:
        logger.error(f"Erro durante o processamento e inserção de dados: {e}")

