import pandas as pd
from src.services.database import db_manager
from src.utils.logging_config import get_logger
import numpy as np
import concurrent.futures
import threading

logger = get_logger(__name__)

def fetch_data_in_batches(query, batch_size=100000):
    offset = 0
    try:
        while True:
            # Cria uma nova conexão para cada lote de dados
            with db_manager.get_connection() as connection:
                paginated_query = f"{query} LIMIT {batch_size} OFFSET {offset};"
                result = db_manager.execute_query(paginated_query)

                if 'data' in result and 'columns' in result:
                    if len(result['data']) == 0:
                        break
                    df = pd.DataFrame(result['data'], columns=result['columns'])
                    logger.info(f"Batch com {len(df)} registros retornado.")
                    yield df  # Retorna o DataFrame como batch
                else:
                    logger.warning("Nenhum dado foi retornado pela query.")
                    break
            offset += batch_size
    except Exception as e:
        logger.error(f"Erro ao buscar dados: {e}")

def insert_data_in_batches(df, table_name, batch_size=1000):
    """
    Insere os dados agregados de resumo processados em lotes na tabela indicadores_vendas_produtos_resumo.
    Utiliza threading para maior controle em ambientes de execução paralela.
    """
    try:
        thread_name = threading.current_thread().name
        logger.info(f"Thread {thread_name}: Iniciando inserção de dados.")

        batches = [df[i:i + batch_size] for i in range(0, df.shape[0], batch_size)]
        
        for batch in batches:
            values = []
            for _, row in batch.iterrows():
                row = row.replace({np.nan: 'NULL'})
                values.append(f"('{row['DATA']}', {row['CodigoProduto']}, {row['CodigoSecao']}, "
                              f"{row['CodigoGrupo']}, {row['CodigoSubGrupo']}, {row['TotalUNVendidas']}, "
                              f"{row['ValorTotalVendido']}, {row['Promocao']})")
            
            insert_query = f"""
            INSERT INTO {table_name} (DATA, CodigoProduto, CodigoSecao, CodigoGrupo, CodigoSubGrupo, 
                                       TotalUNVendidas, ValorTotalVendido, Promocao)
            VALUES {', '.join(values)}
            ON DUPLICATE KEY UPDATE 
                TotalUNVendidas = VALUES(TotalUNVendidas),
                ValorTotalVendido = VALUES(ValorTotalVendido),
                Promocao = VALUES(Promocao);
            """
            insert_query = insert_query.replace("'NULL'", "NULL")  # Substituir 'NULL' string por NULL literal em SQL
            
            # Usa uma nova conexão para cada batch
            with db_manager.get_connection() as connection:
                db_manager.execute_query(insert_query)
        
        logger.info(f"Thread {thread_name}: Lote de {len(df)} registros inserido com sucesso na tabela {table_name}.")
    except Exception as e:
        logger.error(f"Thread {thread_name}: Erro ao inserir lote de dados: {e}")


def process_data_and_insert():
    """
    Processa os dados de resumo em blocos e insere na tabela indicadores_vendas_produtos_resumo.
    Cada operação é isolada com uma nova conexão para evitar conflitos.
    """
    query = """
        SELECT cal.DATA, p.CodigoProduto, p.CodigoSecao, p.CodigoGrupo, p.CodigoSubGrupo, 
               IFNULL(SUM(vp.Quantidade), 0) AS TotalUNVendidas, 
               IFNULL(SUM(vp.ValorTotal), 0) AS ValorTotalVendido, 
               IFNULL(MAX(vp.Promocao), 0) AS Promocao
        FROM calendario cal
        CROSS JOIN produtosmaisvendidos p
        LEFT JOIN indicadores_vendas_produtos vp
            ON cal.DATA = vp.DATA
            AND p.CodigoProduto = vp.CodigoProduto
        GROUP BY cal.DATA, p.CodigoProduto;
    """

    logger.info("Iniciando processamento dos dados de resumo de vendas...")

    try:
        # Executor com multithreading
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(insert_data_in_batches, df_batch, 'indicadores_vendas_produtos_resumo')
                for df_batch in fetch_data_in_batches(query)
            ]
            for future in concurrent.futures.as_completed(futures):
                future.result()
    except Exception as e:
        logger.error(f"Erro durante o processamento e inserção de dados: {e}")
