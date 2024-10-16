import pandas as pd
from src.services.database import db_manager
from src.utils.logging_config import get_logger
import numpy as np
import concurrent.futures

logger = get_logger(__name__)

def fetch_data_in_batches(query, batch_size=100000):
    """
    Busca os dados agregados em blocos do banco de dados.
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
    Insere os dados agregados de resumo processados em lotes na tabela indicadores_vendas_produtos_resumo.
    """
    try:
        batches = [df[i:i + batch_size] for i in range(0, df.shape[0], batch_size)]
        
        for batch in batches:
            values = []
            for _, row in batch.iterrows():
                row = row.replace({np.nan: 'NULL'})
                values.append(f"('{row['DATA']}', {row['CodigoProduto']}, {row['CodigoSecao']}, "
                              f"{row['CodigoGrupo']}, {row['CodigoSubGrupo']}, {row['CodigoSupermercado']}, "
                              f"{row['TotalUNVendidas']}, {row['ValorTotalVendido']}, {row['Promocao']})")
            
            insert_query = f"""
            INSERT INTO {table_name} (DATA, CodigoProduto, CodigoSecao, CodigoGrupo, CodigoSubGrupo, 
                                       CodigoSupermercado, TotalUNVendidas, ValorTotalVendido, Promocao)
            VALUES {', '.join(values)}
            ON DUPLICATE KEY UPDATE 
                TotalUNVendidas = VALUES(TotalUNVendidas),
                ValorTotalVendido = VALUES(ValorTotalVendido),
                Promocao = VALUES(Promocao);
            """
            # Substituir 'NULL' string por NULL literal em SQL
            insert_query = insert_query.replace("'NULL'", "NULL")
            db_manager.execute_query(insert_query)
        
        logger.info(f"Lote de {len(df)} registros inserido com sucesso na tabela {table_name}.")
    except Exception as e:
        logger.error(f"Erro ao inserir lote de dados: {e}")

def process_data_and_insert():
    """
    Processa os dados de resumo em blocos e insere na tabela indicadores_vendas_produtos_resumo.
    """
    query = """
        SELECT vp.DATA, vp.CodigoProduto, vp.CodigoSecao, vp.CodigoGrupo, vp.CodigoSubGrupo, 
               1 AS CodigoSupermercado, 
               SUM(vp.Quantidade) AS TotalUNVendidas, 
               SUM(vp.ValorTotal) AS ValorTotalVendido, 
               CASE WHEN MAX(vp.Promocao) > 0 THEN 1 ELSE 0 END AS Promocao
        FROM indicadores_vendas_produtos vp
        WHERE vp.DATA IS NOT NULL
        GROUP BY vp.DATA, vp.CodigoProduto, vp.CodigoSecao, vp.CodigoGrupo, vp.CodigoSubGrupo;
    """

    logger.info("Iniciando processamento dos dados de resumo de vendas...")

    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(insert_data_in_batches, df_batch, 'indicadores_vendas_produtos_resumo') for df_batch in fetch_data_in_batches(query)]
            for future in concurrent.futures.as_completed(futures):
                future.result()
    except Exception as e:
        logger.error(f"Erro durante o processamento e inserção de dados: {e}")
