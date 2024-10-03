import pandas as pd
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Função para buscar registros em blocos
def fetch_data_in_batches(query, batch_size=100000):
    """
    Busca dados em blocos (chunks) do banco de dados usando o método execute_query diretamente.
    """
    try:
        # Use a função db_manager.execute_query para obter dados
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


# Inserir os dados processados no banco de dados
def insert_data_in_batches(df, table_name):
    """
    Insere os dados processados em lotes na tabela de destino usando pandas e db_manager.
    """
    try:
        for index, row in df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (DATA, CodigoProduto, CodigoSecao, CodigoGrupo, CodigoSubGrupo, 
                                       CodigoSupermercado, TotalUNVendidas, ValorTotalVendido, Promocao)
            VALUES ('{row['DATA']}', {row['CodigoProduto']}, {row['CodigoSecao']}, {row['CodigoGrupo']}, 
                    {row['CodigoSubGrupo']}, {row['CodigoSupermercado']}, {row['TotalUNVendidas']}, 
                    {row['ValorTotalVendido']}, {row['Promocao']})
            """
            # Executar a query para cada linha sem encerrar a conexão
            db_manager.execute_query(insert_query)
        logger.info(f"Lote de {len(df)} registros inserido com sucesso na tabela {table_name}.")
    except Exception as e:
        logger.error(f"Erro ao inserir lote de dados: {e}")

# Processar os dados e inserir no novo formato
def process_data_and_insert():
    """
    Processa os dados em blocos e insere na tabela indicadores_vendas_produtos_resumo.
    """
    # SQL para buscar e agrupar os dados
    query = """
    SELECT c.DATA, p.CodigoProduto, COALESCE(va2.CodigoSecao, p.CodigoSecao) AS CodigoSecao, 
           COALESCE(va2.CodigoGrupo, p.CodigoGrupo) AS CodigoGrupo,
           COALESCE(va2.CodigoSubGrupo, p.CodigoSubGrupo) AS CodigoSubGrupo, 
           COALESCE(va2.CodigoSupermercado, p.CodigoSupermercado) AS CodigoSupermercado,
           ROUND(IFNULL(SUM(va2.Quantidade), 0), 2) AS TotalUNVendidas, 
           ROUND(IFNULL(SUM(va2.ValorTotal), 0), 2) AS ValorTotalVendido, 
           IFNULL(va2.Promocao, 0) AS Promocao
    FROM calendario c 
    CROSS JOIN (SELECT DISTINCT CodigoProduto, CodigoSecao, CodigoGrupo, CodigoSubGrupo, CodigoSupermercado 
                FROM indicadores_vendas_produtos) p 
    LEFT JOIN indicadores_vendas_produtos va2 
    ON c.DATA = va2.DATA AND p.CodigoProduto = va2.CodigoProduto
    GROUP BY c.DATA, p.CodigoProduto
    ORDER BY p.CodigoProduto, c.DATA;
    """
    
    logger.info("Iniciando processamento em blocos para a tabela indicadores_vendas_produtos_resumo...")
    
    try:
        # Iterar sobre os dados em blocos
        for df_batch in fetch_data_in_batches(query):
            # Inserir o lote de dados processados no banco de destino
            insert_data_in_batches(df_batch, 'indicadores_vendas_produtos_resumo')
    except Exception as e:
        logger.error(f"Erro durante o processo de inserção de dados: {e}")
    else:
        logger.info("Processamento e inserção finalizados com sucesso.")