import pandas as pd
from src.services.database import db_manager
from src.utils.logging_config import get_logger
import numpy as np

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
    Se o registro já existir, apenas registra no log e continua.
    """
    try:
        for index, row in df.iterrows():
            # Substituir valores NaN por NULL
            row = row.replace({np.nan: 'NULL'})
            
            # Query para verificar se o registro já existe
            check_query = f"""
            SELECT COUNT(1) FROM {table_name}
            WHERE DATA = '{row['DATA']}' 
              AND CodigoVenda = {row['CodigoVenda']}
              AND CodigoProduto = {row['CodigoProduto']}
            """
            
            result = db_manager.execute_query(check_query)
            
            # Se o registro já existir, registrar no log e continuar
            if result['data'][0][0] > 0:
                logger.info(f"Registro já existente para DATA: {row['DATA']}, CodigoVenda: {row['CodigoVenda']}, CodigoProduto: {row['CodigoProduto']}. Não foi inserido.")
                continue
            
            # Query de inserção
            insert_query = f"""
            INSERT INTO {table_name} (DATA, CodigoVenda, CodigoProduto, CodigoSecao, CodigoGrupo, 
                                       CodigoSubGrupo, CodigoSupermercado, Quantidade, ValorTotal, Promocao)
            VALUES ('{row['DATA']}', {row['CodigoVenda']}, {row['CodigoProduto']}, {row['CodigoSecao']}, 
                    {row['CodigoGrupo']}, {row['CodigoSubGrupo']}, {row['CodigoSupermercado']}, 
                    {row['Quantidade']}, {row['ValorTotal']}, {row['Promocao']})
            """
            # Ajustar o formato de valores NULL corretamente no SQL
            insert_query = insert_query.replace("'NULL'", "NULL")
            
            # Executar a query para cada linha sem encerrar a conexão
            db_manager.execute_query(insert_query)
        logger.info(f"Lote de {len(df)} registros inserido com sucesso na tabela {table_name}.")
    except Exception as e:
        logger.error(f"Erro ao inserir lote de dados: {e}")

# Processar os dados e inserir no novo formato
def process_data_and_insert():
    """
    Processa os dados em blocos e insere na tabela indicadores_vendas_produtos.
    """
    # SQL para buscar os dados
    query = """
        SELECT v.DATA, v.Codigo AS CodigoVenda, vp.CodigoProduto, vp.CodigoSecao, 
               vp.CodigoGrupo, vp.CodigoSubGrupo, 1 AS CodigoSupermercado, 
               vp.Quantidade, vp.ValorTotal, vp.Promocao
        FROM vendas_auxiliar v
        INNER JOIN vendasprodutos_auxiliar vp ON v.Codigo = vp.CodigoVenda
        WHERE v.`DATA` IS NOT NULL;
    """
    
    logger.info("Iniciando processamento em blocos para a tabela indicadores_vendas_produtos...")
    
    try:
        # Iterar sobre os dados em blocos
        for df_batch in fetch_data_in_batches(query):
            # Inserir o lote de dados processados no banco de destino
            insert_data_in_batches(df_batch, 'indicadores_vendas_produtos')
    except Exception as e:
        logger.error(f"Erro durante o processo de inserção de dados: {e}")
    else:
        logger.info("Processamento e inserção finalizados com sucesso.")
