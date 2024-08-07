import pandas as pd
from src.services.database import db_manager
from src.utils.logging_config import get_logger
from datetime import datetime, timedelta

logger = get_logger(__name__)

def identify_promotions(batch_size=10000):
    """
    Identifica promoções na tabela de vendas e produtos, baseando-se em critérios como descontos e marcações de preços promocionais.
    """
    try:
        logger.info("Iniciando a identificação de promoções...")
        # Carregar dados das tabelas em lotes
        offset = 0
        promotions_list = []

        while True:
            logger.info(f"Carregando lote com offset {offset}...")
            vendas_query = f"SELECT * FROM vendasexport LIMIT {batch_size} OFFSET {offset}"
            produtos_query = f"SELECT * FROM vendasprodutosexport LIMIT {batch_size} OFFSET {offset}"
            
            vendas_result = db_manager.execute_query(vendas_query)
            produtos_result = db_manager.execute_query(produtos_query)
            
            if 'data' in vendas_result and 'columns' in vendas_result and 'data' in produtos_result and 'columns' in produtos_result:
                if not vendas_result['data'] or not produtos_result['data']:
                    logger.info("Nenhum dado retornado, terminando o processamento dos lotes.")
                    break
                
                df_vendas = pd.DataFrame(vendas_result['data'], columns=vendas_result['columns'])
                df_produtos = pd.DataFrame(produtos_result['data'], columns=produtos_result['columns'])
                
                # Verificar colunas antes da mesclagem
                logger.info(f"Colunas em df_vendas: {df_vendas.columns.tolist()}")
                logger.info(f"Colunas em df_produtos: {df_produtos.columns.tolist()}")
                
                # Mesclar tabelas usando a coluna correta para o join
                df_merged = pd.merge(df_vendas, df_produtos, left_on='Codigo', right_on='CodigoVenda')
                
                # Verificar colunas após a mesclagem
                logger.info(f"Colunas em df_merged: {df_merged.columns.tolist()}")
                
                # Verificar se a coluna 'data' está presente
                if 'data' not in df_merged.columns:
                    logger.error("A coluna 'data' não está presente nos dados mesclados.")
                    return None
                
                logger.info("Coluna 'data' encontrada, processando promoções...")
                
                # Identificar promoções
                df_merged['data'] = pd.to_datetime(df_merged['data'])
                df_promotions = df_merged[(df_merged['Desconto'] > 0) | (df_merged['PrecoemPromocao'] == 1)]
                
                # Logar o número de registros com descontos e preços promocionais
                num_descontos = df_merged[df_merged['Desconto'] > 0].shape[0]
                num_preco_promocao = df_merged[df_merged['PrecoemPromocao'] == 1].shape[0]
                logger.info(f"Registros com desconto: {num_descontos}, registros com preço em promoção: {num_preco_promocao}")
                
                # Adicionar lógica temporal (opcional)
                df_promotions = df_promotions.sort_values(by=['CodigoProduto', 'data'])
                df_promotions['PromocaoContinua'] = df_promotions.groupby('CodigoProduto')['data'].diff().dt.days <= 30
                
                # Filtrar promoções contínuas
                df_promotions_continua = df_promotions[df_promotions['PromocaoContinua'].fillna(False)]
                
                logger.info(f"Lote com offset {offset} processado, {df_promotions_continua.shape[0]} promoções contínuas identificadas.")
                promotions_list.append(df_promotions_continua)
                
                offset += batch_size
            else:
                logger.error("Erro ao carregar dados das tabelas.")
                break
        
        if promotions_list:
            df_all_promotions = pd.concat(promotions_list, ignore_index=True)
            logger.info(f"Promoções identificadas: {df_all_promotions.shape[0]} registros encontrados no total.")
            
            # Inserir promoções na tabela promotions_identified
            for _, row in df_all_promotions.iterrows():
                # Verificar duplicidade antes de inserir
                check_query = f"""
                SELECT COUNT(*) FROM promotions_identified
                WHERE CodigoProduto = {row['CodigoProduto']} AND DataInicioPromocao = '{row['data'].date()}' AND DataFimPromocao = '{(row['data'] + timedelta(days=30)).date()}'
                """
                result = db_manager.execute_query(check_query)
                if result['data'][0][0] == 0:
                    insert_query = f"""
                    INSERT INTO promotions_identified (CodigoProduto, DataInicioPromocao, DataFimPromocao, ValorUnitario, ValorTabela) 
                    VALUES ({row['CodigoProduto']}, '{row['data'].date()}', '{(row['data'] + timedelta(days=30)).date()}', {row['valorunitario']}, {row['ValorTabela']})
                    """
                    db_manager.execute_query(insert_query)
                else:
                    logger.info(f"Promoção duplicada encontrada: CodigoProduto={row['CodigoProduto']}, DataInicioPromocao={row['data'].date()}, DataFimPromocao={(row['data'] + timedelta(days=30)).date()}")
            
            logger.info("Promoções inseridas na tabela promotions_identified com sucesso.")
            return df_all_promotions
        else:
            logger.error("Nenhuma promoção identificada.")
            return None
    except Exception as e:
        logger.error(f"Erro ao identificar promoções: {e}")
        return None