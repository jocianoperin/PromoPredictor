import pandas as pd
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def identify_promotions():
    """
    Identifica promoções na tabela de vendas e produtos, baseando-se em critérios como descontos e marcações de preços promocionais.
    """
    try:
        # Carregar dados das tabelas
        vendas_query = "SELECT * FROM vendasexport"
        produtos_query = "SELECT * FROM vendasprodutosexport"
        
        vendas_result = db_manager.execute_query(vendas_query)
        produtos_result = db_manager.execute_query(produtos_query)
        
        if 'data' in vendas_result and 'columns' in vendas_result and 'data' in produtos_result and 'columns' in produtos_result:
            df_vendas = pd.DataFrame(vendas_result['data'], columns=vendas_result['columns'])
            df_produtos = pd.DataFrame(produtos_result['data'], columns=produtos_result['columns'])
            
            # Mesclar tabelas usando a coluna correta para o join
            df_merged = pd.merge(df_vendas, df_produtos, left_on='Codigo', right_on='CodigoVenda')
            
            # Identificar promoções
            df_merged['Data'] = pd.to_datetime(df_merged['Data'])
            df_promotions = df_merged[(df_merged['Desconto'] > 0) | (df_merged['PrecoemPromocao'] == 1)]
            
            # Adicionar lógica temporal (opcional)
            # Exemplo: Identificar períodos de promoção contínuos
            df_promotions = df_promotions.sort_values(by=['CodigoProduto', 'Data'])
            df_promotions['PromocaoContinua'] = df_promotions.groupby('CodigoProduto')['Data'].diff().dt.days <= 30  # Exemplo: Promoções dentro de 30 dias
            
            # Filtrar promoções contínuas
            df_promotions_continua = df_promotions[df_promotions['PromocaoContinua'].fillna(False)]
            
            # Log de promoções identificadas
            logger.info(f"Promoções identificadas: {df_promotions_continua.shape[0]} registros encontrados.")
            
            # Inserir promoções na tabela promotion_identified
            for _, row in df_promotions_continua.iterrows():
                insert_query = f"""
                INSERT INTO promotion_identified (CodigoProduto, DataInicioPromocao, DataFimPromocao, ValorUnitario, ValorTabela) 
                VALUES ({row['CodigoProduto']}, '{row['Data'].date()}', '{(row['Data'] + pd.DateOffset(days=30)).date()}', {row['ValorUnitario']}, {row['ValorTabela']})
                """
                db_manager.execute_query(insert_query)
            
            logger.info("Promoções inseridas na tabela promotion_identified com sucesso.")
            return df_promotions_continua
        else:
            logger.error("Erro ao carregar dados das tabelas.")
            return None
    except Exception as e:
        logger.error(f"Erro ao identificar promoções: {e}")
        return None
