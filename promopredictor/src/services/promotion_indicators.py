import pandas as pd
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def calculate_promotion_indicators():
    """
    Calcula os indicadores de promoção usando a tabela de promoções identificadas,
    e insere esses indicadores na tabela sales_indicators.
    """
    try:
        # Carregar dados das tabelas
        promotions_query = "SELECT * FROM promotions_identified"
        vendas_query = "SELECT * FROM vendasexport"
        produtos_query = "SELECT * FROM vendasprodutosexport"
        
        promotions_result = db_manager.execute_query(promotions_query)
        vendas_result = db_manager.execute_query(vendas_query)
        produtos_result = db_manager.execute_query(produtos_query)
        
        if 'data' in promotions_result and 'columns' in promotions_result and \
           'data' in vendas_result and 'columns' in vendas_result and \
           'data' in produtos_result and 'columns' in produtos_result:
            df_promotions = pd.DataFrame(promotions_result['data'], columns=promotions_result['columns'])
            df_vendas = pd.DataFrame(vendas_result['data'], columns=vendas_result['columns'])
            df_produtos = pd.DataFrame(produtos_result['data'], columns=produtos_result['columns'])
            
            # Mesclar tabelas vendas e produtos para obter dados completos das vendas
            df_merged = pd.merge(df_vendas, df_produtos, left_on='Codigo', right_on='CodigoVenda')
            df_merged['Data'] = pd.to_datetime(df_merged['Data'])
            
            # Filtrar as vendas que estão dentro dos períodos das promoções identificadas
            indicadores_list = []

            for _, promo in df_promotions.iterrows():
                inicio_promocao = promo['DataInicioPromocao']
                fim_promocao = promo['DataFimPromocao']
                codigo_produto = promo['CodigoProduto']
                
                vendas_promocao = df_merged[(df_merged['CodigoProduto'] == codigo_produto) & 
                                            (df_merged['Data'] >= inicio_promocao) & 
                                            (df_merged['Data'] <= fim_promocao)]
                
                quantidade_total = vendas_promocao['Quantidade'].sum()
                valor_total_vendido = vendas_promocao['ValorTotal'].sum()
                numero_pedidos = vendas_promocao['ExportID'].nunique()
                ticket_medio = valor_total_vendido / numero_pedidos if numero_pedidos > 0 else 0
                
                indicadores_list.append({
                    'CodigoProduto': codigo_produto,
                    'DataInicioPromocao': inicio_promocao,
                    'DataFimPromocao': fim_promocao,
                    'QuantidadeTotal': quantidade_total,
                    'ValorTotalVendido': valor_total_vendido,
                    'TicketMedio': ticket_medio
                })
            
            df_indicators = pd.DataFrame(indicadores_list)
            
            # Inserir indicadores na tabela sales_indicators
            for _, row in df_indicators.iterrows():
                insert_query = f"""
                INSERT INTO sales_indicators (CodigoProduto, DataInicioPromocao, DataFimPromocao, QuantidadeTotal, ValorTotalVendido, TicketMedio) 
                VALUES ({row['CodigoProduto']}, '{row['DataInicioPromocao']}', '{row['DataFimPromocao']}', {row['QuantidadeTotal']}, {row['ValorTotalVendido']}, {row['TicketMedio']})
                """
                db_manager.execute_query(insert_query)
            
            logger.info("Indicadores de promoção inseridos na tabela sales_indicators com sucesso.")
            return df_indicators
        else:
            logger.error("Erro ao carregar dados das tabelas.")
            return None
    except Exception as e:
        logger.error(f"Erro ao calcular indicadores de promoção: {e}")
        return None