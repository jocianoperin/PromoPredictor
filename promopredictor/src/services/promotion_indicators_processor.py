import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def insert_indicators(row):
    """
    Função auxiliar para inserir indicadores na tabela sales_indicators.
    """
    try:
        insert_query = f"""
        INSERT INTO sales_indicators (PromotionId, CodigoProduto, DataInicioPromocao, DataFimPromocao, QuantidadeTotal, ValorTotalVendido, ValorCusto, TotalVendaCompleta, TicketMedio) 
        VALUES ({row['PromotionId']}, {row['CodigoProduto']}, '{row['DataInicioPromocao']}', '{row['DataFimPromocao']}', {row['QuantidadeTotal']}, {row['ValorTotalVendido']}, {row['ValorCusto']}, {row['TotalVendaCompleta']}, {row['TicketMedio']})
        """
        db_manager.execute_query(insert_query)
        logger.info(f"Indicador inserido com sucesso para produto {row['CodigoProduto']} na promoção de {row['DataInicioPromocao']} a {row['DataFimPromocao']}")
    except Exception as e:
        logger.error(f"Erro ao inserir indicador: {e}")

def calculate_promotion_indicators():
    """
    Calcula os indicadores de promoção usando a tabela de promoções identificadas,
    e insere esses indicadores na tabela sales_indicators.
    """
    try:
        logger.info("Iniciando o cálculo dos indicadores de promoção...")
        
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
            
            # Merge the vendas and produtos dataframes using the correct keys
            df_sales = pd.merge(df_vendas, df_produtos, how='inner', left_on='Codigo', right_on='CodigoVenda')
            logger.info(f"Colunas disponíveis em df_sales após o merge: {df_sales.columns.tolist()}")

            # Processamento paralelo para cálculo de indicadores
            with ThreadPoolExecutor() as executor:
                futures = []
                for _, promo in df_promotions.iterrows():
                    futures.append(executor.submit(calculate_and_insert_indicators, promo, df_sales))
                
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        logger.info("Cálculo de indicadores finalizado com sucesso.")
                    else:
                        logger.warning("Ocorreu um problema durante o cálculo dos indicadores.")

    except Exception as e:
        logger.error(f"Erro ao calcular indicadores de promoção: {e}")

def calculate_and_insert_indicators(promo, df_sales):
    """
    Função auxiliar para calcular e inserir indicadores para uma promoção específica.
    """
    try:
        start_date = promo['DataInicioPromocao']
        end_date = promo['DataFimPromocao']
        product_code = promo['CodigoProduto']
        promotion_id = promo['id']
        
        # Filtrar vendas dentro do período da promoção para o produto específico
        sales_in_promo = df_sales[
            (df_sales['CodigoProduto'] == product_code) &
            (df_sales['data'] >= start_date) &
            (df_sales['data'] <= end_date)
        ]
        
        # Calcular indicadores
        quantity_total = float(sales_in_promo['Quantidade'].sum())
        value_total_sold = float(sales_in_promo['ValorTotal'].sum())
        
        # Verificação do ValorCusto
        valor_custo = float(promo['ValorCusto']) if promo['ValorCusto'] is not None else 0.00

        total_venda_completa = float(sales_in_promo.groupby('CodigoVenda')['TotalPedido'].first().sum())
        ticket_medio = value_total_sold / quantity_total if quantity_total > 0 else 0
        
        # Preparar linha para inserção
        indicator_row = {
            'PromotionId': promotion_id,
            'CodigoProduto': product_code,
            'DataInicioPromocao': promo['DataInicioPromocao'],
            'DataFimPromocao': promo['DataFimPromocao'],
            'QuantidadeTotal': quantity_total,
            'ValorTotalVendido': value_total_sold,
            'ValorCusto': valor_custo,
            'TotalVendaCompleta': total_venda_completa,
            'TicketMedio': ticket_medio
        }
        
        # Inserir indicadores no banco de dados
        insert_indicators(indicator_row)
        
        return True
    
    except KeyError as e:
        logger.error(f"Erro ao acessar a chave {e} durante o cálculo dos indicadores.")
        return False
    
    except Exception as e:
        logger.error(f"Erro ao calcular indicadores para produto {promo['CodigoProduto']}: {e}")
        return False
