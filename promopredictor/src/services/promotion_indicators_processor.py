import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def insert_indicators(row):
    """
    Função auxiliar para inserir indicadores na tabela sales_indicators.
    """
    try:
        insert_query = f"""
        INSERT INTO sales_indicators (PromotionId, CodigoProduto, DataInicioPromocao, DataFimPromocao, QuantidadeTotal, ValorTotalVendido, ValorCusto, ValorTabela, ValorUnitarioVendido, TotalVendaCompleta, TicketMedio, MargemLucro, PercentualDescontoMedio, ElasticidadePrecoDemanda) 
        VALUES ({row['PromotionId']}, {row['CodigoProduto']}, '{row['DataInicioPromocao']}', '{row['DataFimPromocao']}', {row['QuantidadeTotal']}, {row['ValorTotalVendido']}, {row['ValorCusto']}, {row['ValorTabela']}, {row['ValorUnitarioVendido']}, {row['TotalVendaCompleta']}, {row['TicketMedio']}, {row['MargemLucro']}, {row['PercentualDescontoMedio']}, {row['ElasticidadePrecoDemanda']})
        """
        db_manager.execute_query(insert_query)
        thread_id = threading.get_ident()
        logger.info(f"[Thread-{thread_id}] Indicador inserido com sucesso para produto {row['CodigoProduto']} na promoção de {row['DataInicioPromocao']} a {row['DataFimPromocao']}")
    except Exception as e:
        thread_id = threading.get_ident()
        logger.error(f"[Thread-{thread_id}] Erro ao inserir indicador: {e}")

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
        historical_sales_query = """
            SELECT v.Codigo AS CodigoVenda, p.CodigoProduto, v.data, v.TotalPedido, 
                   p.Quantidade, p.valorunitario, p.ValorTotal
            FROM vendasexport v
            INNER JOIN vendasprodutosexport p ON v.Codigo = p.CodigoVenda
            WHERE v.data < CURDATE()
        """

        promotions_result = db_manager.execute_query(promotions_query)
        vendas_result = db_manager.execute_query(vendas_query)
        produtos_result = db_manager.execute_query(produtos_query)
        historical_sales_result = db_manager.execute_query(historical_sales_query)
        
        if 'data' in promotions_result and 'columns' in promotions_result and \
           'data' in vendas_result and 'columns' in vendas_result and \
           'data' in produtos_result and 'columns' in produtos_result and \
           'data' in historical_sales_result and 'columns' in historical_sales_result:
            df_promotions = pd.DataFrame(promotions_result['data'], columns=promotions_result['columns'])
            df_vendas = pd.DataFrame(vendas_result['data'], columns=vendas_result['columns'])
            df_produtos = pd.DataFrame(produtos_result['data'], columns=produtos_result['columns'])
            df_historical_sales = pd.DataFrame(historical_sales_result['data'], columns=historical_sales_result['columns'])
            
            # Verificar colunas disponíveis em df_sales e df_historical_sales
            df_sales = pd.merge(df_vendas, df_produtos, how='inner', left_on='Codigo', right_on='CodigoVenda')
            logger.info(f"Colunas disponíveis em df_sales após o merge: {df_sales.columns.tolist()}")
            logger.info(f"Colunas disponíveis em df_historical_sales: {df_historical_sales.columns.tolist()}")

            # Acessar a coluna 'data' corretamente
            if 'data' in df_sales.columns:
                data_column = 'data'
            else:
                logger.error("A coluna 'data' não está presente no DataFrame 'df_sales'.")
                return False

            # Verificar se a coluna 'valorunitario' está presente em df_sales
            if 'valorunitario' not in df_sales.columns:
                logger.error("A coluna 'valorunitario' não está presente no DataFrame 'df_sales'.")
                return False

            # Processamento paralelo para cálculo de indicadores
            with ThreadPoolExecutor() as executor:
                futures = []
                for _, promo in df_promotions.iterrows():
                    futures.append(executor.submit(calculate_and_insert_indicators, promo, df_sales, df_historical_sales, data_column))
                
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        logger.info("Cálculo de indicadores finalizado com sucesso.")
                    else:
                        logger.warning("Ocorreu um problema durante o cálculo dos indicadores.")

    except Exception as e:
        logger.error(f"Erro ao calcular indicadores de promoção: {e}")

def calculate_and_insert_indicators(promo, df_sales, df_historical_sales, data_column):
    """
    Função auxiliar para calcular e inserir indicadores para uma promoção específica.
    """
    try:
        thread_id = threading.get_ident()
        
        # Verificar se a coluna CodigoProduto existe no DataFrame promo
        if 'CodigoProduto' not in promo:
            logger.error(f"[Thread-{thread_id}] A coluna 'CodigoProduto' não está presente no DataFrame 'promo'.")
            return False
        
        start_date = promo['DataInicioPromocao']
        end_date = promo['DataFimPromocao']
        product_code = promo['CodigoProduto']
        promotion_id = promo['id']

        # Verificar se a coluna CodigoProduto existe no DataFrame df_sales
        if 'CodigoProduto' not in df_sales.columns:
            logger.error(f"[Thread-{thread_id}] A coluna 'CodigoProduto' não está presente no DataFrame 'df_sales'.")
            return False
        
        # Filtrar vendas dentro do período da promoção para o produto específico
        sales_in_promo = df_sales[
            (df_sales['CodigoProduto'] == product_code) &
            (df_sales[data_column] >= start_date) &
            (df_sales[data_column] <= end_date)
        ]

        logger.info(f"[Thread-{thread_id}] Vendas durante a promoção: {sales_in_promo.shape[0]} linhas")

        # Verificar se a coluna CodigoProduto existe no DataFrame df_historical_sales
        if 'CodigoProduto' not in df_historical_sales.columns:
            logger.error(f"[Thread-{thread_id}] A coluna 'CodigoProduto' não está presente no DataFrame 'df_historical_sales'.")
            return False
        
        # Filtrar vendas antes da promoção
        sales_before_promo = df_historical_sales[
            (df_historical_sales['CodigoProduto'] == product_code) &
            (df_historical_sales['data'] < start_date)
        ]

        logger.info(f"[Thread-{thread_id}] Vendas antes da promoção: {sales_before_promo.shape[0]} linhas")

        # Calcular indicadores
        quantity_total = float(sales_in_promo['Quantidade'].sum())
        value_total_sold = float(sales_in_promo['ValorTotal'].sum())
        valor_unitario_vendido = float(sales_in_promo['valorunitario'].mean())  # Valor unitário médio vendido
        valor_custo = float(promo['ValorCusto']) if promo['ValorCusto'] is not None else 0.00
        valor_tabela = float(promo['ValorTabela']) if promo['ValorTabela'] is not None else 0.00
        total_venda_completa = float(sales_in_promo.groupby('CodigoVenda')['TotalPedido'].first().sum())
        ticket_medio = value_total_sold / quantity_total if quantity_total > 0 else 0
        
        # Calcular Margem de Lucro
        lucro_bruto = value_total_sold - (valor_custo * quantity_total)
        margem_lucro = (lucro_bruto / value_total_sold) * 100 if value_total_sold > 0 else 0
        
        # Calcular Percentual de Desconto Médio (Convertendo 'promo['ValorUnitario']' para float)
        valor_unitario_promocao = float(promo['ValorUnitario'])  # Converte para float
        percentual_desconto_medio = ((valor_tabela - valor_unitario_promocao) / valor_tabela) * 100

        # Calcular Elasticidade Preço-Demanda
        if not sales_before_promo.empty:
            quantity_before_promo = float(sales_before_promo['Quantidade'].sum())
            price_before_promo = float(sales_before_promo['valorunitario'].mean())
            change_in_quantity = (quantity_total - quantity_before_promo) / quantity_before_promo
            change_in_price = (price_before_promo - valor_unitario_vendido) / price_before_promo
            elasticidade_preco_demanda = change_in_quantity / change_in_price if change_in_price != 0 else None
        else:
            elasticidade_preco_demanda = None
        
        # Preparar linha para inserção
        indicator_row = {
            'PromotionId': promotion_id,
            'CodigoProduto': product_code,
            'DataInicioPromocao': promo['DataInicioPromocao'],
            'DataFimPromocao': promo['DataFimPromocao'],
            'QuantidadeTotal': quantity_total,
            'ValorTotalVendido': value_total_sold,
            'ValorCusto': valor_custo,
            'ValorTabela': valor_tabela,
            'ValorUnitarioVendido': valor_unitario_vendido,
            'TotalVendaCompleta': total_venda_completa,
            'TicketMedio': ticket_medio,
            'MargemLucro': margem_lucro,
            'PercentualDescontoMedio': percentual_desconto_medio,
            'ElasticidadePrecoDemanda': elasticidade_preco_demanda
        }
        
        # Inserir indicadores no banco de dados
        insert_indicators(indicator_row)
        logger.info(f"[Thread-{thread_id}] Indicador inserido com sucesso para produto {product_code} na promoção de {promo['DataInicioPromocao']} a {promo['DataFimPromocao']}")

        return True
    
    except KeyError as e:
        logger.error(f"[Thread-{thread_id}] Erro ao acessar a chave {e} durante o cálculo dos indicadores.")
        return False
    
    except Exception as e:
        # Capturar e logar o erro específico
        logger.error(f"[Thread-{thread_id}] Erro inesperado ao calcular indicadores para produto {promo['CodigoProduto']}: {e}", exc_info=True)
        return False