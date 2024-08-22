import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import traceback
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def insert_indicators(row):
    """
    Função auxiliar para inserir indicadores na tabela sales_indicators.
    """
    try:
        insert_query = f"""
        INSERT INTO sales_indicators (PromotionId, CodigoProduto, DataInicioPromocao, DataFimPromocao, QuantidadeTotal, 
                                      ValorTotalVendido, ValorCusto, ValorTabela, ValorUnitarioVendido, TotalVendaCompleta, 
                                      TicketMedio, MargemLucro, PercentualDescontoMedio, ElasticidadePrecoDemanda, 
                                      EstoqueMedioAntesPromocao, EstoqueNoDiaPromocao, ImpactoEmOutrasCategorias, 
                                      VolumeVendasPosPromocao, ComparacaoComPromocoesPassadas) 
        VALUES ({row['PromotionId']}, {row['CodigoProduto']}, '{row['DataInicioPromocao']}', '{row['DataFimPromocao']}', 
                {row['QuantidadeTotal']}, {row['ValorTotalVendido']}, {row['ValorCusto']}, {row['ValorTabela']}, 
                {row['ValorUnitarioVendido']}, {row['TotalVendaCompleta']}, {row['TicketMedio']}, {row['MargemLucro']}, 
                {row['PercentualDescontoMedio']}, {row['ElasticidadePrecoDemanda']}, {row['EstoqueMedioAntesPromocao']}, 
                {row['EstoqueNoDiaPromocao']}, {row['ImpactoEmOutrasCategorias']}, {row['VolumeVendasPosPromocao']}, 
                {row['ComparacaoComPromocoesPassadas']})
        """
        db_manager.execute_query(insert_query)
        thread_id = threading.get_ident()
        logger.info(f"[Thread-{thread_id}] Indicador inserido com sucesso para produto {row['CodigoProduto']} na promoção de {row['DataInicioPromocao']} a {row['DataFimPromocao']}")
    except Exception as e:
        thread_id = threading.get_ident()
        logger.error(f"[Thread-{thread_id}] Erro ao inserir indicador: {e}")
        logger.error(traceback.format_exc())

def calcular_estoque_para_promocao(codigo_produto, data_inicio_promocao):
    """
    Calcula o estoque médio antes da promoção e o estoque no dia da promoção para um produto específico.
    
    Args:
        codigo_produto (int): O código do produto.
        data_inicio_promocao (str): A data de início da promoção (formato 'YYYY-MM-DD').
    
    Retorna:
        dict: Um dicionário contendo 'estoque_medio_antes_promocao' e 'estoque_no_dia_promocao'.
    """
    try:
        logger.debug(f"Iniciando cálculo do estoque para o produto {codigo_produto} na data de início {data_inicio_promocao}")

        # Calcular estoque médio antes da promoção
        query_estoque_medio = f"""
            SELECT AVG(EstoqueAtual) AS EstoqueMedioAntesPromocao
            FROM auditoriaestoquexport
            WHERE CodigoProduto = {codigo_produto} AND DataHora < '{data_inicio_promocao}';
        """
        result_medio = db_manager.execute_query(query_estoque_medio)
        estoque_medio_antes_promocao = result_medio['data'][0][0] if result_medio['data'] else 0

        # Calcular estoque no dia da promoção
        query_estoque_no_dia = f"""
            SELECT EstoqueAtual
            FROM auditoriaestoquexport
            WHERE CodigoProduto = {codigo_produto} AND DataHora <= '{data_inicio_promocao} 23:59:59'
            ORDER BY DataHora DESC
            LIMIT 1;
        """
        result_dia = db_manager.execute_query(query_estoque_no_dia)
        estoque_no_dia_promocao = result_dia['data'][0][0] if result_dia['data'] else 0

        return {
            'estoque_medio_antes_promocao': estoque_medio_antes_promocao,
            'estoque_no_dia_promocao': estoque_no_dia_promocao
        }

    except Exception as e:
        logger.error(f"Erro ao calcular estoques para a promoção (produto: {codigo_produto}, data_inicio_promocao: {data_inicio_promocao}): {e}")
        logger.error(traceback.format_exc())
        return {
            'estoque_medio_antes_promocao': 0,
            'estoque_no_dia_promocao': 0
        }

def calculate_category_impact(promo, df_sales, df_produtos, df_historical_sales, data_column):
    """
    Calcula o impacto da promoção nas vendas de outras categorias de produtos.

    Args:
        promo (pd.Series): Linha da promoção atual.
        df_sales (pd.DataFrame): DataFrame contendo as vendas durante o período da promoção.
        df_produtos (pd.DataFrame): DataFrame contendo informações dos produtos.
        df_historical_sales (pd.DataFrame): DataFrame contendo vendas anteriores à promoção.
        data_column (str): O nome da coluna que contém as datas de venda.

    Retorna:
        float: O impacto percentual nas outras categorias.
    """
    try:
        logger.debug(f"Iniciando cálculo do impacto em outras categorias para a promoção: {promo['id']}")

        codigo_produto = promo['CodigoProduto']
        start_date = promo['DataInicioPromocao']
        end_date = promo['DataFimPromocao']

        # Verifique se 'CodigoGrupo' existe em df_produtos
        if 'CodigoGrupo' not in df_produtos.columns:
            logger.error(f"A coluna 'CodigoGrupo' não está presente em df_produtos.")
            return 0.0

        # Verifique se a coluna 'data_column' está presente em df_sales
        if data_column not in df_sales.columns:
            logger.error(f"A coluna '{data_column}' não está presente em df_sales.")
            return 0.0

        # Verifique se a coluna 'data_column' está presente em df_historical_sales
        if data_column not in df_historical_sales.columns:
            logger.error(f"A coluna '{data_column}' não está presente em df_historical_sales.")
            return 0.0

        # Categoria do produto em promoção
        categoria_produto = df_produtos.loc[df_produtos['CodigoProduto'] == codigo_produto, 'CodigoGrupo'].values[0]

        # Filtrar vendas de outras categorias durante o período da promoção
        outras_vendas = df_sales[
            (df_sales[data_column] >= start_date) &
            (df_sales[data_column] <= end_date) &
            (df_sales['CodigoProduto'].isin(df_produtos[df_produtos['CodigoGrupo'] != categoria_produto]['CodigoProduto']))
        ]

        if outras_vendas.empty:
            return 0.0

        # Comparar com as vendas médias anteriores
        outras_vendas_anteriores = df_historical_sales[
            (df_historical_sales['CodigoProduto'].isin(df_produtos[df_produtos['CodigoGrupo'] != categoria_produto]['CodigoProduto'])) &
            (df_historical_sales[data_column] < start_date)
        ]

        if outras_vendas_anteriores.empty:
            return 0.0

        media_anteriores = outras_vendas_anteriores['ValorTotal'].mean()
        total_durante_promocao = outras_vendas['ValorTotal'].sum()

        impacto = ((total_durante_promocao - media_anteriores) / media_anteriores) * 100

        return impacto

    except KeyError as e:
        logger.error(f"Erro ao acessar a chave {e} no cálculo do impacto em outras categorias: {e}")
        logger.error(traceback.format_exc())
        return 0.0

    except Exception as e:
        logger.error(f"Erro ao calcular impacto em outras categorias para a promoção {promo['id']}: {e}")
        logger.error(traceback.format_exc())
        return 0.0


def calcular_volume_pos_promocao(codigo_produto, data_fim_promocao):
    """
    Calcula o volume de vendas do produto após o término da promoção.

    Args:
        codigo_produto (int): O código do produto.
        data_fim_promocao (str): A data de término da promoção (formato 'YYYY-MM-DD').

    Retorna:
        float: O volume de vendas após a promoção.
    """
    try:
        logger.debug(f"Iniciando cálculo do volume de vendas pós-promoção para o produto {codigo_produto}, fim da promoção: {data_fim_promocao}")

        # Verifique se data_fim_promocao é uma string ou um objeto datetime
        if not isinstance(data_fim_promocao, (str, datetime.date)):
            logger.error("data_fim_promocao precisa ser uma string ou um objeto datetime.date.")
            return 0.0

        # Converta `data_fim_promocao` para string se for um objeto `datetime.date`
        if isinstance(data_fim_promocao, datetime.date):
            data_fim_promocao = data_fim_promocao.strftime('%Y-%m-%d')

        # Período de análise após a promoção (7 dias)
        data_fim_periodo = (datetime.strptime(data_fim_promocao, '%Y-%m-%d') + timedelta(days=7)).strftime('%Y-%m-%d')

        # Realize a query para calcular o volume pós-promocional
        query_volume_pos = f"""
            SELECT SUM(Quantidade) AS VolumePosPromocao
            FROM vendasprodutosexport vpe
            JOIN vendasexport ve ON vpe.CodigoVenda = ve.Codigo
            WHERE vpe.CodigoProduto = {codigo_produto} AND ve.data BETWEEN '{data_fim_promocao}' AND '{data_fim_periodo}';
        """
        result_volume = db_manager.execute_query(query_volume_pos)
        volume_pos_promocao = result_volume['data'][0][0] if result_volume['data'] else 0

        return volume_pos_promocao

    except KeyError as e:
        logger.error(f"Erro ao acessar a chave {e} no cálculo do volume pós-promoção (produto: {codigo_produto}, data_fim_promocao: {data_fim_promocao}): {e}")
        logger.error(traceback.format_exc())
        return 0.0

    except Exception as e:
        logger.error(f"Erro ao calcular o volume de vendas pós-promoção para o produto {codigo_produto}, fim da promoção: {data_fim_promocao}: {e}")
        logger.error(traceback.format_exc())
        return 0.0


def comparar_com_promocoes_passadas(codigo_produto, data_inicio_promocao, data_fim_promocao):
    """
    Compara os indicadores da promoção atual com promoções anteriores do mesmo produto.

    Args:
        codigo_produto (int): O código do produto.
        data_inicio_promocao (str): A data de início da promoção (formato 'YYYY-MM-DD').
        data_fim_promocao (str): A data de término da promoção (formato 'YYYY-MM-DD').

    Retorna:
        dict: Um dicionário com a comparação dos indicadores.
    """
    try:
        query_comparacao = f"""
            SELECT AVG(QuantidadeTotal) AS QuantidadeMediaPassada, AVG(ValorTotalVendido) AS ValorMedioPassado
            FROM sales_indicators
            WHERE CodigoProduto = {codigo_produto} AND DataFimPromocao < '{data_inicio_promocao}';
        """
        result_comparacao = db_manager.execute_query(query_comparacao)

        if result_comparacao['data']:
            quantidade_media_passada = result_comparacao['data'][0][0] or 0.0
            valor_medio_passado = result_comparacao['data'][0][1] or 0.0
        else:
            quantidade_media_passada = 0.0
            valor_medio_passado = 0.0

        return {
            'QuantidadeMediaPassada': quantidade_media_passada,
            'ValorMedioPassado': valor_medio_passado
        }

    except Exception as e:
        logger.error(f"Erro ao comparar com promoções passadas: {e}")
        return {
            'QuantidadeMediaPassada': 0.0,
            'ValorMedioPassado': 0.0
        }

def calculate_promotion_indicators():
    """
    Calcula os indicadores de promoção usando a tabela de promoções identificadas,
    e insere esses indicadores na tabela sales_indicators.
    """
    try:
        logger.info("Iniciando o cálculo dos indicadores de promoção...")

        # Carregar dados das tabelas com JOIN para incluir CodigoGrupo
        promotions_query = "SELECT * FROM promotions_identified"
        vendas_query = "SELECT * FROM vendasexport"
        produtos_query = """
            SELECT vp.*, pe.CodigoGrupo
            FROM vendasprodutosexport vp
            JOIN produtosexport pe ON vp.CodigoProduto = pe.Codigo
        """
        historical_sales_query = """
            SELECT v.Codigo AS CodigoVenda, p.CodigoProduto, v.data AS data, v.TotalPedido, 
                p.Quantidade, p.valorunitario, p.ValorTotal, pe.CodigoGrupo
            FROM vendasexport v
            INNER JOIN vendasprodutosexport p ON v.Codigo = p.CodigoVenda
            INNER JOIN produtosexport pe ON p.CodigoProduto = pe.Codigo
            WHERE v.data < CURDATE()
        """

        promotions_result = db_manager.execute_query(promotions_query)
        vendas_result = db_manager.execute_query(vendas_query)
        produtos_result = db_manager.execute_query(produtos_query)
        historical_sales_result = db_manager.execute_query(historical_sales_query)

        if all('data' in result and 'columns' in result for result in [promotions_result, vendas_result, produtos_result, historical_sales_result]):
            df_promotions = pd.DataFrame(promotions_result['data'], columns=promotions_result['columns'])
            df_vendas = pd.DataFrame(vendas_result['data'], columns=vendas_result['columns'])
            df_produtos = pd.DataFrame(produtos_result['data'], columns=produtos_result['columns'])
            df_historical_sales = pd.DataFrame(historical_sales_result['data'], columns=historical_sales_result['columns'])
            
            # Definir df_sales usando o merge entre df_vendas e df_produtos
            df_sales = pd.merge(df_vendas, df_produtos, how='inner', left_on='Codigo', right_on='CodigoVenda')

            # Verifique e renomeie a coluna 'Data' para 'data' se necessário
            if 'Data' in df_sales.columns and 'data' not in df_sales.columns:
                df_sales.rename(columns={'Data': 'data'}, inplace=True)

            if 'Data' in df_historical_sales.columns and 'data' not in df_historical_sales.columns:
                df_historical_sales.rename(columns={'Data': 'data'}, inplace=True)

            # Processamento paralelo para cálculo de indicadores
            with ThreadPoolExecutor() as executor:
                futures = [executor.submit(calculate_and_insert_indicators, promo, df_sales, df_historical_sales, df_produtos, 'data') for _, promo in df_promotions.iterrows()]
                
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        logger.info("Cálculo de indicadores finalizado com sucesso.")
                    else:
                        logger.warning("Ocorreu um problema durante o cálculo dos indicadores.")

    except Exception as e:
        logger.error(f"Erro ao calcular indicadores de promoção: {e}")



def calculate_and_insert_indicators(promo, df_sales, df_historical_sales, df_produtos, data_column):
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

        # Verificar se a coluna 'data' existe no DataFrame df_sales
        if data_column not in df_sales.columns:
            logger.error(f"[Thread-{thread_id}] A coluna '{data_column}' não está presente no DataFrame 'df_sales'.")
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
            logger.error(f"Colunas presentes: {df_historical_sales.columns.tolist()}")
            return False

        # Verificar se a coluna 'data' existe no DataFrame df_historical_sales
        if data_column not in df_historical_sales.columns:
            logger.error(f"[Thread-{thread_id}] A coluna '{data_column}' não está presente no DataFrame 'df_historical_sales'.")
            return False

        # Filtrar vendas antes da promoção
        sales_before_promo = df_historical_sales[
            (df_historical_sales['CodigoProduto'] == product_code) &
            (df_historical_sales[data_column] < start_date)
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

        # Calcular Estoque Médio Antes da Promoção e Estoque no Dia da Promoção
        estoques = calcular_estoque_para_promocao(product_code, start_date)

        # Calcular o Impacto em Outras Categorias de Produtos
        impacto_categorias = calculate_category_impact(promo, df_sales, df_historical_sales, df_produtos, data_column)

        # Calcular Volume de Vendas Pós-Promoção
        volume_pos_promocao = calcular_volume_pos_promocao(product_code, end_date)

        # Comparação com Promoções Passadas
        comparacao_passada = comparar_com_promocoes_passadas(product_code, start_date, end_date)

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
            'ElasticidadePrecoDemanda': elasticidade_preco_demanda,
            'EstoqueMedioAntesPromocao': estoques['estoque_medio_antes_promocao'],
            'EstoqueNoDiaPromocao': estoques['estoque_no_dia_promocao'],
            'ImpactoEmOutrasCategorias': impacto_categorias,
            'VolumeVendasPosPromocao': volume_pos_promocao,
            'ComparacaoComPromocoesPassadas': comparacao_passada['QuantidadeMediaPassada']  # ou o que for apropriado
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