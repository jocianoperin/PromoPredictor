import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import threading
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def detect_promotions(window_size=2, threshold=-0.05, n_workers=4):
    """
    Detecta promoções com base na redução de preço de venda em 5% ou mais, sem alteração no custo.

    Args:
        window_size (int): Tamanho da janela deslizante para calcular a mudança de preço.
        threshold (float): Percentual de queda no preço para considerar como promoção.
        n_workers (int): Número de threads para executar o processamento em paralelo.

    Retorna:
        None: A função não retorna valores, mas insere promoções detectadas na base de dados.
    """
    try:
        # Carregar os dados das tabelas vendasprodutosexport e vendasexport
        produtos_query = """
        SELECT vp.*, ve.Data
        FROM vendasprodutosexport vp
        JOIN vendasexport ve ON vp.CodigoVenda = ve.Codigo
        """
        produtos_result = db_manager.execute_query(produtos_query)
        
        if 'data' in produtos_result and 'columns' in produtos_result:
            df_produtos = pd.DataFrame(produtos_result['data'], columns=produtos_result['columns'])
            df_produtos['Data'] = pd.to_datetime(df_produtos['Data'])

            # Agrupa por produto e processa em paralelo
            grouped = df_produtos.groupby('CodigoProduto')
            results = []

            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                futures = [executor.submit(process_group, group, window_size, threshold) for _, group in grouped]
                for future in futures:
                    result = future.result()
                    if not result.empty:
                        results.append(result)

            if results:
                promotions_df = pd.concat(results, ignore_index=True)
                logger.info(f"Promoções detectadas: {promotions_df.shape[0]} registros.")

                # Combinar promoções consecutivas antes de inserir
                promotions_df_combined = combine_consecutive_promotions(promotions_df)

                # Inserir promoções combinadas
                insert_promotions(promotions_df_combined)
            else:
                logger.info("Nenhuma promoção detectada.")
        else:
            logger.error("Erro ao carregar dados dos produtos.")
    except Exception as e:
        logger.error(f"Erro ao detectar promoções: {e}")

def process_group(group, window_size, threshold):
    """
    Processa um grupo de produtos para detectar promoções.

    Args:
        group (DataFrame): DataFrame contendo os dados de um grupo de produtos.
        window_size (int): Tamanho da janela deslizante para calcular a mudança de preço.
        threshold (float): Percentual de queda no preço para considerar como promoção.

    Retorna:
        DataFrame: Um DataFrame com as promoções detectadas para o grupo.
    """

    group = group.sort_values(by='Data')
    group['PriceChange'] = group['valorunitario'].pct_change(window_size)
    
    # Nova condição para verificar se o produto está em promoção e se o valor unitário é menor que o valor de tabela
    group['Promotion'] = (
        (group['PrecoemPromocao'] == 1) & 
        (group['valorunitario'] < group['ValorTabela']) &
        (group['PriceChange'] <= threshold) & 
        (group['ValorCusto'] == group['ValorCusto'].shift(window_size))
    )
    
    return group[group['Promotion']]

def combine_consecutive_promotions(df_promotions):
    """
    Combina promoções consecutivas para o mesmo produto.

    Args:
        df_promotions (DataFrame): DataFrame contendo as promoções detectadas.

    Retorna:
        DataFrame: Um DataFrame com promoções consecutivas combinadas em uma única promoção.
    """

    df_promotions_sorted = df_promotions.sort_values(by=['CodigoProduto', 'Data'])
    combined_promotions = []

    current_promotion = None

    for _, row in df_promotions_sorted.iterrows():
        if current_promotion is None:
            current_promotion = row.copy()
            current_promotion['DataInicioPromocao'] = current_promotion['Data']  # Define a data de início
            current_promotion['DataFimPromocao'] = current_promotion['Data']  # Inicializa com a data de início
        else:
            if (current_promotion['CodigoProduto'] == row['CodigoProduto']) and \
               (current_promotion['valorunitario'] == row['valorunitario']) and \
               (current_promotion['ValorCusto'] == row['ValorCusto']) and \
               (current_promotion['ValorTabela'] == row['ValorTabela']) and \
               (current_promotion['DataFimPromocao'] == row['Data'] - pd.Timedelta(days=1)):
                # Estender a promoção atual
                current_promotion['DataFimPromocao'] = row['Data']
            else:
                # Adicionar a promoção combinada anterior à lista
                combined_promotions.append(current_promotion)
                # Iniciar uma nova promoção
                current_promotion = row.copy()
                current_promotion['DataInicioPromocao'] = current_promotion['Data']  # Define a data de início
                current_promotion['DataFimPromocao'] = current_promotion['Data']  # Inicializa com a data de início

    # Adicionar a última promoção combinada
    if current_promotion is not None:
        combined_promotions.append(current_promotion)

    return pd.DataFrame(combined_promotions)

def insert_promotions(promotions_df):
    """
    Insere ou atualiza as promoções detectadas na tabela `promotions_identified`.

    Args:
        promotions_df (DataFrame): DataFrame contendo as promoções a serem inseridas ou atualizadas.

    Retorna:
        None: A função não retorna valores, mas insere ou atualiza promoções na base de dados.
    """
    
    try:
        for _, row in promotions_df.iterrows():
            thread_id = threading.get_ident()

            db_manager.begin_transaction()  # Inicia a transação

            # Verificar se já existe uma promoção com os mesmos detalhes
            check_query = f"""
            SELECT id, DataInicioPromocao, DataFimPromocao 
            FROM promotions_identified 
            WHERE CodigoProduto = {row['CodigoProduto']} 
            AND ValorUnitario = {row['valorunitario']}
            AND ValorTabela = {row['ValorTabela']}
            AND (
                (DataInicioPromocao <= '{row['DataFimPromocao']}' AND DataFimPromocao >= '{row['DataInicioPromocao']}')
            )
            FOR UPDATE;
            """
            result = db_manager.execute_query(check_query)

            if result['data']:
                # Se a promoção já existe, apenas atualize as datas.
                update_query = f"""
                UPDATE promotions_identified
                SET DataFimPromocao = GREATEST(DataFimPromocao, '{row['DataFimPromocao']}'),
                    DataInicioPromocao = LEAST(DataInicioPromocao, '{row['DataInicioPromocao']}')
                WHERE id = {result['data'][0][0]}
                """
                db_manager.execute_query(update_query)
            else:
                # Inserir nova promoção
                insert_query = f"""
                INSERT INTO promotions_identified (CodigoProduto, DataInicioPromocao, DataFimPromocao, valorunitario, ValorCusto, ValorTabela) 
                VALUES ({row['CodigoProduto']}, '{row['DataInicioPromocao']}', '{row['DataFimPromocao']}', {row['valorunitario']}, {row['ValorCusto']}, {row['ValorTabela']})
                ON DUPLICATE KEY UPDATE 
                    DataFimPromocao = GREATEST(DataFimPromocao, '{row['DataFimPromocao']}'),
                    DataInicioPromocao = LEAST(DataInicioPromocao, '{row['DataInicioPromocao']}')
                """
                db_manager.execute_query(insert_query)

            db_manager.commit_transaction()  # Comita a transação
            logger.debug(f"[Thread-{thread_id}] Promoções inseridas/atualizadas na tabela promotions_identified com sucesso.")
    except Exception as e:
        db_manager.rollback_transaction()  # Faz rollback da transação em caso de erro
        logger.error(f"[Thread-{thread_id}] Erro ao inserir/atualizar promoções na tabela: {e}")
