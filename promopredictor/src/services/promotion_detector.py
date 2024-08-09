import pandas as pd
from concurrent.futures import ThreadPoolExecutor
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
                insert_promotions(promotions_df)
            else:
                logger.info("Nenhuma promoção detectada.")
        else:
            logger.error("Erro ao carregar dados dos produtos.")
    except Exception as e:
        logger.error(f"Erro ao detectar promoções: {e}")

def process_group(group, window_size, threshold):
    """
    Processa um grupo de produtos para detectar promoções.
    """
    group = group.sort_values(by='Data')
    group['PriceChange'] = group['valorunitario'].pct_change(window_size)
    group['Promotion'] = (group['PriceChange'] <= threshold) & (group['ValorCusto'] == group['ValorCusto'].shift(window_size))
    
    return group[group['Promotion']]

def insert_promotions(promotions_df):
    """
    Insere as promoções detectadas na tabela `promotions_identified`.
    """
    try:
        for _, row in promotions_df.iterrows():
            insert_query = f"""
            INSERT INTO promotions_identified (CodigoProduto, DataInicioPromocao, DataFimPromocao, valorunitario, ValorTabela) 
            VALUES ({row['CodigoProduto']}, '{row['Data'].date()}', '{row['Data'].date()}', {row['valorunitario']}, {row['ValorTabela']})
            """
            db_manager.execute_query(insert_query)
        logger.info("Promoções inseridas na tabela promotions_identified com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao inserir promoções na tabela: {e}")
