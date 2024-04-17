from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger
from src.models.time_series_modeling import train_arima_model, forecast_price
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from datetime import timedelta
from pandas.core.groupby import DataFrameGroupBy
from sqlalchemy.exc import SQLAlchemyError

logger = get_logger(__name__)

def insert_promotion(promo: dict):
    engine = get_db_connection()  # Assume que get_db_connection retorna um engine SQLAlchemy.
    with engine.connect() as connection:
        try:
            logger.info(f"Inserindo/atualizando promoção no banco de dados para o produto {promo['CodigoProduto']}...")
            insert_query = """
                INSERT INTO promotions_identified (CodigoProduto, DataInicioPromocao, DataFimPromocao, ValorUnitario, ValorTabela)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                ValorUnitario = VALUES(ValorUnitario),
                ValorTabela = VALUES(ValorTabela),
                DataInicioPromocao = VALUES(DataInicioPromocao),
                DataFimPromocao = VALUES(DataFimPromocao);
            """
            connection.execute(insert_query, (promo['CodigoProduto'], promo['DataInicioPromocao'],
                                              promo['DataFimPromocao'], promo['ValorUnitario'],
                                              promo['ValorTabela']))
            connection.commit()
            logger.info("Promoção inserida/atualizada com sucesso.")
        except SQLAlchemyError as e:
            logger.error(f"Erro ao inserir/atualizar período promocional: {e}")
            connection.rollback()
        finally:
            engine.dispose()

def fetch_all_products() -> pd.DataFrame:
    engine = get_db_connection()  # Assume que get_db_connection retorna um engine SQLAlchemy.
    try:
        with engine.connect() as connection:
            logger.info("Buscando produtos do banco de dados...")
            query = """
                SELECT 
                    vp.CodigoProduto, 
                    v.Data, 
                    vp.ValorUnitario, 
                    vp.ValorCusto,
                    vp.ValorTabela
                FROM vendasprodutosexport vp
                JOIN vendasexport v ON vp.CodigoVenda = v.Codigo
                ORDER BY vp.CodigoProduto, v.Data;
            """
            result = connection.execute(query)
            products = pd.DataFrame(result.fetchall(), columns=result.keys())
            logger.info(f"Produtos buscados com sucesso. Total de produtos: {len(products)}")
            return products
    except SQLAlchemyError as e:
        logger.error(f"Erro ao buscar produtos: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()

def process_product_chunk(df: pd.DataFrame) -> int:
    promotions_identified = 0
    if not df.empty:
        df.sort_values(by='Data', inplace=True)
        df['Data'] = pd.to_datetime(df['Data'])

        # Cria a série temporal de preços para modelagem
        price_series = df.set_index('Data')['ValorUnitario']
        
        # Treina o modelo ARIMA
        model_fit = train_arima_model(price_series)
        
        # Se o modelo ARIMA falhar, a função train_arima_model deve retornar None
        if model_fit is None:
            logger.error(f"Não foi possível treinar o modelo ARIMA para o produto {df['CodigoProduto'].iloc[0]}")
            return promotions_identified
        
        promo_start, promo_end, previous_date = None, None, None
        for _, row in df.iterrows():
            current_date = row['Data']
            forecasted_price = forecast_price(model_fit, steps=1)
            
            # Condição de promoção baseada na previsão do modelo ARIMA
            if forecasted_price is not None and row['ValorUnitario'] < forecasted_price * 0.95:
                promo_end = current_date
                if promo_start is None:
                    promo_start = current_date
            else:
                if promo_start is not None and promo_end is not None:
                    # Registra a promoção
                    insert_promotion({
                        'CodigoProduto': row['CodigoProduto'],
                        'DataInicioPromocao': promo_start,
                        'DataFimPromocao': promo_end,
                        'ValorUnitario': row['ValorUnitario'],
                        'ValorTabela': row['ValorTabela'],
                    })
                    promotions_identified += 1
                    promo_start, promo_end = None, None
            previous_date = current_date
        
        # Verificar se a última promoção foi finalizada
        if promo_start is not None and promo_end is not None:
            insert_promotion({
                'CodigoProduto': row['CodigoProduto'],
                'DataInicioPromocao': promo_start,
                'DataFimPromocao': promo_end,
                'ValorUnitario': row['ValorUnitario'],
                'ValorTabela': row['ValorTabela'],
            })
            promotions_identified += 1

    return promotions_identified

def organize_sales_by_product(products: pd.DataFrame) -> DataFrameGroupBy:
    logger.info("Organizando vendas por produto...")
    return products.groupby('CodigoProduto')

def process_chunks(products_df: pd.DataFrame):
    if products_df.empty:
        logger.info("Nenhum produto encontrado para processamento.")
        return

    product_sales = organize_sales_by_product(products_df)
    total_promotions_identified = 0

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_product_chunk, group.copy()): code for code, group in product_sales}
        for future in as_completed(futures):
            total_promotions_identified += future.result()

    logger.info(f"Total de promoções identificadas: {total_promotions_identified}")

if __name__ == "__main__":
    products_df = fetch_all_products()
    if not products_df.empty:
        process_chunks(products_df)
    else:
        logger.info("Nenhum produto encontrado para processamento.")
