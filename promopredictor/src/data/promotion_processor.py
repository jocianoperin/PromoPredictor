# src/data/promotion_processor.py
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

logger = get_logger(__name__)

def insert_promotion(promo: dict):
    connection = get_db_connection()
    if connection is not None: 
        
        try:
            with connection.cursor() as cursor:
                insert_query = """
                    INSERT INTO promotions_identified (CodigoProduto, DataInicioPromocao, DataFimPromocao, ValorUnitario, ValorTabela)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    ValorUnitario = VALUES(ValorUnitario),
                    ValorTabela = VALUES(ValorTabela),
                    DataInicioPromocao = VALUES(DataInicioPromocao),
                    DataFimPromocao = VALUES(DataFimPromocao);
                """
                cursor.execute(insert_query, (promo['CodigoProduto'], promo['DataInicioPromocao'],
                                            promo['DataFimPromocao'], promo['ValorUnitario'],
                                            promo['ValorTabela']))
                connection.commit()
        except Exception as e:
            logger.error(f"Erro ao inserir/atualizar período promocional: {e}")
            if connection is not None:
                connection.rollback()
        finally:
            if connection is not None:
                connection.close()
    else:
        logger.error("Falha ao obter conexão com o banco de dados.")

def fetch_all_products() -> pd.DataFrame:
    connection = get_db_connection()
    if connection is None:
        logger.error("Falha ao obter conexão com o banco de dados.")
        return pd.DataFrame()

    try:
        with connection.cursor(dictionary=True) as cursor:
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
            cursor.execute(query)
            return pd.DataFrame(cursor.fetchall())
    except Exception as e:
        logger.error(f"Erro ao buscar produtos: {e}")
        return pd.DataFrame()
    finally:
        if connection is not None:
            connection.close()

def process_product_chunk(product_df: pd.DataFrame) -> int:
    promotions_identified = 0
    if not product_df.empty:
        product_df.sort_values(by='Data', inplace=True)

        avg_cost = product_df['ValorCusto'].mean()
        avg_sale_price = product_df['ValorUnitario'].mean()

        for _, row in product_df.iterrows():
            if row['ValorUnitario'] < avg_sale_price * 0.95 and abs(row['ValorCusto'] - avg_cost) < avg_cost * 0.05:
                # Início e término das promoções não implementados no código original
                # Lógica para determinar o início e término das promoções deve ser adicionada aqui
                pass

        # Lembre-se de chamar insert_promotion para cada promoção identificada
        # insert_promotion(promo)
    
    return promotions_identified

def organize_sales_by_product(products: pd.DataFrame) -> DataFrameGroupBy:
    return products.groupby('CodigoProduto')

def process_chunks(products_df: pd.DataFrame):
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