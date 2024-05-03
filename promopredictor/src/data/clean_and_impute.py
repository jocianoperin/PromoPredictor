# src/data/clean_and_impute.py
from concurrent.futures import ThreadPoolExecutor
from src.data.time_series_preparation import prepare_time_series_data
from src.models.arima_model import train_arima, forecast_arima, impute_values
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def clean_and_impute(table, product_column, date_column, value_columns):
    product_ids = db_manager.get_product_ids_with_nulls(table, product_column, value_columns)  # Supondo que esta função retorna uma lista de IDs

    def process_product(product_id):
        for value_column in value_columns:
            data = prepare_time_series_data(table, product_column, date_column, value_column, product_id)
            if data:
                model = train_arima(data)
                if model:
                    forecast = forecast_arima(model, steps=1)  # Supomos uma previsão de um passo à frente
                    impute_values(table, product_column, date_column, value_column, product_id, forecast)
                else:
                    logger.error(f"Falha no treinamento do modelo ARIMA para o produto {product_id}.")
            else:
                logger.error(f"Nenhum dado para imputação encontrado para o produto {product_id}.")

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(process_product, product_ids)
