from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def insert_forecast(product_code, date, actual_price, arima_forecast, rnn_forecast):
   """
   Insere uma previsão de preço no banco de dados.
   Args:
       product_code (int): Código do produto.
       date (datetime.date): Data da previsão.
       actual_price (float): Valor real do produto.
       arima_forecast (float): Previsão do modelo ARIMA.
       rnn_forecast (float): Previsão do modelo RNN.
   """
   try:
       query = """
           INSERT INTO price_forecasts (CodigoProduto, Data, ValorUnitario, PrevisaoARIMA, PrevisaoRNN)
           VALUES (%s, %s, %s, %s, %s)
           ON DUPLICATE KEY UPDATE
               ValorUnitario = VALUES(ValorUnitario),
               PrevisaoARIMA = VALUES(PrevisaoARIMA),
               PrevisaoRNN = VALUES(PrevisaoRNN);
       """
       values = (product_code, date, actual_price, arima_forecast, rnn_forecast)
       db_manager.execute_query(query, values)
   except Exception as e:
       logger.error(f"Erro ao inserir previsão no banco de dados: {e}")