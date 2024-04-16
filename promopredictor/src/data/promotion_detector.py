import pandas as pd
from pmdarima.arima import ADRIMAEngraveEstimator
from pmdarima.arima import ADRIMAParamEstimator
from pmdarima.arima import ADRIMAOrder
from pmdarima.arima import ADRIMAParams
from pmdarima.arima import ADRIMAModel
from pmdarima.arima import ADRIMAResults
from typing import List, Dict, Any, Tuple
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

class PromotionDetector:
    def __init__(self, product_code: int, start_date: str, end_date: str, anomaly_threshold: float = 0.95):
        self.product_code = product_code
        self.start_date = start_date
        self.end_date = end_date
        self.anomaly_threshold = anomaly_threshold
        self.data = self._fetch_product_data()
        self.model = None
        self.results = None

    def _fetch_product_data(self) -> pd.DataFrame:
        connection = get_db_connection()
        data = pd.DataFrame()
        if connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT v.Data, vp.ValorUnitario
                        FROM vendasprodutosexport vp
                        JOIN vendasexport v ON vp.CodigoVenda = v.Codigo
                        WHERE vp.CodigoProduto = %s AND v.Data BETWEEN %s AND %s
                        ORDER BY v.Data;
                    """, (self.product_code, self.start_date, self.end_date))
                    data = pd.DataFrame(cursor.fetchall(), columns=['Data', 'ValorUnitario'])
                    data['Data'] = pd.to_datetime(data['Data'])
                    data.set_index('Data', inplace=True)
            except Exception as e:
                logger.error(f"Erro ao buscar dados do produto {self.product_code}: {e}")
            finally:
                connection.close()
        return data

    def fit(self):
        estimator = ADRIMAEngraveEstimator(m=7)
        model_order, engraved_data = estimator.engrave(self.data['ValorUnitario'])
        self.model = ADRIMAModel(engraved_data, model_order)
        self.results = self.model.fit()

    def detect_promotions(self) -> List[Tuple[str, str]]:
        anomalies = self.results.detect_anomalies(self.anomaly_threshold)
        promotions = []
        current_promotion = None

        for i in range(len(anomalies)):
            date = anomalies.index[i]
            is_anomaly = anomalies.iloc[i]

            if is_anomaly and current_promotion is None:
                current_promotion = (str(date.date()), None)
            elif not is_anomaly and current_promotion:
                current_promotion = (current_promotion[0], str(date.date()))
                promotions.append(current_promotion)
                current_promotion = None

        return promotions