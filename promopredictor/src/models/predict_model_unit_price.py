import pandas as pd
import numpy as np
import tensorflow as tf
from pathlib import Path
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

BASE_DATA_DIR = Path(__file__).parent.parent.parent / "data"

def load_price_model():
    model_path = BASE_DATA_DIR / "models" / "structured_data_model_unit_price_v2.keras"
    logger.info(f"Carregando modelo de valor unitário de {model_path}")
    return tf.keras.models.load_model(model_path)

def load_future_price_data():
    """
    Carrega o dataset diário de preço e filtra o período que queremos prever (ex: 2024).
    """
    file_path = BASE_DATA_DIR / "cleaned" / "produto_26173_price.csv"
    logger.info(f"Lendo dataset de preço de {file_path}")

    df = pd.read_csv(file_path, parse_dates=['Data'])

    # Filtrando para 2024
    df_pred = df[(df['Data'] >= '2024-01-01') & (df['Data'] <= '2024-03-30')].copy()
    return df_pred

def prepare_features(df: pd.DataFrame):
    """
    Prepara as mesmas features que usamos em train_model_unit_price.
    """
    features = [
        'DiaDaSemana',
        'Mes',
        'Dia',
        'QuantidadeLiquida',
    ]
    X = df[features].fillna(0).to_numpy()
    return X

def predict_price():
    model = load_price_model()
    df_pred = load_future_price_data()
    X_pred = prepare_features(df_pred)

    logger.info("Realizando predições de valor unitário (log).")
    y_pred_log = model.predict(X_pred).flatten()
    # Como treinamos em log, voltamos para escala original
    df_pred['Predicted_ValorUnitario'] = np.expm1(y_pred_log)

    # Salvar
    output_path = BASE_DATA_DIR / "predictions" / "produto_26173_unit_price_predictions_v2.csv"
    df_pred.to_csv(output_path, index=False)
    logger.info(f"Predições de valor unitário salvas em {output_path}")

if __name__ == "__main__":
    predict_price()
