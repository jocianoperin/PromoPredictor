import pandas as pd
import numpy as np
import tensorflow as tf
from pathlib import Path
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

BASE_DATA_DIR = Path(__file__).parent.parent.parent / "data"

# Ajustar o caminho para carregar o modelo centralizado
MODEL_BASE_DIR = Path(__file__).parent.parent.parent / "models" / "price"

def load_price_model():
    model_path = MODEL_BASE_DIR / "structured_data_model_unit_price" / "structured_data_model_unit_price.keras"

    logger.info(f"Carregando modelo de valor unitário de {model_path}")
    return tf.keras.models.load_model(model_path)

def load_future_price_data():
    """
    Carrega o dataset diário de preço e filtra o período que queremos prever (ex: 2024).
    """
    file_path = BASE_DATA_DIR / "cleaned" / "produto_26173_price.csv"
    logger.info(f"Lendo dataset de preço de {file_path}")

    df = pd.read_csv(file_path, parse_dates=['Data'])

    # Adicionar variáveis de defasagem
    for lag in range(1, 4):  # Defasagem de 1 a 3 dias
        df[f'ValorUnitario_lag{lag}'] = df['ValorUnitarioMedio'].shift(lag)
        df[f'QuantidadeLiquida_lag{lag}'] = df['QuantidadeLiquida'].shift(lag)

    # Remover valores nulos criados pelas defasagens
    df = df.dropna()

    # Filtrando para 2024
    df_pred = df[(df['Data'] >= '2024-01-01') & (df['Data'] <= '2024-03-30')].copy()
    return df_pred

def prepare_features(df: pd.DataFrame):
    """
    Prepara as mesmas features que usamos em train_model_unit_price.
    """
    features = [
        'PrecoemPromocao',
        'DiaDaSemana',
        'Mes',
        'Dia',
        'QuantidadeLiquida',
        'is_holiday',
        'is_eve1',
        'is_eve2',
        'is_eve3',
        'ValorCusto',
        'ValorUnitario_lag1',
        'ValorUnitario_lag2',
        'ValorUnitario_lag3',
        'QuantidadeLiquida_lag1',
        'QuantidadeLiquida_lag2',
        'QuantidadeLiquida_lag3',
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
