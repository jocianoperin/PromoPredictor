from src.services.database import db_manager
from src.utils.logging_config import get_logger
import pandas as pd
import joblib

logger = get_logger(__name__)

def fetch_new_data():
    """
    Simula a busca por novos dados para predição.
    """
    new_data = pd.DataFrame({
        'ValorUnitario': [10.0, 12.5],
        'ValorCusto': [8.0, 9.5],
        'Quantidade': [100, 150]
    })
    return new_data

def preprocess_data(df):
    """
    Aplica o mesmo pré-processamento usado no treinamento.
    """
    df.fillna(df.mean(), inplace=True)
    scaler = joblib.load('scaler.pkl')
    numerical_cols = ['ValorUnitario', 'ValorCusto', 'Quantidade']
    df[numerical_cols] = scaler.transform(df[numerical_cols])
    return df

def make_prediction():
    """
    Carrega o modelo treinado e realiza predições sobre novos dados.
    """
    df = fetch_new_data()
    df = preprocess_data(df)
    model = joblib.load('trained_model.pkl')
    predictions = model.predict(df)
    logger.info(f"Predições: {predictions}")


if __name__ == "__main__":
    make_prediction()
