from ..services.database_connection import get_db_connection  # Ajuste o import conforme necessário
from ..utils.logging_config import get_logger
import pandas as pd
import joblib

logger = get_logger(__name__)

def fetch_new_data():
    """
    Simula a busca por novos dados para predição.
    """
    # Implemente aqui a lógica para buscar novos dados
    # Este é um placeholder, substitua pela sua lógica de busca de dados
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
    # Substituindo valores nulos pela média (placeholder, ajuste conforme necessário)
    df.fillna(df.mean(), inplace=True)

    # Normalizando as colunas numéricas (assumindo que o scaler foi salvo)
    scaler = joblib.load('scaler.pkl')  # Certifique-se de que salvou o scaler durante o treinamento
    numerical_cols = ['ValorUnitario', 'ValorCusto', 'Quantidade']
    df[numerical_cols] = scaler.transform(df[numerical_cols])

    return df

def make_prediction():
    df = fetch_new_data()
    df = preprocess_data(df)

    # Carregando o modelo treinado
    model = joblib.load('trained_model.pkl')

    # Fazendo predições
    predictions = model.predict(df)
    logger.info(f"Predições: {predictions}")

if __name__ == "__main__":
    make_prediction()
