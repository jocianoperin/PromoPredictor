import pandas as pd
import tensorflow as tf
from pathlib import Path
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

BASE_DATA_DIR = Path(__file__).parent.parent.parent / "data"

def load_model(model_type):
    """
    Carrega o modelo treinado salvo em disco.

    Parâmetros:
        model_type (str): Tipo do modelo a ser carregado ('quantity' ou 'unit_price').

    Retorna:
        tf.keras.Model: Modelo carregado.
    """
    if model_type == 'quantity':
        model_path = BASE_DATA_DIR / "models/structured_data_model.keras"
    elif model_type == 'unit_price':
        model_path = BASE_DATA_DIR / "models/unit_price_model.keras"
    else:
        logger.error(f"Tipo de modelo desconhecido: {model_type}")
        raise ValueError(f"Tipo de modelo desconhecido: {model_type}")

    logger.info(f"Carregando o modelo de {model_path}.")
    return tf.keras.models.load_model(model_path)

def load_prediction_data():
    """
    Carrega os dados para predição (2024).

    Retorna:
        DataFrame: Dados para predição.
    """
    file_path = BASE_DATA_DIR / "cleaned/produto_26173_clean.csv"
    logger.info(f"Lendo dados de {file_path}.")

    df = pd.read_csv(file_path, parse_dates=['Data'])
    prediction_data = df[(df['Data'] >= '2024-01-01') & (df['Data'] <= '2024-03-30')]

    return prediction_data

def predict(model_type):
    """
    Realiza predições usando o modelo treinado e salva os resultados.

    Parâmetros:
        model_type (str): Tipo do modelo a ser usado ('quantity' ou 'unit_price').
    """
    model = load_model(model_type)
    prediction_data = load_prediction_data()

    # Preparar os dados para predição
    features = [
        'DiaDaSemana', 'Mes', 'Dia', 'QuantidadeLiquida', 
        'Rentabilidade', 'DescontoAplicado', 'AcrescimoAplicado'
    ]
    X_pred = prediction_data[features]

    # Fazer predições
    logger.info(f"Realizando predições para {model_type}.")
    prediction_column = 'Predicted_Quantidade' if model_type == 'quantity' else 'Predicted_ValorUnitarioMedio'
    prediction_data[prediction_column] = model.predict(X_pred).flatten()

    # Salvar as predições
    output_path = BASE_DATA_DIR / f"predictions/produto_26173_{model_type}_predictions.csv"
    prediction_data.to_csv(output_path, index=False, sep=',')
    logger.info(f"Predições salvas em {output_path}.")

if __name__ == "__main__":
    predict('quantity')  # Ou use 'unit_price' para predição de valor unitário médio
