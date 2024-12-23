import pandas as pd
import tensorflow as tf
from pathlib import Path
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

BASE_DATA_DIR = Path(__file__).parent.parent.parent / "data"

def load_model():
    """
    Carrega o modelo treinado salvo em disco.

    Retorna:
        tf.keras.Model: Modelo carregado.
    """
    model_path = BASE_DATA_DIR / "models/structured_data_model.keras"

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

def predict():
    """
    Realiza predições usando o modelo treinado e salva os resultados.
    """
    model = load_model()
    prediction_data = load_prediction_data()
    
    # Preparar os dados para predição
    features = [
        'DiaDaSemana', 'Mes', 'Dia', 'QuantidadeLiquida', 
        'Rentabilidade', 'DescontoAplicado', 'AcrescimoAplicado'
    ]
    X_pred = prediction_data[features]
    
    # Fazer predições
    logger.info("Realizando predições.")
    prediction_data['Predicted_Quantidade'] = model.predict(X_pred).flatten()
    
    # Salvar as predições
    output_path = BASE_DATA_DIR / "predictions/produto_26173_predictions.csv"
    prediction_data.to_csv(output_path, index=False, sep=',')
    logger.info(f"Predições salvas em {output_path}.")

if __name__ == "__main__":
    predict_quantity()