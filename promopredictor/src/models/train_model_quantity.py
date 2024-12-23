from autokeras import AutoModel, RegressionHead, Input
import pandas as pd
from pathlib import Path
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

BASE_DATA_DIR = Path(__file__).parent.parent.parent / "data"

def load_data():
    """
    Carrega os dados limpos do CSV e separa por períodos para treino e validação.

    Retorna:
        tuple: (DataFrame de treino, DataFrame de validação)
    """
    file_path = BASE_DATA_DIR / "cleaned/produto_26173_clean.csv"
    logger.info(f"Lendo dados de {file_path}.")
    
    df = pd.read_csv(file_path, parse_dates=['Data'])
    
    # Separar dados de treinamento (até 2022) e validação (2023)
    train_data = df[(df['Data'] >= '2019-01-01') & (df['Data'] <= '2022-12-31')]
    validation_data = df[(df['Data'] >= '2023-01-01') & (df['Data'] <= '2023-12-31')]
    
    return train_data, validation_data

def prepare_features_and_target(df):
    """
    Prepara os recursos (features) e o alvo (target) para treinamento.

    Parâmetros:
        df (DataFrame): Dados de entrada.

    Retorna:
        tuple: (numpy.ndarray, numpy.ndarray)
    """
    features = [
        'DiaDaSemana', 'Mes', 'Dia', 'QuantidadeLiquida', 
        'Rentabilidade', 'DescontoAplicado', 'AcrescimoAplicado'
    ]
    target = 'Quantidade'
    X = df[features].to_numpy()
    y = df[target].to_numpy()
    return X, y

def train_model():
    """
    Treina o modelo usando Auto-Keras e salva o melhor modelo.
    """
    train_data, validation_data = load_data()
    X_train, y_train = prepare_features_and_target(train_data)
    X_val, y_val = prepare_features_and_target(validation_data)

    # Criar o modelo usando Auto-Keras AutoModel
    logger.info("Iniciando o treinamento do modelo.")
    input_node = Input()
    output_node = RegressionHead()
    model = AutoModel(inputs=input_node, outputs=output_node, max_trials=50)

    # Treinar o modelo
    model.fit(X_train, y_train, validation_data=(X_val, y_val), epochs=50, batch_size=32)

    # Avaliar o modelo
    logger.info("Avaliando o modelo.")
    evaluation = model.evaluate(X_val, y_val, return_dict=True)
    logger.info(f"Resultados de validação: {evaluation}")

    # Salvar o modelo
    model_path = BASE_DATA_DIR / "models/structured_data_model"
    model.export_model().save(f"{model_path}.keras")

    logger.info(f"Modelo salvo em {model_path}.")

if __name__ == "__main__":
    train_model()