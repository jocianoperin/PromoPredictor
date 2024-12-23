from autokeras import AutoModel, RegressionHead, Input
import pandas as pd
import tensorflow as tf
from pathlib import Path
from src.utils.logging_config import get_logger
from src.data_processing.feature_engineering import add_rolling_features

logger = get_logger(__name__)

BASE_DATA_DIR = Path(__file__).parent.parent.parent / "data"

# Definir o caminho para armazenar os modelos dentro de `promopredictor/models/quantity`
MODEL_BASE_DIR = Path(__file__).parent.parent.parent / "models" / "quantity"

def load_data(produto_id, window_size=7):
    """
    Carrega os dados limpos do CSV, aplica engenharia de recursos e separa por períodos.

    Args:
        produto_id (int): Código do produto.
        window_size (int): Tamanho da janela deslizante.

    Returns:
        tuple: Dados de treinamento e validação.
    """
    file_path = BASE_DATA_DIR / "cleaned" / f"produto_{produto_id}_clean.csv"
    logger.info(f"Lendo dados de {file_path}.")
    
    df = pd.read_csv(file_path, parse_dates=['Data'])

    # Adicionar variáveis de janela deslizante
    df = add_rolling_features(
        df,
        columns=['Quantidade'],
        window_size=window_size
    )

    # Remover valores nulos
    df = df.dropna()
    
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
        'Rentabilidade', 'DescontoAplicado', 'AcrescimoAplicado',
        'Quantidade_rolling_mean_7', 'Quantidade_rolling_std_7', 'Quantidade_rolling_sum_7'
    ]
    target = 'Quantidade'
    X = df[features].to_numpy()
    y = df[target].to_numpy()
    return X, y

def train_model(produto_id, window_size=7):
    """
    Treina o modelo usando Auto-Keras e salva o melhor modelo, incorporando janela flutuante.
    """
    # Criar o diretório para salvar o modelo, se necessário
    MODEL_BASE_DIR.mkdir(parents=True, exist_ok=True)

    train_data, validation_data = load_data(produto_id)

    # Adicionar variáveis de janela flutuante
    rolling_columns = ['QuantidadeLiquida', 'Rentabilidade']  # Colunas para calcular rolling features
    train_data = add_rolling_features(train_data, rolling_columns, window_size)
    validation_data = add_rolling_features(validation_data, rolling_columns, window_size)

    # Preparar as features e o target
    X_train, y_train = prepare_features_and_target(train_data)
    X_val, y_val = prepare_features_and_target(validation_data)

    # Criar o modelo usando Auto-Keras AutoModel
    logger.info(f"Iniciando treinamento para o produto {produto_id}.")
    input_node = Input()
    output_node = RegressionHead()
    model = AutoModel(
        inputs=input_node, 
        outputs=output_node, 
        max_trials=50,
        overwrite=False,  # <-- força recriar o tuner do zero
        project_name=str(MODEL_BASE_DIR / f"produto_{produto_id}_quantity_model")
    )

    # Treinar o modelo
    model.fit(
        X_train, y_train, 
        validation_data=(X_val, y_val), 
        epochs=50, 
        batch_size=32,
        callbacks=[
            tf.keras.callbacks.EarlyStopping(
                monitor="val_loss",
                patience=10,
                restore_best_weights=True
            )
        ]
    )

    # Avaliar o modelo
    logger.info(f"Avaliando o modelo para o produto {produto_id}.")
    evaluation = model.evaluate(X_val, y_val, return_dict=True)
    logger.info(f"Resultados de validação para o produto {produto_id}: {evaluation}")

    # Salvar o modelo
    model_path = MODEL_BASE_DIR / f"produto_{produto_id}_quantity_model"
    model.export_model().save(f"{model_path}.keras")

    logger.info(f"Modelo para o produto {produto_id} salvo em {model_path}.keras.")

if __name__ == "__main__":
    train_model()