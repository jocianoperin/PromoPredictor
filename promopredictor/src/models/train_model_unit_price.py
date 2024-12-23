from autokeras import AutoModel, RegressionHead, Input
import tensorflow as tf
import pandas as pd
import numpy as np
from pathlib import Path
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

BASE_DATA_DIR = Path(__file__).parent.parent.parent / "data"

# Ajustar o caminho para armazenar os modelos na pasta correta
MODEL_BASE_DIR = Path(__file__).parent.parent.parent / "models" / "price"

def load_price_data():
    """
    Carrega o dataset diário criado para o valor unitário,
    e faz a separação de treino/val (ex: até 2022 = treino; 2023 = val).
    """
    file_path = BASE_DATA_DIR / "cleaned" / "produto_26173_price.csv"  # Nome de saída do pipeline
    logger.info(f"Lendo dataset de preço de {file_path}.")

    df = pd.read_csv(file_path, parse_dates=['Data'])

    # Adicionar variáveis de defasagem
    for lag in range(1, 4):  # Defasagem de 1 a 3 dias
        df[f'ValorUnitario_lag{lag}'] = df['ValorUnitarioMedio'].shift(lag)
        df[f'QuantidadeLiquida_lag{lag}'] = df['QuantidadeLiquida'].shift(lag)

    # Remover valores nulos criados pelas defasagens
    df = df.dropna()

    # Filtra período
    train_data = df[(df['Data'] >= '2019-01-01') & (df['Data'] <= '2022-12-31')]
    val_data = df[(df['Data'] >= '2023-01-01') & (df['Data'] <= '2023-12-31')]

    return train_data, val_data

def prepare_features_and_target(df: pd.DataFrame, use_log: bool = True):
    """
    Prepara X e y para o modelo.
    Se use_log=True, então a coluna alvo é 'LogValorUnitarioMedio', senão 'ValorUnitarioMedio'.
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
    if use_log:
        target = 'LogValorUnitarioMedio'
    else:
        target = 'ValorUnitarioMedio'

    X = df[features].fillna(0).to_numpy()
    y = df[target].fillna(0).to_numpy()
    return X, y

def train_model_unit_price():
    """
    Treina um modelo AutoKeras para valor unitário (possivelmente em log).
    """
    # Configurar o caminho completo para o project_name
    project_dir = MODEL_BASE_DIR / "structured_data_model_unit_price"
    project_dir.mkdir(parents=True, exist_ok=True)

    train_data, val_data = load_price_data()
    
    # Se quiser usar log, true
    X_train, y_train = prepare_features_and_target(train_data, use_log=True)
    X_val, y_val = prepare_features_and_target(val_data, use_log=True)

    # Criar o modelo
    logger.info("Iniciando o treinamento do modelo de valor unitário.")
    input_node = Input()
    output_node = RegressionHead()
    model = AutoModel(
        inputs=input_node,
        outputs=output_node,
        max_trials=300,
        overwrite=True,  # <-- força recriar o tuner do zero
        project_name=str(project_dir)  # <-- nome distinto
    )

    model.fit(
        X_train, y_train,
        validation_data=(X_val, y_val),
        epochs=200,
        batch_size=32,
        callbacks=[
            tf.keras.callbacks.EarlyStopping(
                monitor="val_loss",
                patience=30,
                restore_best_weights=True
            )
        ]
    )

    # Avaliar
    logger.info("Avaliando o modelo de valor unitário.")
    eval_results = model.evaluate(X_val, y_val, return_dict=True)
    logger.info(f"Resultados de validação (valor unitário, em log): {eval_results}")

    # Salvar
    model_path = project_dir / "structured_data_model_unit_price"
    
    # Salvar o modelo treinado
    model.export_model().save(f"{model_path}.keras")
    logger.info(f"Modelo de valor unitário salvo em {model_path}.keras")

if __name__ == "__main__":
    train_model_unit_price()
