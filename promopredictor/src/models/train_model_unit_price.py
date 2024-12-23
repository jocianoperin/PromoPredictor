from autokeras import AutoModel, RegressionHead, Input
import tensorflow as tf
import pandas as pd
import numpy as np
from pathlib import Path
from src.utils.logging_config import get_logger
from src.data_processing.feature_engineering import add_rolling_features

logger = get_logger(__name__)

BASE_DATA_DIR = Path(__file__).parent.parent.parent / "data"

# Ajustar o caminho para armazenar os modelos na pasta correta
MODEL_BASE_DIR = Path(__file__).parent.parent.parent / "models" / "price"

def load_price_data(produto_id, window_size=7):
    """
    Carrega o dataset diário criado para o valor unitário,
    aplica engenharia de recursos e separa treino e validação.

    Args:
        produto_id (int): Código do produto.
        window_size (int): Tamanho da janela deslizante.

    Returns:
        tuple: Dados de treinamento e validação.
    """
    file_path = BASE_DATA_DIR / "cleaned" / f"produto_{produto_id}_price.csv"
    logger.info(f"Lendo dataset de preço de {file_path}.")

    df = pd.read_csv(file_path, parse_dates=['Data'])

    # Adicionar variáveis de defasagem
    for lag in range(1, 4):
        df[f'ValorUnitario_lag{lag}'] = df['ValorUnitarioMedio'].shift(lag)
        df[f'QuantidadeLiquida_lag{lag}'] = df['QuantidadeLiquida'].shift(lag)

    # Adicionar variáveis de janela deslizante
    df = add_rolling_features(
        df,
        columns=['ValorUnitarioMedio', 'QuantidadeLiquida'],
        window_size=window_size
    )

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
        'ValorUnitarioMedio_rolling_mean_7',
        'ValorUnitarioMedio_rolling_std_7',
        'ValorUnitarioMedio_rolling_sum_7',
        'QuantidadeLiquida_rolling_mean_7',
        'QuantidadeLiquida_rolling_std_7',
        'QuantidadeLiquida_rolling_sum_7',
    ]
    if use_log:
        target = 'LogValorUnitarioMedio'
    else:
        target = 'ValorUnitarioMedio'

    X = df[features].fillna(0).to_numpy()
    y = df[target].fillna(0).to_numpy()
    return X, y

def train_model_unit_price(produto_id, window_size=7):
    """
    Treina um modelo AutoKeras para valor unitário (possivelmente em log), incorporando janela flutuante.
    """
    # Configurar o caminho completo para o project_name
    project_dir = MODEL_BASE_DIR / f"produto_{produto_id}_unit_price_model"
    project_dir.mkdir(parents=True, exist_ok=True)

    train_data, val_data = load_price_data(produto_id)
    
    # Adicionar variáveis de janela flutuante antes de preparar as features
    rolling_columns = ['QuantidadeLiquida', 'ValorUnitarioMedio']  # Colunas para calcular rolling features
    train_data = add_rolling_features(train_data, rolling_columns, window_size)
    val_data = add_rolling_features(val_data, rolling_columns, window_size)

    # Se quiser usar log, true
    X_train, y_train = prepare_features_and_target(train_data, use_log=True)
    X_val, y_val = prepare_features_and_target(val_data, use_log=True)

    # Criar o modelo
    logger.info(f"Iniciando o treinamento do modelo de valor unitário para o produto {produto_id}.")
    input_node = Input()
    output_node = RegressionHead()
    model = AutoModel(
        inputs=input_node,
        outputs=output_node,
        max_trials=300,
        overwrite=False,  # <-- força recriar o tuner do zero
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
    logger.info(f"Avaliando o modelo de valor unitário para o produto {produto_id}.")
    eval_results = model.evaluate(X_val, y_val, return_dict=True)
    logger.info(f"Resultados de validação para o produto {produto_id}: {eval_results}")

    # Salvar
    model_path = project_dir / f"produto_{produto_id}_unit_price_model"
    
    # Salvar o modelo treinado
    model.export_model().save(f"{model_path}.keras")
    logger.info(f"Modelo de valor unitário para o produto {produto_id} salvo em {model_path}.keras.")

if __name__ == "__main__":
    train_model_unit_price()
