import pandas as pd
import os
import numpy as np
import joblib
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelEncoder
from tensorflow.keras.models import Sequential # type: ignore
from tensorflow.keras.layers import LSTM, Dense, Input, Dropout # type: ignore
from tensorflow.keras.callbacks import EarlyStopping # type: ignore
from tensorflow.keras.optimizers import Adam # type: ignore
from src.utils.logging_config import get_logger
import matplotlib.pyplot as plt
import keras_tuner as kt  # Importando o Keras Tuner
# Adicionamos o import do TensorFlow para configurar a GPU
import tensorflow as tf

logger = get_logger(__name__)

# ===========================
# Hiperparâmetros e Configurações
# ===========================

# Tamanho da sequência de entrada (número de passos de tempo)
N_STEPS = 30  # Ajuste conforme necessário

# Número máximo de épocas para treinamento
EPOCHS = 150

# Paciência para o EarlyStopping
PATIENCE = 10

# Tamanho do batch
BATCH_SIZE = 64  # Ajuste conforme necessário

# Caminhos dos diretórios
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(SCRIPT_DIR)))
MODELS_DIR = os.path.join(BASE_DIR, 'promopredictor', 'trained_models')
DATA_DIR = os.path.join(BASE_DIR, 'promopredictor', 'data')
os.makedirs(MODELS_DIR, exist_ok=True)
PLOTS_DIR = os.path.join(BASE_DIR, 'promopredictor', 'plots')
os.makedirs(PLOTS_DIR, exist_ok=True)

# ===========================
# Configurações de GPU
# ===========================

def adjust_gpu_memory():
    """
    Ajusta o uso de memória da GPU para otimizar o treinamento.
    """
    gpus = tf.config.list_physical_devices('GPU')
    if gpus:
        try:
            # Definir limite de memória manualmente
            for gpu in gpus:
                tf.config.experimental.set_virtual_device_configuration(
                    gpu,
                    [tf.config.experimental.VirtualDeviceConfiguration(memory_limit=4000)]
                )
            logical_gpus = tf.config.experimental.list_logical_devices('GPU')
            logger.info(f"{len(gpus)} GPUs físicas, {len(logical_gpus)} GPUs lógicas configuradas.")
        except RuntimeError as e:
            logger.error(f"Erro ao configurar a GPU: {e}")

# ===========================
# Classe Personalizada para LabelEncoder com Tratamento de Rótulos Desconhecidos
# ===========================

class LabelEncoderSafe(LabelEncoder):
    def fit(self, y):
        super().fit(y)
        self.label_to_index_ = {label: idx for idx, label in enumerate(self.classes_)}
        return self

    def transform(self, y):
        y_array = np.array(y)
        transformed = []
        for label in y_array:
            if label in self.label_to_index_:
                transformed.append(self.label_to_index_[label])
            else:
                transformed.append(-1)  # Valor para rótulos desconhecidos
        return np.array(transformed)

# ===========================
# Funções Principais
# ===========================

def train_and_evaluate_models(produto_especifico):
    logger.info(f"Iniciando treinamento para o produto {produto_especifico}")

    # Treinamento do modelo de preço unitário
    df_transacao = load_transaction_data(produto_especifico)
    if df_transacao.empty:
        logger.error(f'Não há dados de transação para o produto {produto_especifico}.')
        return
    train_unit_price_model(df_transacao, produto_especifico)

    # Treinamento do modelo de quantidade vendida
    df_diario = load_daily_data(produto_especifico)
    if df_diario.empty:
        logger.error(f'Não há dados diários para o produto {produto_especifico}.')
        return
    train_quantity_model(df_diario, produto_especifico)

# ===========================
# Funções Auxiliares para o Modelo de Preço Unitário
# ===========================

def train_unit_price_model(df, produto_especifico):
    logger.info(f"Treinando modelo de preço unitário para o produto {produto_especifico}")

    # Convert 'DataHora' to datetime and sort
    df['DataHora'] = pd.to_datetime(df['DataHora'])
    df = df.sort_values('DataHora')

    # Dividir em treinamento e teste
    df_train = df[df['DataHora'] < '2023-01-01'].copy()
    df_test = df[df['DataHora'] >= '2023-01-01'].copy()

    if df_train.empty or df_test.empty:
        logger.error('Dados insuficientes para treinamento ou teste do modelo de preço unitário.')
        return

    # Preparar os dados
    X_train, y_train, scaler_X, scaler_y, label_encoders = prepare_unit_price_data(df_train)
    X_test, y_test, _, _, _ = prepare_unit_price_data(df_test, scaler_X, scaler_y, label_encoders)

    if X_train is None or X_test is None:
        logger.error('Erro na preparação dos dados para o modelo de preço unitário.')
        return

    # Criar sequências
    X_train_seq, y_train_seq = create_sequences(X_train, y_train, N_STEPS)
    X_test_seq, y_test_seq = create_sequences(X_test, y_test, N_STEPS)

    # Verificar se há dados suficientes após a criação das sequências
    if X_train_seq.size == 0 or X_test_seq.size == 0:
        logger.error('Dados insuficientes após a criação das sequências para o modelo de preço unitário.')
        return

    # Definir o modelo usando Keras Tuner
    def build_unit_price_model(hp):

        model = Sequential()
        model.add(Input(shape=(N_STEPS, X_train_seq.shape[2])))

        # Tuning do número de unidades nas camadas LSTM
        lstm_units = hp.Int('lstm_units', min_value=32, max_value=256, step=32)
        model.add(LSTM(units=lstm_units, activation='tanh', return_sequences=True))
        model.add(LSTM(units=lstm_units, activation='tanh'))

        # Tuning da taxa de dropout
        model.add(Dropout(hp.Float('dropout_rate', min_value=0.1, max_value=0.5, step=0.1)))
        model.add(Dense(1))

        # Tuning da taxa de aprendizado
        learning_rate = hp.Float('learning_rate', min_value=1e-4, max_value=1e-2, sampling='log')
        optimizer = Adam(learning_rate=learning_rate)
        model.compile(optimizer=optimizer, loss='mean_squared_error')

        return model

    # Configurar o tuner
    tuner = kt.RandomSearch(
        build_unit_price_model,
        objective='val_loss',
        max_trials=100,  # Ajuste conforme necessário
        executions_per_trial=1,
        directory='kt_tuner',
        project_name=f'unit_price_model_{produto_especifico}'
    )

    # Early stopping callback
    early_stopping = EarlyStopping(monitor='val_loss', patience=PATIENCE, restore_best_weights=True)

    # Realizar a busca de hiperparâmetros
    tuner.search(
        X_train_seq, y_train_seq,
        epochs=EPOCHS,
        validation_data=(X_test_seq, y_test_seq),
        callbacks=[early_stopping],
        verbose=1
    )

    # Obter o melhor modelo
    best_model = tuner.get_best_models(num_models=1)[0]

    # Não é possível obter o histórico diretamente ao usar o Keras Tuner
    # Portanto, não plotaremos o histórico de treinamento neste caso

    # Salvar o scaler das features
    scaler_X_path = os.path.join(MODELS_DIR, f'scaler_X_unit_price_{produto_especifico}.pkl')
    joblib.dump(scaler_X, scaler_X_path)

    # Salvar o scaler da variável alvo
    scaler_y_path = os.path.join(MODELS_DIR, f'scaler_y_unit_price_{produto_especifico}.pkl')
    joblib.dump(scaler_y, scaler_y_path)

    # Salvar os LabelEncoders
    encoders_path = os.path.join(MODELS_DIR, f'label_encoders_unit_price_{produto_especifico}.pkl')
    joblib.dump(label_encoders, encoders_path)

    # Salvar o modelo
    model_path = os.path.join(MODELS_DIR, f'model_unit_price_{produto_especifico}.h5')
    best_model.save(model_path)

    logger.info(f'Modelo de preço unitário para o produto {produto_especifico} salvo com sucesso.')

def prepare_unit_price_data(df, scaler_X=None, scaler_y=None, label_encoders=None):
    # Definir as features e a variável alvo
    features = [
        'Quantidade', 'Desconto', 'Acrescimo', 'DescontoGeral', 'AcrescimoGeral',
        'EmPromocao', 'DiaDaSemana', 'Mes', 'Dia', 'Feriado', 'VesperaDeFeriado',
        'CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo', 'CodigoFabricante',
        'CodigoFornecedor', 'CodigoKitPrincipal', 'ValorCusto'
    ]
    target = 'ValorUnitario'

    missing_features = [col for col in features if col not in df.columns]
    if missing_features:
        logger.error(f"As seguintes features estão faltando no modelo de preço unitário: {missing_features}")
        return None, None, None, None, None

    # Preencher NaNs nas features
    df[features] = df[features].fillna(0)

    # Preencher NaNs no target
    df[target] = df[target].fillna(0)

    # Codificar variáveis categóricas
    categorical_cols = ['CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo', 'CodigoFabricante',
                        'CodigoFornecedor', 'CodigoKitPrincipal']

    if label_encoders is None:
        label_encoders = {}
        for col in categorical_cols:
            le = LabelEncoderSafe()
            df[col] = df[col].astype(str)
            le.fit(df[col])
            df[col] = le.transform(df[col])
            label_encoders[col] = le
    else:
        for col in categorical_cols:
            le = label_encoders[col]
            df[col] = df[col].astype(str)
            df[col] = le.transform(df[col])

    # Criar um DataFrame com features e target
    data = df[features + [target]].copy()

    # Tratar NaNs restantes
    data.dropna(inplace=True)
    data.reset_index(drop=True, inplace=True)

    # Separar features e target
    X = data[features]
    y = data[target].values

    # Escalar os dados
    if scaler_X is None:
        scaler_X = StandardScaler()
        X_scaled = scaler_X.fit_transform(X)
    else:
        X_scaled = scaler_X.transform(X)

    # Escalar a variável alvo
    if scaler_y is None:
        scaler_y = StandardScaler()
        y_scaled = scaler_y.fit_transform(y.reshape(-1, 1)).flatten()
    else:
        y_scaled = scaler_y.transform(y.reshape(-1, 1)).flatten()

    return X_scaled, y_scaled, scaler_X, scaler_y, label_encoders

# ===========================
# Funções Auxiliares para o Modelo de Quantidade Vendida
# ===========================

def train_quantity_model(df, produto_especifico):
    logger.info(f"Treinando modelo de quantidade vendida para o produto {produto_especifico}")

    # Convert 'Data' to datetime and sort
    df['Data'] = pd.to_datetime(df['Data'])
    df = df.sort_values('Data')

    # Criar features lag
    df = create_lag_features(df, N_STEPS)

    # Dividir em treinamento e teste
    df_train = df[df['Data'] < '2023-01-01'].copy()
    df_test = df[df['Data'] >= '2023-01-01'].copy()

    if df_train.empty or df_test.empty:
        logger.error('Dados insuficientes para treinamento ou teste do modelo de quantidade vendida.')
        return

    # Preparar os dados
    X_train, y_train, scaler_X, scaler_y, label_encoders = prepare_quantity_data(df_train)
    X_test, y_test, _, _, _ = prepare_quantity_data(df_test, scaler_X, scaler_y, label_encoders)

    if X_train is None or X_test is None:
        logger.error('Erro na preparação dos dados para o modelo de quantidade vendida.')
        return

    # Criar sequências
    X_train_seq, y_train_seq = create_sequences(X_train, y_train, N_STEPS)
    X_test_seq, y_test_seq = create_sequences(X_test, y_test, N_STEPS)

    # Verificar se há dados suficientes após a criação das sequências
    if X_train_seq.size == 0 or X_test_seq.size == 0:
        logger.error('Dados insuficientes após a criação das sequências para o modelo de quantidade vendida.')
        return

    # Definir o modelo usando Keras Tuner
    def build_quantity_model(hp):

        model = Sequential()
        model.add(Input(shape=(N_STEPS, X_train_seq.shape[2])))

        # Tuning do número de unidades nas camadas LSTM
        lstm_units = hp.Int('lstm_units', min_value=32, max_value=256, step=32)
        model.add(LSTM(units=lstm_units, activation='tanh', return_sequences=True))
        model.add(LSTM(units=lstm_units, activation='tanh'))

        # Tuning da taxa de dropout
        model.add(Dropout(hp.Float('dropout_rate', min_value=0.1, max_value=0.5, step=0.1)))
        model.add(Dense(1))

        # Tuning da taxa de aprendizado
        learning_rate = hp.Float('learning_rate', min_value=1e-4, max_value=1e-2, sampling='log')
        optimizer = Adam(learning_rate=learning_rate)
        model.compile(optimizer=optimizer, loss='mean_squared_error')

        return model

    # Configurar o tuner
    tuner = kt.RandomSearch(
        build_quantity_model,
        objective='val_loss',
        max_trials=100,  # Ajuste conforme necessário
        executions_per_trial=1,
        directory='kt_tuner',
        project_name=f'quantity_model_{produto_especifico}'
    )

    # Early stopping callback
    early_stopping = EarlyStopping(monitor='val_loss', patience=PATIENCE, restore_best_weights=True)

    # Realizar a busca de hiperparâmetros
    tuner.search(
        X_train_seq, y_train_seq,
        epochs=EPOCHS,
        validation_data=(X_test_seq, y_test_seq),
        callbacks=[early_stopping],
        verbose=1
    )

    # Obter o melhor modelo
    best_model = tuner.get_best_models(num_models=1)[0]

    # Não é possível obter o histórico diretamente ao usar o Keras Tuner
    # Portanto, não plotaremos o histórico de treinamento neste caso

    # Salvar o scaler das features
    scaler_X_path = os.path.join(MODELS_DIR, f'scaler_X_quantity_{produto_especifico}.pkl')
    joblib.dump(scaler_X, scaler_X_path)

    # Salvar o scaler da variável alvo
    scaler_y_path = os.path.join(MODELS_DIR, f'scaler_y_quantity_{produto_especifico}.pkl')
    joblib.dump(scaler_y, scaler_y_path)

    # Salvar os LabelEncoders
    encoders_path = os.path.join(MODELS_DIR, f'label_encoders_quantity_{produto_especifico}.pkl')
    joblib.dump(label_encoders, encoders_path)

    # Salvar o modelo
    model_path = os.path.join(MODELS_DIR, f'model_quantity_{produto_especifico}.h5')
    best_model.save(model_path)

    logger.info(f'Modelo de quantidade vendida para o produto {produto_especifico} salvo com sucesso.')

def prepare_quantity_data(df, scaler_X=None, scaler_y=None, label_encoders=None):
    # Definir as features e a variável alvo
    features = [
        'EmPromocao', 'DiaDaSemana', 'Mes', 'Dia', 'Feriado', 'VesperaDeFeriado',
        'Rentabilidade', 'DescontoAplicado', 'AcrescimoAplicado',
        'QuantidadeLiquida_Lag1',  # Features lag
        'QuantidadeLiquida_Lag2',
        'QuantidadeLiquida_Lag3',
        'CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo', 'CodigoFabricante',
        'CodigoFornecedor', 'CodigoKitPrincipal'
    ]
    target = 'QuantidadeLiquida'

    missing_features = [col for col in features if col not in df.columns]
    if missing_features:
        logger.error(f"As seguintes features estão faltando no modelo de quantidade vendida: {missing_features}")
        return None, None, None, None, None

    # Preencher NaNs nas features
    df[features] = df[features].fillna(0)

    # Preencher NaNs no target
    df[target] = df[target].fillna(0)

    # Codificar variáveis categóricas
    categorical_cols = ['CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo', 'CodigoFabricante',
                        'CodigoFornecedor', 'CodigoKitPrincipal']

    if label_encoders is None:
        label_encoders = {}
        for col in categorical_cols:
            le = LabelEncoderSafe()
            df[col] = df[col].astype(str)
            le.fit(df[col])
            df[col] = le.transform(df[col])
            label_encoders[col] = le
    else:
        for col in categorical_cols:
            le = label_encoders[col]
            df[col] = df[col].astype(str)
            df[col] = le.transform(df[col])

    # Criar um DataFrame com features e target
    data = df[features + [target]].copy()

    # Tratar NaNs restantes
    data.dropna(inplace=True)
    data.reset_index(drop=True, inplace=True)

    # Separar features e target
    X = data[features]
    y = data[target].values

    # Escalar os dados
    if scaler_X is None:
        scaler_X = StandardScaler()
        X_scaled = scaler_X.fit_transform(X)
    else:
        X_scaled = scaler_X.transform(X)

    # Escalar a variável alvo
    if scaler_y is None:
        scaler_y = StandardScaler()
        y_scaled = scaler_y.fit_transform(y.reshape(-1, 1)).flatten()
    else:
        y_scaled = scaler_y.transform(y.reshape(-1, 1)).flatten()

    return X_scaled, y_scaled, scaler_X, scaler_y, label_encoders

# ===========================
# Funções Auxiliares Comuns
# ===========================

def load_transaction_data(produto_especifico):
    """
    Carrega os dados de transação para o produto específico.
    """
    data_path = os.path.join(DATA_DIR, f'dados_transacao_{produto_especifico}.csv')
    data_path = os.path.abspath(data_path)
    logger.info(f"Carregando dados de transação de: {data_path}")

    if not os.path.exists(data_path):
        logger.error(f"Arquivo de dados de transação não encontrado para o produto {produto_especifico}.")
        return pd.DataFrame()

    df = pd.read_csv(data_path, parse_dates=['Data', 'DataHora'])
    return df

def load_daily_data(produto_especifico):
    """
    Carrega os dados diários processados para o produto específico.
    """
    data_path = os.path.join(DATA_DIR, f'dados_agrupados_{produto_especifico}.csv')
    data_path = os.path.abspath(data_path)
    logger.info(f"Carregando dados diários de: {data_path}")

    if not os.path.exists(data_path):
        logger.error(f"Arquivo de dados diários não encontrado para o produto {produto_especifico}.")
        return pd.DataFrame()

    df = pd.read_csv(data_path, parse_dates=['Data'])
    return df

def create_sequences(X, y, n_steps):
    """
    Cria sequências de dados para entrada no modelo LSTM.
    """
    Xs, ys = [], []
    for i in range(len(X) - n_steps):
        Xs.append(X[i:(i + n_steps)])
        ys.append(y[i + n_steps])
    return np.array(Xs), np.array(ys)

def create_lag_features(df, n_lags):
    """
    Cria features lag da variável alvo.
    """
    for lag in range(1, 4):  # Criar 3 lags
        df[f'QuantidadeLiquida_Lag{lag}'] = df['QuantidadeLiquida'].shift(lag)
    df.dropna(inplace=True)
    return df

def plot_training_history(history, produto_especifico, target_name):
    """
    Plota e salva as curvas de perda de treinamento e validação.
    """
    plt.figure(figsize=(10, 6))
    plt.plot(history.history['loss'], label='Perda de Treinamento')
    plt.plot(history.history['val_loss'], label='Perda de Validação')
    plt.title(f'Histórico de Treinamento - Produto {produto_especifico} - {target_name}')
    plt.xlabel('Épocas')
    plt.ylabel('Perda')
    plt.legend()
    plt.grid(True)
    plot_path = os.path.join(PLOTS_DIR, f'training_history_{produto_especifico}_{target_name}.png')
    plt.savefig(plot_path)
    plt.close()
    logger.info(f'Gráfico de histórico de treinamento salvo em {plot_path}')


# ===========================
# Execução Principal
# ===========================

if __name__ == "__main__":
    # Lista de produtos a serem processados
    produtos_especificos = [26173]  # Substitua pelos códigos dos produtos desejados
    for produto in produtos_especificos:
        train_and_evaluate_models(produto)

