# src/models/train_model.py

import pandas as pd
import os
import numpy as np
import joblib
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import LabelEncoder
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Input, Dropout
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.optimizers import Adam
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# ===========================
# Hiperparâmetros e Configurações
# ===========================

# Tamanho da sequência de entrada (número de passos de tempo)
N_STEPS = 30  # Reduzido de 90 para 30

# Número de unidades na camada LSTM
LSTM_UNITS = 64  # Reduzido de 200 para 64

# Taxa de aprendizado para os modelos
LEARNING_RATE_UN = 0.001  # Aumentado de 0.0001 para 0.001
LEARNING_RATE_VALOR = 0.001  # Aumentado de 0.0001 para 0.001

# Número máximo de épocas para treinamento
EPOCHS = 150

# Paciência para o EarlyStopping
PATIENCE = 10

# Função de ativação para a camada LSTM
ACTIVATION = 'relu'  # Alterado de 'tanh' para 'relu'

# Otimizadores
OPTIMIZER_UN = Adam(learning_rate=LEARNING_RATE_UN)  # Alterado para Adam
OPTIMIZER_VALOR = Adam(learning_rate=LEARNING_RATE_VALOR)  # Alterado para Adam

# Tamanho do batch
BATCH_SIZE = 64  # Novo parâmetro adicionado

# Caminhos dos diretórios
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(SCRIPT_DIR)))
MODELS_DIR = os.path.join(BASE_DIR, 'trained_models')
DATA_DIR = os.path.join(BASE_DIR, 'promopredictor', 'data')
os.makedirs(MODELS_DIR, exist_ok=True)

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

    df = load_data(produto_especifico)
    if df.empty:
        logger.error(f'Não há dados para o produto {produto_especifico}.')
        return

    # Convert 'Data' to datetime and sort
    df['Data'] = pd.to_datetime(df['Data'])
    df = df.sort_values('Data')

    # Filter data from 2019 to 2023
    df_train = df[(df['Data'] >= '2019-01-01') & (df['Data'] <= '2022-12-31')].copy()
    df_test = df[(df['Data'] >= '2023-01-01') & (df['Data'] <= '2023-12-31')].copy()

    if df_train.empty or df_test.empty:
        logger.error('Dados insuficientes para treinamento ou teste.')
        return

    # Preparar os dados
    X_train, y_train_un, y_train_valor, scaler_X, scaler_y_un, scaler_y_valor, label_encoders = prepare_data(df_train)
    X_test, y_test_un, y_test_valor, _, _, _, _ = prepare_data(df_test, scaler_X, scaler_y_un, scaler_y_valor, label_encoders)

    if X_train is None or X_test is None:
        logger.error('Erro na preparação dos dados.')
        return

    # Criar sequências
    X_train_seq, y_train_un_seq = create_sequences(X_train, y_train_un, N_STEPS)
    X_test_seq, y_test_un_seq = create_sequences(X_test, y_test_un, N_STEPS)
    _, y_train_valor_seq = create_sequences(X_train, y_train_valor, N_STEPS)
    _, y_test_valor_seq = create_sequences(X_test, y_test_valor, N_STEPS)

    # Verificar se há dados suficientes após a criação das sequências
    if X_train_seq.size == 0 or X_test_seq.size == 0:
        logger.error('Dados insuficientes após a criação das sequências.')
        return

    # ===========================
    # Modelo para Quantidade de Unidades Vendidas
    # ===========================
    model_un = Sequential()
    model_un.add(Input(shape=(N_STEPS, X_train_seq.shape[2])))
    model_un.add(LSTM(LSTM_UNITS, activation=ACTIVATION, return_sequences=True))
    model_un.add(LSTM(LSTM_UNITS, activation=ACTIVATION))
    model_un.add(Dropout(0.2))  # Dropout adicionado
    model_un.add(Dense(1))
    model_un.compile(optimizer=OPTIMIZER_UN, loss='mae')  # Função de perda alterada para 'mae'

    # Treinar o modelo
    early_stopping_un = EarlyStopping(monitor='val_loss', patience=PATIENCE, restore_best_weights=True)
    model_un.fit(
        X_train_seq, y_train_un_seq,
        epochs=EPOCHS,
        batch_size=BATCH_SIZE,  # Batch size adicionado
        validation_data=(X_test_seq, y_test_un_seq),
        callbacks=[early_stopping_un],
        verbose=1
    )

    # ===========================
    # Modelo para Valor Unitário
    # ===========================
    model_valor = Sequential()
    model_valor.add(Input(shape=(N_STEPS, X_train_seq.shape[2])))
    model_valor.add(LSTM(LSTM_UNITS, activation=ACTIVATION, return_sequences=True))
    model_valor.add(LSTM(LSTM_UNITS, activation=ACTIVATION))
    model_valor.add(Dropout(0.2))  # Dropout adicionado
    model_valor.add(Dense(1))
    model_valor.compile(optimizer=OPTIMIZER_VALOR, loss='mae')  # Função de perda alterada para 'mae'

    # Treinar o modelo
    early_stopping_valor = EarlyStopping(monitor='val_loss', patience=PATIENCE, restore_best_weights=True)
    model_valor.fit(
        X_train_seq, y_train_valor_seq,
        epochs=EPOCHS,
        batch_size=BATCH_SIZE,  # Batch size adicionado
        validation_data=(X_test_seq, y_test_valor_seq),
        callbacks=[early_stopping_valor],
        verbose=1
    )

    # ===========================
    # Salvar os modelos, scalers e encoders
    # ===========================
    # Salvar o scaler das features
    scaler_X_path = os.path.join(MODELS_DIR, f'scaler_X_{produto_especifico}.pkl')
    joblib.dump(scaler_X, scaler_X_path)

    # Salvar os scalers das variáveis alvo
    scaler_y_un_path = os.path.join(MODELS_DIR, f'scaler_y_un_{produto_especifico}.pkl')
    joblib.dump(scaler_y_un, scaler_y_un_path)
    scaler_y_valor_path = os.path.join(MODELS_DIR, f'scaler_y_valor_{produto_especifico}.pkl')
    joblib.dump(scaler_y_valor, scaler_y_valor_path)

    # Salvar os LabelEncoders
    encoders_path = os.path.join(MODELS_DIR, f'label_encoders_{produto_especifico}.pkl')
    joblib.dump(label_encoders, encoders_path)

    # Salvar os modelos
    model_un_path = os.path.join(MODELS_DIR, f'model_un_{produto_especifico}.h5')
    model_un.save(model_un_path)
    model_valor_path = os.path.join(MODELS_DIR, f'model_valor_{produto_especifico}.h5')
    model_valor.save(model_valor_path)

    logger.info(f'Modelos para o produto {produto_especifico} salvos com sucesso.')

# ===========================
# Funções Auxiliares
# ===========================

def prepare_data(df, scaler_X=None, scaler_y_un=None, scaler_y_valor=None, label_encoders=None):
    """
    Prepara os dados para treinamento e teste.
    """
    # Features já estão presentes no DataFrame após o processamento

    # Verificar se as colunas existem
    features = [
        'EmPromocao', 'DiaDaSemana', 'Mes', 'Dia', 'Feriado', 'VésperaDeFeriado',
        'Rentabilidade', 'DescontoAplicado', 'AcrescimoAplicado',
        'CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo', 'CodigoFabricante',
        'CodigoFornecedor', 'CodigoKitPrincipal'
    ]
    target_un = 'QuantidadeLiquida'
    target_valor = 'ValorUnitario'

    missing_features = [col for col in features if col not in df.columns]
    if missing_features:
        logger.error(f"As seguintes features estão faltando: {missing_features}")
        return None, None, None, None, None, None, None

    # Preencher NaNs nas features
    df[features] = df[features].fillna(0)

    # Preencher NaNs nos targets
    df[target_un] = df[target_un].fillna(0)
    df[target_valor] = df[target_valor].fillna(0)

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

    # Criar um DataFrame com features e targets
    data = df[features + [target_un, target_valor]].copy()

    # Tratar NaNs restantes
    data.dropna(inplace=True)
    data.reset_index(drop=True, inplace=True)

    # Separar features e targets
    X = data[features]
    y_un = data[target_un].values
    y_valor = data[target_valor].values

    # Escalar os dados
    if scaler_X is None:
        scaler_X = MinMaxScaler()
        X_scaled = scaler_X.fit_transform(X)
    else:
        X_scaled = scaler_X.transform(X)

    # Escalar as variáveis alvo
    if scaler_y_un is None:
        scaler_y_un = MinMaxScaler()
        y_un_scaled = scaler_y_un.fit_transform(y_un.reshape(-1, 1)).flatten()
    else:
        y_un_scaled = scaler_y_un.transform(y_un.reshape(-1, 1)).flatten()

    if scaler_y_valor is None:
        scaler_y_valor = MinMaxScaler()
        y_valor_scaled = scaler_y_valor.fit_transform(y_valor.reshape(-1, 1)).flatten()
    else:
        y_valor_scaled = scaler_y_valor.transform(y_valor.reshape(-1, 1)).flatten()

    return X_scaled, y_un_scaled, y_valor_scaled, scaler_X, scaler_y_un, scaler_y_valor, label_encoders

def load_data(produto_especifico):
    """
    Carrega os dados processados para o produto específico.
    """
    data_path = os.path.join(DATA_DIR, f'dados_processados_{produto_especifico}.csv')
    data_path = os.path.abspath(data_path)
    logger.info(f"Carregando dados de: {data_path}")

    if not os.path.exists(data_path):
        logger.error(f"Arquivo de dados não encontrado para o produto {produto_especifico}.")
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

# ===========================
# Execução Principal
# ===========================

if __name__ == "__main__":
    # Lista de produtos a serem processados
    produtos_especificos = [26173, 12345, 67890]  # Substitua pelos códigos dos produtos desejados
    for produto in produtos_especificos:
        train_and_evaluate_models(produto)
