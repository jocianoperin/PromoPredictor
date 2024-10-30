# src/models/train_model.py

import pandas as pd
import os
import numpy as np
import joblib
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Input
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.optimizers import RMSprop
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def train_and_evaluate_models(produto_especifico):

    df = load_data(produto_especifico)
    if df.empty:
        logger.error(f'Não há dados para o produto {produto_especifico}.')
        return

    # Filtrar dados de 2019 a 2023
    df['Data'] = pd.to_datetime(df['Data'])
    df = df.sort_values('Data')
    df_train = df[(df['Data'] >= '2019-01-01') & (df['Data'] <= '2022-12-31')].copy()
    df_test = df[(df['Data'] >= '2023-01-01') & (df['Data'] <= '2023-12-31')].copy()

    if df_train.empty or df_test.empty:
        logger.error('Dados insuficientes para treinamento ou teste.')
        return

    # Preparar os dados
    X_train, y_train_un, y_train_valor, scaler = prepare_data(df_train)
    X_test, y_test_un, y_test_valor, _ = prepare_data(df_test, scaler)

    if X_train is None or X_test is None:
        logger.error('Erro na preparação dos dados.')
        return

    # Criar sequências
    n_steps = 180
    X_train_seq, y_train_un_seq = create_sequences(X_train, y_train_un, n_steps)
    X_test_seq, y_test_un_seq = create_sequences(X_test, y_test_un, n_steps)
    _, y_train_valor_seq = create_sequences(X_train, y_train_valor, n_steps)
    _, y_test_valor_seq = create_sequences(X_test, y_test_valor, n_steps)

    # Verificar se há dados suficientes após a criação das sequências
    if X_train_seq.size == 0 or X_test_seq.size == 0:
        logger.error('Dados insuficientes após a criação das sequências.')
        return

    # Definir o modelo para Quantidade de Unidades Vendidas
    model_un = Sequential()
    model_un.add(Input(shape=(n_steps, X_train_seq.shape[2])))
    model_un.add(LSTM(100, activation='tanh'))
    model_un.add(Dense(1))
    optimizer_un = RMSprop(learning_rate=0.01)
    model_un.compile(optimizer=optimizer_un, loss='mse')

    # Treinar o modelo
    early_stopping = EarlyStopping(monitor='val_loss', patience=10)
    model_un.fit(X_train_seq, y_train_un_seq, epochs=200, validation_data=(X_test_seq, y_test_un_seq), callbacks=[early_stopping])

    # Definir o modelo para Valor Unitário
    model_valor = Sequential()
    model_valor.add(Input(shape=(n_steps, X_train_seq.shape[2])))
    model_valor.add(LSTM(100, activation='tanh'))
    model_valor.add(Dense(1))
    optimizer_valor = RMSprop(learning_rate=0.01)
    model_valor.compile(optimizer=optimizer_valor, loss='mse')

    # Treinar o modelo
    model_valor.fit(X_train_seq, y_train_valor_seq, epochs=200, validation_data=(X_test_seq, y_test_valor_seq), callbacks=[early_stopping])

    # Salvar os modelos e o scaler
    script_dir = os.path.dirname(os.path.abspath(__file__))
    models_dir = os.path.join(script_dir, '..', '..', 'trained_models')
    os.makedirs(models_dir, exist_ok=True)

    # Salvar o scaler
    scaler_path = os.path.join(models_dir, f'scaler_{produto_especifico}.pkl')
    joblib.dump(scaler, scaler_path)

    # Salvar os modelos
    model_un_path = os.path.join(models_dir, f'model_un_{produto_especifico}.h5')
    model_un.save(model_un_path)
    model_valor_path = os.path.join(models_dir, f'model_valor_{produto_especifico}.h5')
    model_valor.save(model_valor_path)

    logger.info(f'Modelos para o produto {produto_especifico} salvos com sucesso.')

def prepare_data(df, scaler=None):
    # Features
    df['DiaDaSemana'] = df['Data'].dt.dayofweek
    df['Mes'] = df['Data'].dt.month
    df['Dia'] = df['Data'].dt.day

    # Tratar divisão por zero
    df['QuantidadeLiquida'].replace(0, np.nan, inplace=True)
    df.dropna(subset=['QuantidadeLiquida'], inplace=True)
    df['ValorUnitario'] = df['ValorTotal'] / df['QuantidadeLiquida']
    df['ValorUnitario'] = df['ValorUnitario'].fillna(0)

    # Preencher NaNs nas features
    df['EmPromocao'] = df['EmPromocao'].fillna(0)
    df['Feriado'] = df['Feriado'].fillna(0)
    df['VésperaDeFeriado'] = df['VésperaDeFeriado'].fillna(0)

    # Selecionar as features e targets, incluindo 'Feriado' e 'VésperaDeFeriado'
    features = ['EmPromocao', 'DiaDaSemana', 'Mes', 'Dia', 'Feriado', 'VésperaDeFeriado']
    target_un = 'QuantidadeLiquida'
    target_valor = 'ValorUnitario'

    # Verificar se as colunas existem
    missing_features = [col for col in features if col not in df.columns]
    if missing_features:
        logger.error(f"As seguintes features estão faltando: {missing_features}")
        return None, None, None, None

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
    if scaler is None:
        scaler = MinMaxScaler()
        X_scaled = scaler.fit_transform(X)
    else:
        X_scaled = scaler.transform(X)

    return X_scaled, y_un, y_valor, scaler

def load_data(produto_especifico):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
    data_path = os.path.join(base_dir, 'promopredictor', 'data', 'dados_processados.csv')
    data_path = os.path.abspath(data_path)
    logger.info(f"Carregando dados de: {data_path}")
    df = pd.read_csv(data_path, parse_dates=['Data'])
    df_produto = df[df['CodigoProduto'] == produto_especifico]
    return df_produto

def create_sequences(X, y, n_steps):
    Xs, ys = [], []
    for i in range(len(X) - n_steps):
        Xs.append(X[i:(i + n_steps)])
        ys.append(y[i + n_steps])
    return np.array(Xs), np.array(ys)

if __name__ == "__main__":
    produto_especifico = 26173  # Substitua pelo código do produto desejado
    train_and_evaluate_models(produto_especifico)
