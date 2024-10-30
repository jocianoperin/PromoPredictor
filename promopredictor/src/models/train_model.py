# src/models/train_model.py

import pandas as pd
import os
import numpy as np
import joblib
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Input
from tensorflow.keras.callbacks import EarlyStopping
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

    # Criar sequências
    n_steps = 30
    X_train_seq, y_train_un_seq = create_sequences(X_train, y_train_un, n_steps)
    X_test_seq, y_test_un_seq = create_sequences(X_test, y_test_un, n_steps)
    _, y_train_valor_seq = create_sequences(X_train, y_train_valor, n_steps)
    _, y_test_valor_seq = create_sequences(X_test, y_test_valor, n_steps)

    # Definir o modelo para Quantidade de Unidades Vendidas
    model_un = Sequential()
    model_un.add(Input(shape=(n_steps, X_train_seq.shape[2])))
    model_un.add(LSTM(50, activation='tanh'))
    model_un.add(Dense(1))
    model_un.compile(optimizer='adam', loss='mse')

    # Treinar o modelo
    early_stopping = EarlyStopping(monitor='val_loss', patience=100)

    model_un.fit(X_train_seq, y_train_un_seq, epochs=200, validation_data=(X_test_seq, y_test_un_seq), callbacks=[early_stopping])

    # Definir o modelo para Valor Unitário
    model_valor = Sequential()
    model_valor.add(Input(shape=(n_steps, X_train_seq.shape[2])))
    model_valor.add(LSTM(50, activation='tanh'))
    model_valor.add(Dense(1))
    model_valor.compile(optimizer='adam', loss='mse')

    # Treinar o modelo
    model_valor.fit(X_train_seq, y_train_valor_seq, epochs=200, validation_data=(X_test_seq, y_test_valor_seq), callbacks=[early_stopping])

    # Salvar os modelos e o scaler
    script_dir = os.path.dirname(os.path.abspath(__file__))
    models_dir = os.path.join(script_dir, '..', '..', 'trained_models')
    models_dir = os.path.abspath(models_dir)
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
    df['ValorUnitario'] = df['ValorTotal'] / df['QuantidadeLiquida'].replace(0, np.nan)
    df['ValorUnitario'].fillna(0, inplace=True)

    # Selecionar as features e targets
    features = ['EmPromocao', 'DiaDaSemana', 'Mes', 'Dia', 'Feriado']
    X = df[features]
    y_un = df['QuantidadeLiquida'].values
    y_valor = df['ValorUnitario'].values

    # Verificar valores inválidos
    if np.any(np.isnan(X)) or np.any(np.isinf(X)):
        logger.error('Dados de entrada contêm valores NaN ou Inf.')
        X = np.nan_to_num(X, nan=0, posinf=0, neginf=0)
    if np.any(np.isnan(y_un)) or np.any(np.isinf(y_un)):
        logger.error('Variável alvo y_un contém valores NaN ou Inf.')
        y_un = np.nan_to_num(y_un, nan=0, posinf=0, neginf=0)
    if np.any(np.isnan(y_valor)) or np.any(np.isinf(y_valor)):
        logger.error('Variável alvo y_valor contém valores NaN ou Inf.')
        y_valor = np.nan_to_num(y_valor, nan=0, posinf=0, neginf=0)

    # Escalar os dados
    if scaler is None:
        scaler = MinMaxScaler()
        X_scaled = scaler.fit_transform(X)
    else:
        X_scaled = scaler.transform(X)

    return X_scaled, y_un, y_valor, scaler

def load_data(produto_especifico):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, '..', '..', 'data', 'dados_processados.csv')
    data_path = os.path.abspath(data_path)
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
