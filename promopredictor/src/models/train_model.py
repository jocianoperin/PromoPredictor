# src/models/train_model.py

import pandas as pd
import os
import numpy as np
import joblib
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, InputLayer
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.optimizers import RMSprop
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# ===========================
# Configurações de Ponderação
# ===========================

# Habilitar ou desabilitar a aplicação de pesos às features
USE_FEATURE_WEIGHTS = False

# Definir os pesos para cada feature. Ajuste os valores conforme necessário.
# Um peso maior aumenta a importância da feature durante o treinamento.
FEATURE_WEIGHTS = {
    'EmPromocao': 2.0,      # Dobrar a importância de 'EmPromocao'
    'Feriado': 1.5,         # Aumentar a importância de 'Feriado'
    # Adicione mais features e seus respectivos pesos se necessário
}

# ===================================
# Hiperparâmetros e Arquitetura da Rede
# ===================================

# Hiperparâmetros: porque todo modelo precisa de uma pitada de magia (e tentativa e erro).
n_steps = 360  # Número de passos na sequência: quantos dias o modelo quer "relembrar" para prever o futuro.
epochs = 150  # Épocas: quantas vezes o modelo vai fazer hora extra na academia de dados.
patience = 10  # EarlyStopping paciência: quanto mais alto, mais paciência o modelo tem pra continuar tentando. Zen mode ativado.

# Arquitetura da rede: a quantidade de neurônios em cada camada
neurons_un = 500  # Camada LSTM para quantidade de unidades: 500 neurônios - porque quem não quer um pouco de "força bruta" num modelo?
neurons_valor = 500  # Camada LSTM para valor unitário: 500 neurônios - esses caras vão quebrar a cabeça pensando nos preços.
learning_rate_un = 0.0001  # Taxa de aprendizado para o modelo de quantidade - (0.0001) aprende devagar para não surtar nos números.
learning_rate_valor = 0.01  # Taxa de aprendizado para o modelo de valor - (0.01) um pouco mais ousado na hora de aprender.

def train_and_evaluate_models(produto_especifico):
    df = load_data(produto_especifico)
    if df.empty:
        logger.error(f'Não há dados para o produto {produto_especifico}.')
        return

    # Preparar os dados com ou sem pesos nas features
    if USE_FEATURE_WEIGHTS:
        X_train, y_train_un, y_train_valor, scaler = prepare_data(df, feature_weights=FEATURE_WEIGHTS)
        # Dividir os dados em treino e teste após a agregação
        df_train = X_train[df['Data'] <= '2022-12-31'].copy()
        df_test = X_train[df['Data'] >= '2023-01-01'].copy()
        y_train_un_seq = y_train_un[df['Data'] <= '2022-12-31']
        y_test_un_seq = y_train_un[df['Data'] >= '2023-01-01']
        y_train_valor_seq = y_train_valor[df['Data'] <= '2022-12-31']
        y_test_valor_seq = y_train_valor[df['Data'] >= '2023-01-01']
    else:
        # Dividir os dados em treino e teste antes da preparação
        df_train = df[(df['Data'] >= '2019-01-01') & (df['Data'] <= '2022-12-31')].copy()
        df_test = df[(df['Data'] >= '2023-01-01') & (df['Data'] <= '2023-12-31')].copy()

        X_train, y_train_un, y_train_valor, scaler = prepare_data(df_train)
        X_test, y_test_un, y_test_valor, _ = prepare_data(df_test, scaler)

    if X_train is None or (not USE_FEATURE_WEIGHTS and X_test is None):
        logger.error('Erro na preparação dos dados.')
        return

    # Criar sequências
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
    model_un.add(InputLayer(input_shape=(n_steps, X_train_seq.shape[2])))
    model_un.add(LSTM(neurons_un, activation='tanh'))
    model_un.add(Dense(1))
    optimizer_un = RMSprop(learning_rate=learning_rate_un)
    model_un.compile(optimizer=optimizer_un, loss='mse')

    # EarlyStopping: é tipo um amigo sensato que diz "chega de teimar, vamos dar um tempo, nada está melhorando..."
    # Um lembrete de que às vezes na vida, insistir não é o caminho. Toda vez que eu ler isso, vou lembrar de parar de forçar a barra 😆
    early_stopping = EarlyStopping(monitor='val_loss', patience=patience)

    # Treinar o modelo
    model_un.fit(
        X_train_seq,
        y_train_un_seq,
        epochs=epochs,
        validation_data=(X_test_seq, y_test_un_seq),
        callbacks=[early_stopping]
    )

    # Definir o modelo para Valor Unitário
    model_valor = Sequential()
    model_valor.add(InputLayer(input_shape=(n_steps, X_train_seq.shape[2])))
    model_valor.add(LSTM(neurons_valor, activation='tanh'))
    model_valor.add(Dense(1))
    optimizer_valor = RMSprop(learning_rate=learning_rate_valor)
    model_valor.compile(optimizer=optimizer_valor, loss='mse')

    # Treinar o modelo
    model_valor.fit(
        X_train_seq,
        y_train_valor_seq,
        epochs=epochs,
        validation_data=(X_test_seq, y_test_valor_seq),
        callbacks=[early_stopping]
    )

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

def prepare_data(df, scaler=None, feature_weights=None):
    # Features temporais
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

    # Aplicar pesos às features, se fornecidos
    if feature_weights:
        for feature, weight in feature_weights.items():
            if feature in X.columns:
                X[feature] = X[feature] * weight
                logger.debug(f"Aplicado peso {weight} à feature '{feature}'.")

    # Escalar os dados
    if scaler is None:
        scaler = MinMaxScaler()
        X_scaled = scaler.fit_transform(X)
        logger.debug("Aplicado MinMaxScaler com fit.")
    else:
        X_scaled = scaler.transform(X)
        logger.debug("Aplicado MinMaxScaler com transform.")

    return X_scaled, y_un, y_valor, scaler

def load_data(produto_especifico):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
    data_path = os.path.join(base_dir, 'promopredictor', 'data', 'dados_processados.csv')
    data_path = os.path.abspath(data_path)
    logger.info(f"Carregando dados de: {data_path}")
    df = pd.read_csv(data_path, parse_dates=['Data'])

    # Filtrar pelo produto específico
    df_produto = df[df['CodigoProduto'] == produto_especifico]

    if df_produto.empty:
        logger.error(f'Não há dados para o produto {produto_especifico}.')
        return pd.DataFrame()

    # Agregar por dia
    df_diario = df_produto.groupby('Data').agg({
        'QuantidadeLiquida': 'sum',      # Soma das quantidades líquidas vendidas no dia
        'EmPromocao': 'max',             # Se o produto esteve em promoção em algum momento do dia
        'Feriado': 'max',                # Se o dia foi feriado
        'VésperaDeFeriado': 'max',       # Se o dia foi véspera de feriado
        # Adicione outras agregações conforme necessário
    }).reset_index()

    # Preencher datas faltantes (dias sem vendas) com zeros ou valores apropriados
    all_dates = pd.date_range(start=df_diario['Data'].min(), end=df_diario['Data'].max(), freq='D')
    df_diario = df_diario.set_index('Data').reindex(all_dates).fillna({
        'QuantidadeLiquida': 0,
        'EmPromocao': 0,
        'Feriado': 0,
        'VésperaDeFeriado': 0
    }).rename_axis('Data').reset_index()

    # Adicionar features temporais
    df_diario['DiaDaSemana'] = df_diario['Data'].dt.dayofweek
    df_diario['Mes'] = df_diario['Data'].dt.month
    df_diario['Dia'] = df_diario['Data'].dt.day

    return df_diario

def create_sequences(X, y, n_steps):
    Xs, ys = [], []
    for i in range(len(X) - n_steps):
        Xs.append(X[i:(i + n_steps)])
        ys.append(y[i + n_steps])
    return np.array(Xs), np.array(ys)

if __name__ == "__main__":
    produto_especifico = 26173  # Substitua pelo código do produto desejado
    train_and_evaluate_models(produto_especifico)
