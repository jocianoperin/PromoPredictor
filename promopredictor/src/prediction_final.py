import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, LSTM, Dropout
from kerastuner.tuners import RandomSearch
from utils.logging_config import get_logger

# Configurar logging
logger = get_logger("lstm_training")

# **1. Carregando o Dataset**
logger.info("Carregando o dataset.")
csv_path = "/home/jociano/Projects/PromoPredictor/promopredictor/data/dados_transacao_26173.csv"
full_data = pd.read_csv(csv_path, parse_dates=['Data'], dayfirst=True)
logger.info(f"Dataset carregado com {full_data.shape[0]} registros.")

# **2. Dividindo os Dados em Treinamento e Predição**
logger.info("Dividindo os dados em conjunto de treinamento e predição.")
treino_fim = '2023-12-31'
predicao_inicio = '2024-01-01'
predicao_fim = '2024-03-30'

treino_teste_data = full_data[full_data['Data'] <= treino_fim]
predicao_data = full_data[(full_data['Data'] >= predicao_inicio) & (full_data['Data'] <= predicao_fim)]

logger.info(f"Conjunto de treinamento: {treino_teste_data.shape[0]} registros.")
logger.info(f"Conjunto de predição: {predicao_data.shape[0]} registros.")

# **3. Agregando os Dados por Dia**
logger.info("Agregando os dados por dia.")
treino_teste_agg = treino_teste_data.groupby('Data').agg({
    'ValorUnitario': 'mean',
    'Quantidade': 'sum'
}).reset_index()

predicao_agg = predicao_data.groupby('Data').agg({
    'ValorUnitario': 'mean',
    'Quantidade': 'sum'
}).reset_index()

logger.info(f"Tamanho após agregação (treino): {treino_teste_agg.shape}.")
logger.info(f"Tamanho após agregação (predição): {predicao_agg.shape}.")

# **4. Normalizando os Dados**
logger.info("Normalizando os dados com MinMaxScaler.")
scaler = MinMaxScaler()
treino_teste_scaled = scaler.fit_transform(treino_teste_agg[['ValorUnitario', 'Quantidade']])
treino_teste_agg[['ValorUnitario_scaled', 'Quantidade_scaled']] = treino_teste_scaled

# Normalizando os dados de predição
predicao_agg[['ValorUnitario_scaled', 'Quantidade_scaled']] = scaler.transform(
    predicao_agg[['ValorUnitario', 'Quantidade']]
)

# **5. Preparando os Dados para LSTM**
logger.info("Preparando os dados para o modelo LSTM.")
n_steps = 30  # Janela de 30 dias
features = treino_teste_agg[['ValorUnitario_scaled', 'Quantidade_scaled']].values
X, y = [], []

for i in range(len(features) - n_steps):
    X.append(features[i:i + n_steps])
    y.append(features[i + n_steps])

X, y = np.array(X), np.array(y)

logger.info(f"Formato do conjunto de treinamento - X: {X.shape}, y: {y.shape}.")

# **6. Função para Construir o Modelo (Keras Tuner)**
def build_model(hp):
    logger.info("Construindo o modelo LSTM com hiperparâmetros ajustáveis.")
    model = Sequential()
    for i in range(hp.Int("num_layers", 2, 5)):  # Ajusta o número de camadas entre 2 e 5
        model.add(LSTM(
            units=hp.Int(f"units_layer_{i}", min_value=64, max_value=256, step=64),
            activation='tanh',
            return_sequences=True if i < hp.Int("num_layers", 2, 5) - 1 else False,
            input_shape=(n_steps, 2) if i == 0 else None
        ))
        model.add(Dropout(hp.Float(f"dropout_layer_{i}", min_value=0.1, max_value=0.5, step=0.1)))
    model.add(Dense(2))  # Saída com 2 valores
    model.compile(optimizer='adam', loss='mse', metrics=['mae'])
    return model

# **7. Configurando o Keras Tuner**
logger.info("Configurando o Keras Tuner para busca de hiperparâmetros.")
tuner = RandomSearch(
    build_model,
    objective='val_loss',
    max_trials=10,
    executions_per_trial=2,
    directory='ktuner_dir',
    project_name='lstm_tuning'
)

# **8. Realizando a Busca de Hiperparâmetros**
logger.info("Iniciando a busca pelos melhores hiperparâmetros.")
tuner.search(
    X, y,
    epochs=200,
    validation_split=0.2,
    batch_size=16,
    verbose=2
)

# **9. Treinando o Melhor Modelo**
logger.info("Selecionando os melhores hiperparâmetros e treinando o modelo.")
best_hps = tuner.get_best_hyperparameters(num_trials=1)[0]
logger.info(f"Melhores hiperparâmetros encontrados: {best_hps.values}")

best_model = tuner.hypermodel.build(best_hps)
history = best_model.fit(
    X, y,
    epochs=200,
    validation_split=0.2,
    batch_size=16,
    verbose=2
)

# **10. Predizendo os Valores**
logger.info("Realizando predições para o período de 2024.")
predicao_features = treino_teste_agg[['ValorUnitario_scaled', 'Quantidade_scaled']].values[-n_steps:]
predicao_X = [predicao_features]
predicao_X = np.array(predicao_X)

predictions_scaled = best_model.predict(predicao_X)
predictions = scaler.inverse_transform(predictions_scaled)

# **11. Comparação com os Valores Reais**
logger.info("Comparando os valores reais com os valores previstos.")
comparacao = predicao_agg.copy()
comparacao['ValorUnitario_Predito'] = predictions[:, 0]
comparacao['Quantidade_Predita'] = predictions[:, 1]

# **12. Salvando os Resultados**
output_path = "comparacao_resultados.csv"
comparacao.to_csv(output_path, index=False)
logger.info(f"Resultados salvos em '{output_path}'.")
