# src/models/predict_sales.py

import pandas as pd
import numpy as np
import os
import joblib
from tensorflow.keras.models import load_model # type: ignore
from src.utils.logging_config import get_logger
from workalendar.america import Brazil
from datetime import timedelta
from src.models.train_model import LabelEncoderSafe, N_STEPS  # Certifique-se de importar N_STEPS
import matplotlib.pyplot as plt

logger = get_logger(__name__)

# ===========================
# Hiperparâmetros e Configurações
# ===========================

# Período para previsões futuras
FUTURE_START_DATE = '2024-01-01'
FUTURE_END_DATE = '2024-03-31'

# Caminhos para os diretórios de dados e modelos
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(SCRIPT_DIR)))
DATA_DIR = os.path.join(BASE_DIR, 'promopredictor', 'data')
MODELS_DIR = os.path.join(BASE_DIR, 'promopredictor', 'trained_models')
PREDICTIONS_DIR = os.path.join(DATA_DIR, 'predictions')
os.makedirs(PREDICTIONS_DIR, exist_ok=True)

# ===========================
# Funções Principais
# ===========================

def predict_future_sales(produto_especifico):
    logger.info(f"Iniciando previsões para o produto {produto_especifico}")

    # Previsão do preço unitário
    predict_unit_price(produto_especifico)

    # Previsão da quantidade vendida
    predict_quantity_sold(produto_especifico)

# ===========================
# Funções Auxiliares para Previsão do Preço Unitário
# ===========================

def predict_unit_price(produto_especifico):
    logger.info(f"Realizando previsão do preço unitário para o produto {produto_especifico}")

    # Carregar dados de transação
    df = load_transaction_data(produto_especifico)
    if df.empty:
        logger.error(f'Não há dados de transação para o produto {produto_especifico}.')
        return

    # Preparar dados
    df['DataHora'] = pd.to_datetime(df['DataHora'])
    df = df.sort_values('DataHora')

    # Carregar modelos e scalers
    model_path = os.path.join(MODELS_DIR, f'model_unit_price_{produto_especifico}.h5')
    scaler_X_path = os.path.join(MODELS_DIR, f'scaler_X_unit_price_{produto_especifico}.pkl')
    scaler_y_path = os.path.join(MODELS_DIR, f'scaler_y_unit_price_{produto_especifico}.pkl')
    encoders_path = os.path.join(MODELS_DIR, f'label_encoders_unit_price_{produto_especifico}.pkl')

    if not os.path.exists(model_path):
        logger.error(f"Modelo não encontrado em {model_path}")
        return

    model = load_model(model_path)
    scaler_X = joblib.load(scaler_X_path)
    scaler_y = joblib.load(scaler_y_path)
    label_encoders = joblib.load(encoders_path)

    # Preparar dados futuros para previsão
    future_dates = pd.date_range(start=FUTURE_START_DATE, end=FUTURE_END_DATE, freq='D')
    df_future = pd.DataFrame({'Data': future_dates})
    df_future['CodigoProduto'] = produto_especifico

    # Adicionar características futuras
    df_future = add_future_features(df_future, df, label_encoders, nivel='transacao')

    # Selecionar features
    features = [
        'Quantidade', 'Desconto', 'Acrescimo', 'DescontoGeral', 'AcrescimoGeral',
        'EmPromocao', 'DiaDaSemana', 'Mes', 'Dia', 'Feriado', 'VesperaDeFeriado',
        'CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo', 'CodigoFabricante',
        'CodigoFornecedor', 'CodigoKitPrincipal', 'ValorCusto'
    ]

    X_future = df_future[features]

    # Escalar os dados
    X_future_scaled = scaler_X.transform(X_future)

    # Fazer previsões
    predictions_scaled = []
    X_input_list = list(X_future_scaled[:N_STEPS])

    for i in range(N_STEPS, len(X_future_scaled)):
        X_input_seq = np.array(X_input_list[-N_STEPS:]).reshape(1, N_STEPS, -1)
        y_pred_scaled = model.predict(X_input_seq)
        predictions_scaled.append(y_pred_scaled[0][0])
        X_input_list.append(X_future_scaled[i])

    # Converter previsões para a escala original
    predictions = scaler_y.inverse_transform(np.array(predictions_scaled).reshape(-1, 1)).flatten()

    # Preparar DataFrame de previsões
    df_predictions = df_future.iloc[N_STEPS:].copy()
    df_predictions['ValorUnitarioPrevisto'] = predictions

    # Salvar previsões
    predictions_path = os.path.join(PREDICTIONS_DIR, f'predictions_unit_price_{produto_especifico}.csv')
    df_predictions.to_csv(predictions_path, index=False)
    logger.info(f"Previsões de preço unitário salvas em {predictions_path}")

# ===========================
# Funções Auxiliares para Previsão da Quantidade Vendida
# ===========================

def predict_quantity_sold(produto_especifico):
    logger.info(f"Realizando previsão da quantidade vendida para o produto {produto_especifico}")

    # Carregar dados diários
    df = load_daily_data(produto_especifico)
    if df.empty:
        logger.error(f'Não há dados diários para o produto {produto_especifico}.')
        return

    # Preparar dados
    df['Data'] = pd.to_datetime(df['Data'])
    df = df.sort_values('Data')

    # Carregar modelos e scalers
    model_path = os.path.join(MODELS_DIR, f'model_quantity_{produto_especifico}.h5')
    scaler_X_path = os.path.join(MODELS_DIR, f'scaler_X_quantity_{produto_especifico}.pkl')
    scaler_y_path = os.path.join(MODELS_DIR, f'scaler_y_quantity_{produto_especifico}.pkl')
    encoders_path = os.path.join(MODELS_DIR, f'label_encoders_quantity_{produto_especifico}.pkl')

    if not os.path.exists(model_path):
        logger.error(f"Modelo não encontrado em {model_path}")
        return

    model = load_model(model_path)
    scaler_X = joblib.load(scaler_X_path)
    scaler_y = joblib.load(scaler_y_path)
    label_encoders = joblib.load(encoders_path)

    # Preparar dados futuros para previsão
    future_dates = pd.date_range(start=FUTURE_START_DATE, end=FUTURE_END_DATE, freq='D')
    df_future = pd.DataFrame({'Data': future_dates})
    df_future['CodigoProduto'] = produto_especifico

    # Adicionar características futuras
    df_future = add_future_features(df_future, df, label_encoders, nivel='diario')

    # Criar features lag iniciais a partir dos dados históricos
    df = create_lag_features(df, N_STEPS)
    last_lags = df[['QuantidadeLiquida_Lag1', 'QuantidadeLiquida_Lag2', 'QuantidadeLiquida_Lag3']].iloc[-1].values.tolist()
    df_future['QuantidadeLiquida_Lag1'] = last_lags[0]
    df_future['QuantidadeLiquida_Lag2'] = last_lags[1]
    df_future['QuantidadeLiquida_Lag3'] = last_lags[2]

    # Selecionar features
    features = [
        'EmPromocao', 'DiaDaSemana', 'Mes', 'Dia', 'Feriado', 'VesperaDeFeriado',
        'Rentabilidade', 'DescontoAplicado', 'AcrescimoAplicado',
        'QuantidadeLiquida_Lag1', 'QuantidadeLiquida_Lag2', 'QuantidadeLiquida_Lag3',
        'CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo', 'CodigoFabricante',
        'CodigoFornecedor', 'CodigoKitPrincipal'
    ]

    X_future = df_future[features]

    # Escalar os dados
    X_future_scaled = scaler_X.transform(X_future)

    # Fazer previsões
    predictions_scaled = []
    X_input_list = list(X_future_scaled[:N_STEPS])

    for i in range(N_STEPS, len(X_future_scaled)):
        X_input_seq = np.array(X_input_list[-N_STEPS:]).reshape(1, N_STEPS, -1)
        y_pred_scaled = model.predict(X_input_seq)
        predictions_scaled.append(y_pred_scaled[0][0])

        # Atualizar features lag
        next_input = X_future_scaled[i]
        next_input[features.index('QuantidadeLiquida_Lag1')] = y_pred_scaled[0][0]
        next_input[features.index('QuantidadeLiquida_Lag2')] = X_input_list[-1][features.index('QuantidadeLiquida_Lag1')]
        next_input[features.index('QuantidadeLiquida_Lag3')] = X_input_list[-1][features.index('QuantidadeLiquida_Lag2')]
        X_input_list.append(next_input)

    # Converter previsões para a escala original
    predictions = scaler_y.inverse_transform(np.array(predictions_scaled).reshape(-1, 1)).flatten()

    # Preparar DataFrame de previsões
    df_predictions = df_future.iloc[N_STEPS:].copy()
    df_predictions['QuantidadePrevista'] = predictions

    # Salvar previsões
    predictions_path = os.path.join(PREDICTIONS_DIR, f'predictions_quantity_{produto_especifico}.csv')
    df_predictions.to_csv(predictions_path, index=False)
    logger.info(f"Previsões de quantidade vendida salvas em {predictions_path}")

# ===========================
# Funções Auxiliares Comuns
# ===========================

def load_transaction_data(produto_especifico):
    data_path = os.path.join(DATA_DIR, f'dados_transacao_{produto_especifico}.csv')
    data_path = os.path.abspath(data_path)
    logger.info(f"Carregando dados de transação de: {data_path}")

    if not os.path.exists(data_path):
        logger.error(f"Arquivo de dados de transação não encontrado para o produto {produto_especifico}.")
        return pd.DataFrame()

    df = pd.read_csv(data_path, parse_dates=['Data', 'DataHora'])
    return df

def load_daily_data(produto_especifico):
    data_path = os.path.join(DATA_DIR, f'dados_agrupados_{produto_especifico}.csv')
    data_path = os.path.abspath(data_path)
    logger.info(f"Carregando dados diários de: {data_path}")

    if not os.path.exists(data_path):
        logger.error(f"Arquivo de dados diários não encontrado para o produto {produto_especifico}.")
        return pd.DataFrame()

    df = pd.read_csv(data_path, parse_dates=['Data'])
    return df

def add_future_features(df_future, df_historico, label_encoders, nivel='transacao'):
    # Adicionar características temporais
    df_future['DiaDaSemana'] = df_future['Data'].dt.dayofweek
    df_future['Mes'] = df_future['Data'].dt.month
    df_future['Dia'] = df_future['Data'].dt.day

    # Marcar feriados e vésperas de feriado prolongado
    cal = Brazil()
    df_future['Feriado'] = df_future['Data'].apply(lambda x: 1 if cal.is_holiday(x) else 0)
    df_future['VesperaDeFeriado'] = df_future['Data'].apply(lambda x: 1 if is_feriado_prolongado(x, cal) else 0)

    # Definir 'EmPromocao' conforme planejamento de promoções
    df_future['EmPromocao'] = df_future['Data'].apply(lambda x: 1 if x.day <= 14 else 0)

    # Features categóricas
    categorical_cols = ['CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo', 'CodigoFabricante',
                        'CodigoFornecedor', 'CodigoKitPrincipal']

    for col in categorical_cols:
        most_common_value = df_historico[col].astype(str).mode()[0]
        le = label_encoders[col]
        if most_common_value in le.classes_:
            df_future[col] = le.transform([most_common_value])[0]
        else:
            df_future[col] = -1  # Valor para rótulos desconhecidos

    # Features numéricas
    if nivel == 'transacao':
        df_future['Quantidade'] = df_historico['Quantidade'].mean()
        df_future['Desconto'] = df_historico['Desconto'].mean()
        df_future['Acrescimo'] = df_historico['Acrescimo'].mean()
        df_future['DescontoGeral'] = df_historico['DescontoGeral'].mean()
        df_future['AcrescimoGeral'] = df_historico['AcrescimoGeral'].mean()
        df_future['ValorCusto'] = df_historico['ValorCusto'].mean()
    else:
        df_future['Rentabilidade'] = df_historico['Rentabilidade'].mean()
        df_future['DescontoAplicado'] = df_historico['DescontoAplicado'].mean()
        df_future['AcrescimoAplicado'] = df_historico['AcrescimoAplicado'].mean()

    df_future.fillna(0, inplace=True)
    return df_future

def create_lag_features(df, n_lags):
    for lag in range(1, 4):
        df[f'QuantidadeLiquida_Lag{lag}'] = df['QuantidadeLiquida'].shift(lag)
    df.fillna(0, inplace=True)
    return df

def is_feriado_prolongado(date, calendar):
    if calendar.is_holiday(date + timedelta(days=1)):
        return True
    elif calendar.is_holiday(date + timedelta(days=3)) and date.weekday() == 4:
        return True
    elif calendar.is_holiday(date + timedelta(days=2)) and date.weekday() == 3:
        return True
    return False

# ===========================
# Execução Principal
# ===========================

if __name__ == "__main__":
    # Lista de produtos a serem processados
    produtos_especificos = [26173]  # Substitua pelos códigos dos produtos desejados
    for produto in produtos_especificos:
        predict_future_sales(produto)
