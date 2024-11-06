# src/models/predict_sales.py

import pandas as pd
import numpy as np
import os
import joblib
from tensorflow.keras.models import load_model  # type: ignore
from src.utils.logging_config import get_logger
from workalendar.america import Brazil
from datetime import timedelta
from src.models.train_model import LabelEncoderSafe, N_STEPS  # Certifique-se de importar N_STEPS
import matplotlib.pyplot as plt
import seaborn as sns

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

    # Verificar propriedades do scaler_y
    logger.debug(f"Scaler_Y Mean: {getattr(scaler_y, 'center_', 'Não disponível')}")
    logger.debug(f"Scaler_Y Scale: {scaler_y.scale_}")

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

    # Verificar se todas as features estão presentes
    missing_features = [col for col in features if col not in df_future.columns]
    if missing_features:
        logger.error(f"As seguintes features estão faltando nos dados futuros: {missing_features}")
        return

    X_future = df_future[features]

    # Escalar os dados
    try:
        X_future_scaled = scaler_X.transform(X_future)
    except Exception as e:
        logger.error(f"Erro ao escalar os dados futuros: {e}")
        return

    # Carregar os últimos N_STEPS de dados históricos para iniciar as previsões
    df_historico = load_transaction_data(produto_especifico)
    df_historico = df_historico.sort_values('DataHora')
    df_historico_features = df_historico[features].tail(N_STEPS)
    if len(df_historico_features) < N_STEPS:
        logger.error(f'Dados históricos insuficientes para criar uma sequência de {N_STEPS} passos de tempo.')
        return

    X_historico_scaled = scaler_X.transform(df_historico_features)
    X_input_list = list(X_historico_scaled)

    predictions_scaled = []

    for i in range(len(X_future_scaled)):
        if len(X_input_list) < N_STEPS:
            logger.error(f'Dados insuficientes para criar uma sequência de {N_STEPS} passos de tempo.')
            break

        # Criar sequência de entrada
        X_input_seq = np.array(X_input_list[-N_STEPS:]).reshape(1, N_STEPS, -1)

        # Fazer previsão
        y_pred_scaled = model.predict(X_input_seq)
        predictions_scaled.append(y_pred_scaled[0][0])

        # Log da previsão escalada
        logger.debug(f"Previsão escalada (y_pred_scaled): {y_pred_scaled[0][0]}")

        # Atualizar a lista de entrada com a nova previsão escalada
        X_input_list.append(X_future_scaled[i])

    # Converter previsões para a escala original e aplicar a transformação inversa
    try:
        predictions = scaler_y.inverse_transform(np.array(predictions_scaled).reshape(-1, 1)).flatten()
        predictions = np.expm1(predictions)  # Inverso de log1p
    except Exception as e:
        logger.error(f"Erro ao inverter a escala das previsões: {e}")
        return

    # Log das previsões invertidas
    logger.debug(f"Previsões invertidas (ValorUnitarioPrevisto): {predictions}")

    # Preparar DataFrame de previsões
    df_predictions = df_future.iloc[:len(predictions)].copy()
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

    # Verificar propriedades do scaler_y
    logger.debug(f"Scaler_Y Mean: {getattr(scaler_y, 'center_', 'Não disponível')}")
    logger.debug(f"Scaler_Y Scale: {scaler_y.scale_}")

    # Preparar dados futuros para previsão
    future_dates = pd.date_range(start=FUTURE_START_DATE, end=FUTURE_END_DATE, freq='D')
    df_future = pd.DataFrame({'Data': future_dates})
    df_future['CodigoProduto'] = produto_especifico

    # Adicionar características futuras
    df_future = add_future_features(df_future, df, label_encoders, nivel='diario')

    # Selecionar features
    features = [
        'EmPromocao', 'DiaDaSemana', 'Mes', 'Dia', 'Feriado', 'VesperaDeFeriado',
        'Rentabilidade', 'DescontoAplicado', 'AcrescimoAplicado',
        'QuantidadeLiquida_Lag1', 'QuantidadeLiquida_Lag2', 'QuantidadeLiquida_Lag3',
        'CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo', 'CodigoFabricante',
        'CodigoFornecedor', 'CodigoKitPrincipal'
    ]

    # Verificar se todas as features estão presentes
    missing_features = [col for col in features if col not in df_future.columns]
    if missing_features:
        logger.error(f"As seguintes features estão faltando nos dados futuros: {missing_features}")
        return

    X_future = df_future[features]

    # Escalar os dados
    try:
        X_future_scaled = scaler_X.transform(X_future)
    except Exception as e:
        logger.error(f"Erro ao escalar os dados futuros: {e}")
        return

    # Carregar os últimos N_STEPS de dados históricos para iniciar as previsões
    df_historico = load_daily_data(produto_especifico)
    df_historico = df_historico.sort_values('Data').tail(N_STEPS)
    if len(df_historico) < N_STEPS:
        logger.error(f'Dados históricos insuficientes para criar uma sequência de {N_STEPS} passos de tempo.')
        return

    X_historico = df_historico[features]
    X_historico_scaled = scaler_X.transform(X_historico)
    X_input_list = list(X_historico_scaled)

    predictions_scaled = []

    for i in range(len(X_future_scaled)):
        if len(X_input_list) < N_STEPS:
            logger.error(f'Dados insuficientes para criar uma sequência de {N_STEPS} passos de tempo.')
            break

        # Criar sequência de entrada
        X_input_seq = np.array(X_input_list[-N_STEPS:]).reshape(1, N_STEPS, -1)

        # Fazer previsão
        y_pred_scaled = model.predict(X_input_seq)
        predictions_scaled.append(y_pred_scaled[0][0])

        # Log da previsão escalada
        logger.debug(f"Previsão escalada (y_pred_scaled): {y_pred_scaled[0][0]}")

        # Atualizar as features lag com a nova previsão escalada
        new_lag1 = y_pred_scaled[0][0]
        new_lag2 = X_input_list[-1][features.index('QuantidadeLiquida_Lag1')]
        new_lag3 = X_input_list[-1][features.index('QuantidadeLiquida_Lag2')]

        logger.debug(f"Atualizando lags com: QuantidadeLiquida_Lag1={new_lag1}, QuantidadeLiquida_Lag2={new_lag2}, QuantidadeLiquida_Lag3={new_lag3}")

        # Atualizar as features lag nas previsões futuras
        X_future_scaled[i][features.index('QuantidadeLiquida_Lag1')] = new_lag1
        X_future_scaled[i][features.index('QuantidadeLiquida_Lag2')] = new_lag2
        X_future_scaled[i][features.index('QuantidadeLiquida_Lag3')] = new_lag3

        # Adicionar a nova entrada para a sequência
        X_input_list.append(X_future_scaled[i])

    # Converter previsões para a escala original
    try:
        predictions = scaler_y.inverse_transform(np.array(predictions_scaled).reshape(-1, 1)).flatten()
    except Exception as e:
        logger.error(f"Erro ao inverter a escala das previsões: {e}")
        return

    # Preparar DataFrame de previsões
    df_predictions = df_future.iloc[:len(predictions)].copy()
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
    # Aqui você pode ajustar a lógica de acordo com o planejamento real de promoções
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
    # Chamada principal para iniciar as previsões
    # Exemplo: produto_especifico = 26173
    produto_especifico = 26173  # Substitua pelo código do produto desejado
    predict_future_sales(produto_especifico)
