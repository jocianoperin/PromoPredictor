# src/models/predict_sales.py

import pandas as pd
import numpy as np
import os
import joblib
from tensorflow.keras.models import load_model
from src.utils.logging_config import get_logger
from workalendar.america import Brazil
from datetime import timedelta

logger = get_logger(__name__)

# ===========================
# Hiperparâmetros e Configurações
# ===========================

# Número de passos na sequência (deve corresponder ao usado no treinamento)
N_STEPS = 30

# Período para previsões futuras
FUTURE_START_DATE = '2024-01-01'
FUTURE_END_DATE = '2024-03-31'

# Caminhos para os diretórios de dados e modelos
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_PATH = os.path.join(BASE_DIR, 'data', 'dados_processados.csv')
MODELS_DIR = os.path.join(BASE_DIR, 'trained_models')
PREDICTIONS_DIR = os.path.join(BASE_DIR, 'data', 'predictions')

# ===========================
# Funções Auxiliares
# ===========================

def fetch_data_for_prediction(produto_especifico):
    """
    Carrega os dados processados para o produto específico.
    
    Args:
        produto_especifico (int): Código do produto para o qual as previsões serão realizadas.
    
    Returns:
        pd.DataFrame: DataFrame filtrado para o produto específico.
    """
    logger.info(f"Carregando dados de: {DATA_PATH}")
    
    if not os.path.exists(DATA_PATH):
        logger.error(f"Arquivo de dados não encontrado em: {DATA_PATH}")
        return None
    
    df = pd.read_csv(DATA_PATH, parse_dates=['Data'])
    df_produto = df[df['CodigoProduto'] == produto_especifico]
    logger.info(f"Dados carregados: {len(df_produto)} registros para o produto {produto_especifico}.")
    return df_produto

def preprocess_data(df, scaler):
    """
    Pré-processa os dados históricos para previsão.
    
    Args:
        df (pd.DataFrame): DataFrame com os dados históricos.
        scaler (sklearn.preprocessing.MinMaxScaler): Scaler ajustado para transformar os dados.
    
    Returns:
        np.ndarray: Dados escalados prontos para serem usados no modelo.
    """
    try:
        logger.info("Iniciando o pré-processamento dos dados.")
        df['Data'] = pd.to_datetime(df['Data'])
        df = df.sort_values(by=['Data'])

        # Features de tempo
        df['DiaDaSemana'] = df['Data'].dt.dayofweek
        df['Mes'] = df['Data'].dt.month
        df['Dia'] = df['Data'].dt.day
        df['ValorUnitario'] = df['ValorTotal'] / df['QuantidadeLiquida']
        df['ValorUnitario'] = df['ValorUnitario'].replace([np.inf, -np.inf], np.nan).fillna(0)

        # Preencher NaNs nas features
        df['EmPromocao'] = df['EmPromocao'].fillna(0)
        df['Feriado'] = df['Feriado'].fillna(0)
        df['VésperaDeFeriado'] = df['VésperaDeFeriado'].fillna(0)

        # Selecionar as colunas para o modelo, incluindo 'VésperaDeFeriado'
        features = ['EmPromocao', 'DiaDaSemana', 'Mes', 'Dia', 'Feriado', 'VésperaDeFeriado']
        X = df[features]

        # Escalar os dados usando o scaler ajustado
        logger.info("Escalando os dados.")
        data_scaled = scaler.transform(X)

        logger.info("Pré-processamento concluído com sucesso.")
        return data_scaled
    except Exception as e:
        logger.error(f"Erro no pré-processamento dos dados: {e}")
        return None

def is_feriado_prolongado(date, calendar):
    """
    Identifica se a data é véspera de um feriado prolongado.
    
    Args:
        date (pd.Timestamp): Data a ser verificada.
        calendar (workalendar.core.Calendar): Calendário para verificar feriados.
    
    Returns:
        bool: True se for véspera de feriado prolongado, False caso contrário.
    """
    if calendar.is_holiday(date + timedelta(days=1)):
        return True
    elif calendar.is_holiday(date + timedelta(days=3)) and date.weekday() == 4:
        return True
    elif calendar.is_holiday(date + timedelta(days=2)) and date.weekday() == 3:
        return True
    return False

# ===========================
# Função Principal
# ===========================

def predict_future_sales(produto_especifico):
    """
    Realiza previsões futuras de vendas para um produto específico.
    
    Args:
        produto_especifico (int): Código do produto para o qual as previsões serão realizadas.
    """
    logger.info("Iniciando o processo de previsão.")

    # Carregar dados do produto específico
    df = fetch_data_for_prediction(produto_especifico)
    if df is None or df.empty:
        logger.error("Nenhum dado retornado para previsão. Abortando o processo.")
        return

    try:
        logger.info(f"Diretório dos modelos: {MODELS_DIR}")

        # Carregar os modelos treinados e o scaler
        model_un_path = os.path.join(MODELS_DIR, f'model_un_{produto_especifico}.h5')
        model_valor_path = os.path.join(MODELS_DIR, f'model_valor_{produto_especifico}.h5')
        scaler_path = os.path.join(MODELS_DIR, f'scaler_{produto_especifico}.pkl')

        logger.info(f"Carregando modelos e scaler: {model_un_path}, {model_valor_path}, {scaler_path}")

        # Verificar existência dos arquivos
        if not os.path.exists(model_un_path) or not os.path.exists(model_valor_path) or not os.path.exists(scaler_path):
            logger.error("Modelos ou scaler não encontrados.")
            return

        # Carregar modelos e scaler
        model_un = load_model(model_un_path, compile=False)
        model_valor = load_model(model_valor_path, compile=False)
        scaler = joblib.load(scaler_path)
        logger.info("Modelos e scaler carregados com sucesso.")

        # Pré-processar os dados históricos
        data_scaled = preprocess_data(df, scaler)
        if data_scaled is None or len(data_scaled) == 0:
            logger.error("Erro no pré-processamento dos dados para previsão.")
            return

        # Preparar dados futuros para previsão
        future_dates = pd.date_range(start=FUTURE_START_DATE, end=FUTURE_END_DATE, freq='D')
        df_future = pd.DataFrame({'Data': future_dates})
        df_future['EmPromocao'] = 0  # Ajuste conforme necessário

        # Adicionar features temporais
        df_future['DiaDaSemana'] = df_future['Data'].dt.dayofweek
        df_future['Mes'] = df_future['Data'].dt.month
        df_future['Dia'] = df_future['Data'].dt.day

        # Marcar feriados e vésperas de feriado prolongado
        cal = Brazil()
        df_future['Feriado'] = df_future['Data'].apply(lambda x: 1 if cal.is_holiday(x) else 0)
        df_future['VésperaDeFeriado'] = df_future['Data'].apply(lambda x: 1 if is_feriado_prolongado(x, cal) else 0)

        # Preencher NaNs nas features futuras
        df_future['EmPromocao'] = df_future['EmPromocao'].fillna(0)
        df_future['Feriado'] = df_future['Feriado'].fillna(0)
        df_future['VésperaDeFeriado'] = df_future['VésperaDeFeriado'].fillna(0)

        # Selecionar colunas
        features = ['EmPromocao', 'DiaDaSemana', 'Mes', 'Dia', 'Feriado', 'VésperaDeFeriado']
        X_future = df_future[features]
        X_future_scaled = scaler.transform(X_future)
        logger.info("Dados futuros preparados e escalados.")

        # Utilizar os últimos 'N_STEPS' dias dos dados históricos para iniciar as previsões
        X_history = data_scaled[-N_STEPS:]
        logger.info(f"Usando os últimos {N_STEPS} dias como histórico para as previsões.")

        predictions_un = []
        predictions_valor = []

        logger.info("Iniciando a geração de previsões.")

        for i in range(len(X_future_scaled)):
            # Concatenar o histórico com os dados futuros até o dia atual
            X_input = np.concatenate([X_history, X_future_scaled[:i+1]], axis=0)
            if len(X_input) < N_STEPS:
                logger.debug(f"Dia {i+1}: Dados insuficientes para criar a sequência. Pulando.")
                continue  # Pular se não houver dados suficientes
            X_input_seq = []
            for j in range(len(X_input) - N_STEPS + 1):
                X_input_seq.append(X_input[j:j + N_STEPS])

            X_input_seq = np.array(X_input_seq)

            # Previsão de unidades vendidas
            y_pred_un = model_un.predict(X_input_seq)
            predictions_un.append(y_pred_un[-1][0])

            # Previsão de valor unitário
            y_pred_valor = model_valor.predict(X_input_seq)
            predictions_valor.append(y_pred_valor[-1][0])

            if (i + 1) % 50 == 0 or (i + 1) == len(X_future_scaled):
                logger.info(f"Previsões geradas para {i + 1} dias de previsão.")

        logger.info(f"Total de previsões geradas: {len(predictions_un)}")

        # Ajustar o DataFrame de previsões
        df_predictions = df_future.iloc[N_STEPS - 1:].copy()
        logger.info(f"DataFrame de previsões tem {len(df_predictions)} linhas.")

        # Verificar se o número de previsões corresponde ao número de linhas no DataFrame
        expected_length = len(df_predictions)
        actual_length = len(predictions_un)
        logger.info(f"Esperado: {expected_length}, Atual: {actual_length}")

        if actual_length < expected_length:
            logger.error(f"Previsões geradas ({actual_length}) são menores que o esperado ({expected_length}).")
            return
        elif actual_length > expected_length:
            logger.warning(f"Previsões geradas ({actual_length}) são maiores que o esperado ({expected_length}). Cortando as excedentes.")
            logger.info(f"Cortando 'predictions_un' para os últimos {expected_length} elementos.")
            logger.info(f"Cortando 'predictions_valor' para os últimos {expected_length} elementos.")

            # Cortar as previsões excedentes
            predictions_un = predictions_un[-expected_length:]
            predictions_valor = predictions_valor[-expected_length:]

        df_predictions['ValorUnitarioPrevisto'] = predictions_valor
        df_predictions['QuantidadeTotalPrevista'] = predictions_un

        # Salvar previsões
        os.makedirs(PREDICTIONS_DIR, exist_ok=True)
        predictions_path = os.path.join(PREDICTIONS_DIR, f'predictions_{produto_especifico}.csv')
        df_predictions.to_csv(predictions_path, index=False)

    except Exception as e:
        logger.error(f"Erro durante o processo de previsão: {e}")

# ===========================
# Execução Principal
# ===========================

if __name__ == "__main__":
    produto_especifico = 26173  # Substitua pelo código do produto desejado
    predict_future_sales(produto_especifico)