# src/models/predict_sales.py

import pandas as pd
import numpy as np
import os
import joblib
from tensorflow.keras.models import load_model
from src.utils.logging_config import get_logger
from workalendar.america import Brazil
from datetime import timedelta
from src.models.train_model import LabelEncoderSafe  # Certifique-se de que LabelEncoderSafe esteja acessível

logger = get_logger(__name__)

# ===========================
# Hiperparâmetros e Configurações
# ===========================

# Número de passos na sequência (deve corresponder ao usado no treinamento)
N_STEPS = 30  # Atualizado para corresponder ao valor em train_model.py

# Período para previsões futuras
FUTURE_START_DATE = '2024-01-01'
FUTURE_END_DATE = '2024-03-31'

# Caminhos para os diretórios de dados e modelos
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, 'promopredictor', 'data')
MODELS_DIR = os.path.join(BASE_DIR, 'trained_models')
PREDICTIONS_DIR = os.path.join(DATA_DIR, 'predictions')

# ===========================
# Funções Auxiliares
# ===========================

def fetch_data_for_prediction(produto_especifico):
    """
    Carrega os dados processados para o produto específico.
    """
    data_path = os.path.join(DATA_DIR, f'dados_processados_{produto_especifico}.csv')
    logger.info(f"Carregando dados de: {data_path}")

    if not os.path.exists(data_path):
        logger.error(f"Arquivo de dados não encontrado em: {data_path}")
        return None

    df = pd.read_csv(data_path, parse_dates=['Data'])
    return df

def preprocess_data(df, scaler_X, label_encoders):
    """
    Pré-processa os dados históricos para previsão.
    """
    try:
        logger.info("Iniciando o pré-processamento dos dados.")
        df['Data'] = pd.to_datetime(df['Data'])
        df = df.sort_values(by=['Data'])

        # Preencher NaNs nas features
        features = [
            'EmPromocao', 'DiaDaSemana', 'Mes', 'Dia', 'Feriado', 'VésperaDeFeriado',
            'Rentabilidade', 'DescontoAplicado', 'AcrescimoAplicado',
            'CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo', 'CodigoFabricante',
            'CodigoFornecedor', 'CodigoKitPrincipal'
        ]

        df[features] = df[features].fillna(0)

        # Definir as features categóricas e numéricas
        categorical_cols = [
            'CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo',
            'CodigoFabricante', 'CodigoFornecedor', 'CodigoKitPrincipal'
        ]

        numerical_cols = ['Rentabilidade', 'DescontoAplicado', 'AcrescimoAplicado']

        # Codificar variáveis categóricas
        for col in categorical_cols:
            le = label_encoders[col]
            df[col] = df[col].astype(str)
            df[col] = le.transform(df[col])

        # Selecionar as features para o modelo
        X = df[features]

        # Escalar os dados usando o scaler ajustado
        logger.info("Escalando os dados.")
        data_scaled = scaler_X.transform(X)

        logger.info("Pré-processamento concluído com sucesso.")
        return data_scaled
    except Exception as e:
        logger.error(f"Erro no pré-processamento dos dados: {e}")
        return None

def is_feriado_prolongado(date, calendar):
    """
    Identifica se a data é véspera de um feriado prolongado.
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
    """
    logger.info(f"Iniciando o processo de previsão para o produto {produto_especifico}.")

    # Carregar dados do produto específico
    df = fetch_data_for_prediction(produto_especifico)
    if df is None or df.empty:
        logger.error("Nenhum dado retornado para previsão. Abortando o processo.")
        return

    try:
        logger.info(f"Diretório dos modelos: {MODELS_DIR}")

        # Carregar os modelos treinados, os scalers e os encoders
        model_un_path = os.path.join(MODELS_DIR, f'model_un_{produto_especifico}.h5')
        model_valor_path = os.path.join(MODELS_DIR, f'model_valor_{produto_especifico}.h5')
        scaler_X_path = os.path.join(MODELS_DIR, f'scaler_X_{produto_especifico}.pkl')
        scaler_y_un_path = os.path.join(MODELS_DIR, f'scaler_y_un_{produto_especifico}.pkl')
        scaler_y_valor_path = os.path.join(MODELS_DIR, f'scaler_y_valor_{produto_especifico}.pkl')
        encoders_path = os.path.join(MODELS_DIR, f'label_encoders_{produto_especifico}.pkl')

        logger.info("Carregando modelos, scalers e encoders.")

        # Verificar existência dos arquivos
        if not all(os.path.exists(path) for path in [model_un_path, model_valor_path, scaler_X_path, scaler_y_un_path, scaler_y_valor_path, encoders_path]):
            logger.error("Modelos, scalers ou encoders não encontrados.")
            return

        # Carregar modelos, scalers e encoders
        model_un = load_model(model_un_path, compile=False)
        model_valor = load_model(model_valor_path, compile=False)
        scaler_X = joblib.load(scaler_X_path)
        scaler_y_un = joblib.load(scaler_y_un_path)
        scaler_y_valor = joblib.load(scaler_y_valor_path)
        label_encoders = joblib.load(encoders_path)
        logger.info("Modelos, scalers e encoders carregados com sucesso.")

        # Pré-processar os dados históricos
        data_scaled = preprocess_data(df, scaler_X, label_encoders)
        if data_scaled is None or len(data_scaled) == 0:
            logger.error("Erro no pré-processamento dos dados para previsão.")
            return

        # Preparar dados futuros para previsão
        future_dates = pd.date_range(start=FUTURE_START_DATE, end=FUTURE_END_DATE, freq='D')
        df_future = pd.DataFrame({'Data': future_dates})
        df_future['CodigoProduto'] = produto_especifico

        # Adicionar features temporais
        df_future['DiaDaSemana'] = df_future['Data'].dt.dayofweek
        df_future['Mes'] = df_future['Data'].dt.month
        df_future['Dia'] = df_future['Data'].dt.day

        # Marcar feriados e vésperas de feriado prolongado
        cal = Brazil()
        df_future['Feriado'] = df_future['Data'].apply(lambda x: 1 if cal.is_holiday(x) else 0)
        df_future['VésperaDeFeriado'] = df_future['Data'].apply(lambda x: 1 if is_feriado_prolongado(x, cal) else 0)

        # Definir 'EmPromocao' conforme necessário (padrão é 0)
        df_future['EmPromocao'] = 0  # Ajuste conforme necessário

        # Definir as features categóricas e numéricas
        categorical_cols = [
            'CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo',
            'CodigoFabricante', 'CodigoFornecedor', 'CodigoKitPrincipal'
        ]

        numerical_cols = ['Rentabilidade', 'DescontoAplicado', 'AcrescimoAplicado']

        additional_features = numerical_cols + categorical_cols

        # Para features categóricas, usar o valor mais frequente dos dados históricos
        for col in categorical_cols:
            most_common_value = df[col].astype(str).mode()[0]
            le = label_encoders[col]
            if most_common_value in le.classes_:
                df_future[col] = le.transform([most_common_value])[0]
            else:
                df_future[col] = -1  # Valor para rótulos desconhecidos

        # Para features numéricas, preencher com zeros ou outro valor padrão
        for col in numerical_cols:
            df_future[col] = 0  # Ou df[col].mean() se fizer sentido

        # Garantir que não haja NaNs
        df_future.fillna(0, inplace=True)

        # Selecionar features
        features = [
            'EmPromocao', 'DiaDaSemana', 'Mes', 'Dia', 'Feriado', 'VésperaDeFeriado'
        ] + additional_features

        X_future = df_future[features]

        # Escalar os dados futuros
        X_future_scaled = scaler_X.transform(X_future)
        logger.info("Dados futuros preparados e escalados.")

        # Utilizar os últimos 'N_STEPS' dias dos dados históricos para iniciar as previsões
        X_history = data_scaled[-N_STEPS:]
        logger.info(f"Usando os últimos {N_STEPS} dias como histórico para as previsões.")

        predictions_un_scaled = []
        predictions_valor_scaled = []

        logger.info("Iniciando a geração de previsões.")

        # Inicializar a lista com o histórico
        X_input_list = list(X_history)

        for i in range(len(X_future_scaled)):
            # Adicionar a próxima entrada futura
            X_input_list.append(X_future_scaled[i])

            # Verificar se temos N_STEPS para fazer a previsão
            if len(X_input_list) < N_STEPS:
                continue  # Pular se não houver dados suficientes

            # Obter a sequência de entrada
            X_input_seq = np.array(X_input_list[-N_STEPS:]).reshape(1, N_STEPS, -1)

            # Previsão de unidades vendidas
            y_pred_un_scaled = model_un.predict(X_input_seq)
            predictions_un_scaled.append(y_pred_un_scaled[0][0])

            # Previsão de valor unitário
            y_pred_valor_scaled = model_valor.predict(X_input_seq)
            predictions_valor_scaled.append(y_pred_valor_scaled[0][0])

            # Atualizar o histórico com a previsão (opcional, se quiser usar as previsões nas próximas entradas)
            # X_input_list[-1][-2] = y_pred_un_scaled[0][0]  # Se quiser adicionar a previsão ao input

            if (i + 1) % 10 == 0 or (i + 1) == len(X_future_scaled):
                logger.info(f"Previsões geradas para {i + 1} dias de previsão.")

        logger.info(f"Total de previsões geradas: {len(predictions_un_scaled)}")

        # Ajustar o DataFrame de previsões
        df_predictions = df_future.iloc[N_STEPS - 1:].copy()
        logger.info(f"DataFrame de previsões tem {len(df_predictions)} linhas.")

        # Verificar se o número de previsões corresponde ao número de linhas no DataFrame
        expected_length = len(df_predictions)
        actual_length = len(predictions_un_scaled)
        logger.info(f"Esperado: {expected_length}, Atual: {actual_length}")

        if actual_length < expected_length:
            logger.error(f"Previsões geradas ({actual_length}) são menores que o esperado ({expected_length}).")
            return
        elif actual_length > expected_length:
            logger.warning(f"Previsões geradas ({actual_length}) são maiores que o esperado ({expected_length}). Cortando as excedentes.")
            predictions_un_scaled = predictions_un_scaled[-expected_length:]
            predictions_valor_scaled = predictions_valor_scaled[-expected_length:]

        # Converter as previsões de volta à escala original
        predictions_un = scaler_y_un.inverse_transform(np.array(predictions_un_scaled).reshape(-1, 1)).flatten()
        predictions_valor = scaler_y_valor.inverse_transform(np.array(predictions_valor_scaled).reshape(-1, 1)).flatten()

        df_predictions['ValorUnitarioPrevisto'] = predictions_valor
        df_predictions['QuantidadeTotalPrevista'] = predictions_un
        df_predictions['CodigoProduto'] = produto_especifico

        # Salvar previsões
        os.makedirs(PREDICTIONS_DIR, exist_ok=True)
        predictions_path = os.path.join(PREDICTIONS_DIR, f'predictions_{produto_especifico}.csv')
        df_predictions.to_csv(predictions_path, index=False)
        logger.info(f"Previsões salvas em {predictions_path}")

    except Exception as e:
        logger.error(f"Erro durante o processo de previsão para o produto {produto_especifico}: {e}")

# ===========================
# Execução Principal
# ===========================

if __name__ == "__main__":
    # Exemplo de uso
    produtos_especificos = [26173, 12345, 67890]
    for produto in produtos_especificos:
        predict_future_sales(produto)
