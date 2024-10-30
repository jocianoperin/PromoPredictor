# src/models/predict_sales.py

import pandas as pd
import numpy as np
import os
import joblib
from tensorflow.keras.models import load_model
from src.utils.logging_config import get_logger
import holidays

logger = get_logger(__name__)

# Definir script_dir no início do script
script_dir = os.path.dirname(os.path.abspath(__file__))

def fetch_data_for_prediction(produto_especifico):
    data_path = os.path.join(script_dir, '..', '..', 'data', 'dados_processados.csv')
    data_path = os.path.abspath(data_path)
    df = pd.read_csv(data_path, parse_dates=['Data'])
    df_produto = df[df['CodigoProduto'] == produto_especifico]
    return df_produto

def preprocess_data(df, scaler):
    try:
        df['Data'] = pd.to_datetime(df['Data'])
        df = df.sort_values(by=['Data'])

        # Features de tempo
        df['DiaDaSemana'] = df['Data'].dt.dayofweek
        df['Mes'] = df['Data'].dt.month
        df['Dia'] = df['Data'].dt.day
        df['ValorUnitario'] = df['ValorTotal'] / df['QuantidadeLiquida']
        df['ValorUnitario'].fillna(0, inplace=True)

        # Selecionar as colunas para o modelo
        features = ['EmPromocao', 'DiaDaSemana', 'Mes', 'Dia', 'Feriado']
        X = df[features]

        # Escalar os dados usando o scaler ajustado
        data_scaled = scaler.transform(X)

        return data_scaled
    except Exception as e:
        logger.error(f"Erro no pré-processamento dos dados: {e}")
        return None

def predict_future_sales(produto_especifico):
    logger.info("Iniciando o processo de previsão...")

    df = fetch_data_for_prediction(produto_especifico)
    if df is None or df.empty:
        logger.error("Nenhum dado retornado para previsão. Abortando o processo.")
        return

    try:
        models_dir = os.path.abspath(os.path.join(script_dir, '..', '..', 'trained_models'))

        # Carregar os modelos treinados e o scaler
        model_un_path = os.path.join(models_dir, f'model_un_{produto_especifico}.h5')
        model_valor_path = os.path.join(models_dir, f'model_valor_{produto_especifico}.h5')
        scaler_path = os.path.join(models_dir, f'scaler_{produto_especifico}.pkl')

        if not os.path.exists(model_un_path) or not os.path.exists(model_valor_path) or not os.path.exists(scaler_path):
            logger.error("Modelos ou scaler não encontrados.")
            return

        model_un = load_model(model_un_path, compile=False)
        model_un.compile(optimizer='adam', loss='mse')

        model_valor = load_model(model_valor_path, compile=False)
        model_valor.compile(optimizer='adam', loss='mse')

        scaler = joblib.load(scaler_path)

        # Pré-processar os dados históricos
        data_scaled = preprocess_data(df, scaler)
        if data_scaled is None or len(data_scaled) == 0:
            logger.error("Erro no pré-processamento dos dados para previsão.")
            return

        # Preparar dados futuros para previsão
        future_dates = pd.date_range(start='2024-01-01', end='2024-03-31', freq='D')
        df_future = pd.DataFrame({'Data': future_dates})
        df_future['EmPromocao'] = 0  # Ajuste conforme necessário

        df_future['DiaDaSemana'] = df_future['Data'].dt.dayofweek
        df_future['Mes'] = df_future['Data'].dt.month
        df_future['Dia'] = df_future['Data'].dt.day
        br_holidays = holidays.Brazil()
        df_future['Feriado'] = df_future['Data'].apply(lambda x: 1 if x in br_holidays else 0)

        # Selecionar colunas
        features = ['EmPromocao', 'DiaDaSemana', 'Mes', 'Dia', 'Feriado']
        X_future = df_future[features]
        X_future_scaled = scaler.transform(X_future)

        # Utilizar os últimos 'n_steps' dias dos dados históricos para iniciar as previsões
        n_steps = 30
        X_history = data_scaled[-n_steps:]

        predictions_un = []
        predictions_valor = []

        for i in range(len(X_future_scaled)):
            if i == 0:
                X_input = np.concatenate([X_history, X_future_scaled[:i+1]], axis=0)
            else:
                X_input = np.concatenate([X_history, X_future_scaled[:i+1]], axis=0)

            X_input_seq = []
            for j in range(len(X_input) - n_steps + 1):
                X_input_seq.append(X_input[j:j + n_steps])

            X_input_seq = np.array(X_input_seq)

            # Previsão de unidades vendidas
            y_pred_un = model_un.predict(X_input_seq)
            predictions_un.append(y_pred_un[-1][0])

            # Previsão de valor unitário
            y_pred_valor = model_valor.predict(X_input_seq)
            predictions_valor.append(y_pred_valor[-1][0])

        # Preparar DataFrame para as previsões
        df_predictions = pd.DataFrame({
            'Data': df_future['Data'],
            'ValorUnitarioPrevisto': predictions_valor,
            'QuantidadeTotalPrevista': predictions_un
        })

        # Salvar previsões
        predictions_dir = os.path.join(script_dir, '..', '..', 'data', 'predictions')
        os.makedirs(predictions_dir, exist_ok=True)
        predictions_path = os.path.join(predictions_dir, f'predictions_{produto_especifico}.csv')
        df_predictions.to_csv(predictions_path, index=False)

        logger.info("Previsões realizadas e salvas com sucesso.")

    except Exception as e:
        logger.error(f"Erro durante a previsão: {e}")

if __name__ == "__main__":
    produto_especifico = 26173  # Substitua pelo código do produto desejado
    predict_future_sales(produto_especifico)
