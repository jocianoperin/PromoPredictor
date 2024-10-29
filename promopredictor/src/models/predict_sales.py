# src/models/predict_sales.py

import pandas as pd
import numpy as np
import os
import joblib
from tensorflow.keras.models import load_model
from src.services.database import db_manager
from src.utils.logging_config import get_logger
from src.utils.utils import clear_predictions_table, insert_predictions
from src.utils.data_preparation import create_sequences, scale_data
import holidays

logger = get_logger(__name__)

def fetch_data_for_prediction(produto_especifico):
    """
    Busca os dados históricos até 31/12/2023 para o produto específico.
    """
    query = f"""
        SELECT DATA, TotalUNVendidas, ValorTotalVendido, Promocao
        FROM indicadores_vendas_produtos_resumo
        WHERE CodigoProduto = '{produto_especifico}' AND DATA <= '2023-12-31'
        ORDER BY DATA;
    """
    try:
        result = db_manager.execute_query(query)
        if result and 'data' in result and 'columns' in result:
            df = pd.DataFrame(result['data'], columns=result['columns'])
            logger.info(f"Quantidade de registros retornados: {len(df)}")
            return df
        else:
            logger.warning("Nenhum dado foi retornado pela query.")
            return None
    except Exception as e:
        logger.error(f"Erro ao buscar dados para previsão: {e}")
        return None

def preprocess_data(df, scaler):
    """
    Pré-processa os dados para previsão, utilizando o scaler ajustado.
    """
    try:
        df['DATA'] = pd.to_datetime(df['DATA'])
        df = df.sort_values(by=['DATA'])

        # Converter 'Promocao' para float
        df['Promocao'] = df['Promocao'].astype(float)

        # Features de tempo
        df['dia_da_semana'] = df['DATA'].dt.dayofweek
        df['mes'] = df['DATA'].dt.month
        df['dia'] = df['DATA'].dt.day

        # Adicionar feature de feriado
        br_holidays = holidays.Brazil()
        df['feriado'] = df['DATA'].apply(lambda x: 1 if x in br_holidays else 0)

        # Remover colunas desnecessárias
        df.drop(columns=['DATA'], inplace=True)

        # Preencher valores ausentes com zero
        df.fillna(0, inplace=True)

        # Selecionar as colunas para o modelo
        cols = ['TotalUNVendidas', 'ValorTotalVendido', 'Promocao', 'dia_da_semana', 'mes', 'dia', 'feriado']
        df = df[cols]

        # Normalizar os dados usando o scaler ajustado
        data_scaled = scaler.transform(df)

        return data_scaled
    except Exception as e:
        logger.error(f"Erro no pré-processamento dos dados: {e}")
        return None

def predict_future_sales(produto_especifico):
    """
    Faz a previsão para o período futuro com o modelo LSTM treinado para o produto específico.
    """
    logger.info("Iniciando o processo de previsão...")

    df = fetch_data_for_prediction(produto_especifico)
    if df is None or df.empty:
        logger.error("Nenhum dado retornado para previsão. Abortando o processo.")
        return

    try:
        models_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../trained_models'))

        # Carregar os modelos treinados e o scaler
        model_un_path = os.path.join(models_dir, f'model_un_{produto_especifico}.h5')
        model_valor_path = os.path.join(models_dir, f'model_valor_{produto_especifico}.h5')
        scaler_path = os.path.join(models_dir, f'scaler_{produto_especifico}.pkl')

        if not os.path.exists(model_un_path) or not os.path.exists(model_valor_path) or not os.path.exists(scaler_path):
            logger.error("Modelos ou scaler não encontrados.")
            return

        model_un = load_model(model_un_path)
        model_valor = load_model(model_valor_path)
        scaler = joblib.load(scaler_path)

        # Pré-processar os dados históricos
        data_scaled = preprocess_data(df, scaler)
        if data_scaled is None or len(data_scaled) == 0:
            logger.error("Erro no pré-processamento dos dados para previsão.")
            return

        # Preparar dados futuros para previsão
        future_dates = pd.date_range(start='2024-01-01', end='2024-03-31', freq='D')
        df_future = pd.DataFrame({'DATA': future_dates})
        df_future['Promocao'] = 0  # Ajuste conforme necessário

        df_future['dia_da_semana'] = df_future['DATA'].dt.dayofweek
        df_future['mes'] = df_future['DATA'].dt.month
        df_future['dia'] = df_future['DATA'].dt.day
        br_holidays = holidays.Brazil()
        df_future['feriado'] = df_future['DATA'].apply(lambda x: 1 if x in br_holidays else 0)

        # Preencher valores ausentes com zero
        df_future.fillna(0, inplace=True)

        # Selecionar colunas
        cols = ['Promocao', 'dia_da_semana', 'mes', 'dia', 'feriado']

        # Utilizar os últimos 'seq_length' dias dos dados históricos para iniciar as previsões
        seq_length = 30
        data_history = data_scaled[-seq_length:]

        predictions_un = []
        predictions_valor = []

        # Definir os nomes das colunas explicitamente
        column_names = ['TotalUNVendidas', 'ValorTotalVendido', 'Promocao', 'dia_da_semana', 'mes', 'dia', 'feriado']

        # Inicializar seq_input com data_history
        seq_input = data_history.copy()

        for i in range(len(df_future)):
            # Adicionar as features do dia futuro
            future_row = df_future.iloc[i][cols].values.reshape(1, -1)
            # Preencher zeros para as colunas de target (TotalUNVendidas e ValorTotalVendido)
            zeros = np.zeros((1, 2))
            future_data = np.concatenate([zeros, future_row], axis=1)

            # Converter future_data para DataFrame com nomes de colunas
            future_data_df = pd.DataFrame(future_data, columns=column_names)

            # Escalar os dados futuros usando o scaler
            future_data_scaled = scaler.transform(future_data_df)

            # Atualizar a sequência de entrada com os dados futuros
            seq_input = np.concatenate([seq_input[1:], future_data_scaled], axis=0)

            # Ajustar a forma para (1, seq_length, data_scaled.shape[1])
            seq_input_reshaped = seq_input.reshape(1, seq_length, data_scaled.shape[1])

            # Fazer previsões
            y_pred_un = model_un.predict(seq_input_reshaped)
            y_pred_valor = model_valor.predict(seq_input_reshaped)

            # Guardar as previsões
            predictions_un.append(y_pred_un[0][0])
            predictions_valor.append(y_pred_valor[0][0])

            # Preparar os dados para a próxima iteração
            # Atualizar future_data_scaled com as previsões
            future_data_scaled[0, 0] = y_pred_un[0][0]  # TotalUNVendidas
            future_data_scaled[0, 1] = y_pred_valor[0][0]  # ValorTotalVendido

            # Atualizar seq_input[-1] com o future_data_scaled atualizado
            seq_input[-1] = future_data_scaled

        # Inverter a escala das previsões
        predictions = np.array([predictions_un, predictions_valor]).T
        zeros = np.zeros((predictions.shape[0], data_scaled.shape[1] - 2))
        predictions_full = np.concatenate([predictions, zeros], axis=1)
        predictions_rescaled = scaler.inverse_transform(predictions_full)

        # Preparar DataFrame para inserção
        df_predictions = pd.DataFrame({
            'DATA': df_future['DATA'],
            'CodigoProduto': produto_especifico,
            'TotalUNVendidas': predictions_rescaled[:, 0].clip(0).round().astype(int),
            'ValorTotalVendido': predictions_rescaled[:, 1].clip(0).round(2),
            'Promocao': df_future['Promocao']
        })

        # Inserir previsões no banco de dados
        clear_predictions_table()
        insert_predictions(df_predictions)

        logger.info("Previsões realizadas e inseridas com sucesso.")

    except Exception as e:
        logger.error(f"Erro durante a previsão: {e}")

if __name__ == "__main__":
    produto_especifico = 'codigo_do_produto_desejado'  # Substitua pelo código do produto desejado
    predict_future_sales(produto_especifico)
