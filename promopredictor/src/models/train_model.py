# src/models/train_model.py

import pandas as pd
import os
import numpy as np
import joblib
import holidays
from src.services.database import db_manager
from src.utils.logging_config import get_logger
from src.utils.data_preparation import create_sequences, scale_data
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, LSTM
from tensorflow.keras.callbacks import EarlyStopping

logger = get_logger(__name__)

def fetch_data_for_training(start_date, end_date, produto_especifico):
    """
    Busca dados da tabela indicadores_vendas_produtos_resumo para o período especificado e produto específico.
    """
    query = f"""
        SELECT DATA, TotalUNVendidas, ValorTotalVendido, Promocao
        FROM indicadores_vendas_produtos_resumo
        WHERE DATA BETWEEN '{start_date}' AND '{end_date}'
          AND CodigoProduto = '{produto_especifico}';
    """
    try:
        result = db_manager.execute_query(query)
        if 'data' in result and 'columns' in result:
            logger.info(f"Quantidade de registros retornados: {len(result['data'])}")
            df = pd.DataFrame(result['data'], columns=result['columns'])
            return df
        else:
            logger.warning("Nenhum dado foi retornado pela query.")
            return None
    except Exception as e:
        logger.error(f"Erro ao buscar dados: {e}")
        return None

def preprocess_data(df):
    """
    Pré-processa os dados para o treinamento do modelo LSTM.
    """
    try:
        logger.info("Iniciando o pré-processamento dos dados...")
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

        # Normalizar os dados
        data_scaled, scaler = scale_data(df)

        logger.info("Pré-processamento concluído.")
        return data_scaled, scaler
    except Exception as e:
        logger.error(f"Erro durante o pré-processamento: {e}")
        return None, None

def train_and_evaluate_model(produto_especifico):
    """
    Realiza o treinamento e avaliação do modelo LSTM para um produto específico.
    """
    logger.info("Iniciando o processo de treinamento do modelo LSTM...")

    df = fetch_data_for_training('2019-01-01', '2023-12-31', produto_especifico)

    if df is not None and not df.empty:
        # Pré-processar os dados
        data_scaled, scaler = preprocess_data(df)
        if data_scaled is None:
            logger.error("Erro no pré-processamento dos dados.")
            return

        # Criar sequências
        seq_length = 30  # Usar 30 dias de histórico para prever o próximo dia
        target_cols = ['TotalUNVendidas', 'ValorTotalVendido']
        X, y = create_sequences(data_scaled, seq_length, target_cols)

        # Dividir em treino e validação
        split = int(0.8 * len(X))
        X_train, X_val = X[:split], X[split:]
        y_train_un, y_val_un = y['TotalUNVendidas'][:split], y['TotalUNVendidas'][split:]
        y_train_valor, y_val_valor = y['ValorTotalVendido'][:split], y['ValorTotalVendido'][split:]

        # Construir o modelo LSTM para TotalUNVendidas
        model_un = Sequential()
        model_un.add(LSTM(50, activation='relu', input_shape=(X_train.shape[1], X_train.shape[2])))
        model_un.add(Dense(1))
        model_un.compile(optimizer='adam', loss='mean_absolute_error')

        # Treinar o modelo para TotalUNVendidas
        early_stop = EarlyStopping(monitor='val_loss', patience=5)
        model_un.fit(X_train, y_train_un, epochs=50, validation_data=(X_val, y_val_un), callbacks=[early_stop])

        # Construir o modelo LSTM para ValorTotalVendido
        model_valor = Sequential()
        model_valor.add(LSTM(50, activation='relu', input_shape=(X_train.shape[1], X_train.shape[2])))
        model_valor.add(Dense(1))
        model_valor.compile(optimizer='adam', loss='mean_absolute_error')

        # Treinar o modelo para ValorTotalVendido
        model_valor.fit(X_train, y_train_valor, epochs=50, validation_data=(X_val, y_val_valor), callbacks=[early_stop])

        # Salvar os modelos
        models_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../trained_models'))
        if not os.path.exists(models_dir):
            os.makedirs(models_dir)

        model_un.save(os.path.join(models_dir, f'model_un_{produto_especifico}.h5'))
        model_valor.save(os.path.join(models_dir, f'model_valor_{produto_especifico}.h5'))
        logger.info("Modelos salvos com sucesso.")

        # Salvar o scaler
        scaler_path = os.path.join(models_dir, f'scaler_{produto_especifico}.pkl')
        joblib.dump(scaler, scaler_path)
        logger.info(f"Scaler salvo em: {scaler_path}")

    else:
        logger.error("Não foi possível treinar o modelo devido à ausência de dados.")

if __name__ == "__main__":
    produto_especifico = 'codigo_do_produto_desejado'  # Substitua pelo código do produto desejado
    train_and_evaluate_model(produto_especifico)
