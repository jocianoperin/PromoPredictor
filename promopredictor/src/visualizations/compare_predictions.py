# src/visualizations/compare_predictions.py

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from tensorflow.keras.models import load_model
import joblib
from src.utils.logging_config import get_logger
from sklearn.metrics import mean_absolute_error, mean_squared_error

logger = get_logger(__name__)

def compare_predictions(produto_especifico):
    # Diretório base
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Carregar dados reais para 2023
    data_path = os.path.join(script_dir, '..', '..', 'data', 'dados_processados.csv')
    data_path = os.path.abspath(data_path)
    logger.info(f"Carregando dados reais de: {data_path}")

    if not os.path.exists(data_path):
        logger.error(f"Arquivo de dados não encontrado em: {data_path}")
        return

    df = pd.read_csv(data_path, parse_dates=['Data'])
    df = df[df['CodigoProduto'] == produto_especifico]
    df['Data'] = pd.to_datetime(df['Data'])
    df = df[(df['Data'] >= '2023-01-01') & (df['Data'] <= '2023-12-31')].copy()
    df = df.sort_values('Data').reset_index(drop=True)

    if df.empty:
        logger.error('Nenhum dado real disponível para 2023.')
        return

    # Preparar os dados
    df['DiaDaSemana'] = df['Data'].dt.dayofweek
    df['Mes'] = df['Data'].dt.month
    df['Dia'] = df['Data'].dt.day
    df['ValorUnitario'] = df['ValorTotal'] / df['QuantidadeLiquida']
    df['ValorUnitario'] = df['ValorUnitario'].replace([np.inf, -np.inf], np.nan).fillna(0)
    df['Feriado'] = df['Feriado'].fillna(0)
    df['VésperaDeFeriado'] = df['VésperaDeFeriado'].fillna(0)

    # Agrupar por dia
    df_daily = df.groupby('Data').agg({
        'EmPromocao': 'sum',  # ou 'mean', dependendo da lógica
        'DiaDaSemana': 'first',
        'Mes': 'first',
        'Dia': 'first',
        'Feriado': 'first',
        'VésperaDeFeriado': 'first',
        'QuantidadeLiquida': 'sum',
        'ValorUnitario': 'mean'  # Média diária
    }).reset_index()

    features = ['EmPromocao', 'DiaDaSemana', 'Mes', 'Dia', 'Feriado', 'VésperaDeFeriado']
    X_real = df_daily[features]

    # Carregar scaler e modelos
    models_dir = os.path.abspath(os.path.join(script_dir, '..', '..', 'trained_models'))
    scaler_path = os.path.join(models_dir, f'scaler_{produto_especifico}.pkl')
    model_un_path = os.path.join(models_dir, f'model_un_{produto_especifico}.h5')
    model_valor_path = os.path.join(models_dir, f'model_valor_{produto_especifico}.h5')

    if not os.path.exists(scaler_path) or not os.path.exists(model_un_path) or not os.path.exists(model_valor_path):
        logger.error('Modelos ou scaler não encontrados.')
        return

    scaler = joblib.load(scaler_path)
    model_un = load_model(model_un_path, compile=False)
    model_valor = load_model(model_valor_path, compile=False)

    # Escalar os dados
    try:
        X_scaled = scaler.transform(X_real)
    except Exception as e:
        logger.error(f"Erro ao escalar os dados: {e}")
        return

    # Criar sequências
    n_steps = 30  # Certifique-se de que este valor é o mesmo utilizado durante o treinamento
    X_sequences = []
    for i in range(len(X_scaled) - n_steps):
        X_sequences.append(X_scaled[i:i+n_steps])
    X_sequences = np.array(X_sequences)

    if len(X_sequences) == 0:
        logger.error("Nenhuma sequência criada. Verifique o valor de n_steps e o tamanho dos dados.")
        return

    # Prever valores
    y_pred_un = model_un.predict(X_sequences).flatten()
    y_pred_valor = model_valor.predict(X_sequences).flatten()

    # Ajustar os dados reais para alinhamento
    y_real_un = df_daily['QuantidadeLiquida'].values[n_steps:]
    y_real_valor = df_daily['ValorUnitario'].values[n_steps:]
    dates = df_daily['Data'].values[n_steps:]

    if len(y_real_un) != len(y_pred_un) or len(y_real_valor) != len(y_pred_valor):
        logger.error("Desalinhamento entre previsões e dados reais.")
        return

    # Calcular métricas
    mae_un = mean_absolute_error(y_real_un, y_pred_un)
    mae_valor = mean_absolute_error(y_real_valor, y_pred_valor)

    logger.info(f'MAE Quantidade de Unidades Vendidas: {mae_un}')
    logger.info(f'MAE Valor Unitário: {mae_valor}')

    # Diretório para salvar os gráficos
    plots_dir = os.path.join(script_dir, '..', '..', 'plots')
    os.makedirs(plots_dir, exist_ok=True)

    # Plotar e salvar Quantidade de Unidades Vendidas
    plt.figure(figsize=(15,5))
    plt.plot(dates, y_real_un, label='Real')
    plt.plot(dates, y_pred_un, label='Previsto')
    plt.title('Quantidade de Unidades Vendidas - Real vs Previsto (2023)')
    plt.xlabel('Data')
    plt.ylabel('Quantidade de Unidades Vendidas')
    plt.legend()
    plt.tight_layout()
    plot_path_un = os.path.join(plots_dir, f'quantidade_unidades_vendidas_{produto_especifico}.png')
    plt.savefig(plot_path_un)
    plt.close()

    # Plotar e salvar Valor Unitário
    plt.figure(figsize=(15,5))
    plt.plot(dates, y_real_valor, label='Real')
    plt.plot(dates, y_pred_valor, label='Previsto')
    plt.title('Valor Unitário - Real vs Previsto (2023)')
    plt.xlabel('Data')
    plt.ylabel('Valor Unitário')
    plt.legend()
    plt.tight_layout()
    plot_path_valor = os.path.join(plots_dir, f'valor_unitario_{produto_especifico}.png')
    plt.savefig(plot_path_valor)
    plt.close()

    logger.info("Gráficos salvos com sucesso.")

if __name__ == "__main__":
    produto_especifico = 26173  # Substitua pelo código do produto desejado
    compare_predictions(produto_especifico)
