# src/visualizations/compare_predictions.py

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error
from src.utils.logging_config import get_logger
from src.models.train_model import N_STEPS

logger = get_logger(__name__)

# ===========================
# Hiperparâmetros e Configurações
# ===========================

# Período para comparação
FUTURE_START_DATE = '2024-01-01'
FUTURE_END_DATE = '2024-03-31'

# Caminhos para os diretórios de dados e modelos
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(SCRIPT_DIR)))
DATA_DIR = os.path.join(BASE_DIR, 'promopredictor', 'data')
PREDICTIONS_DIR = os.path.join(DATA_DIR, 'predictions')
PLOTS_DIR = os.path.join(BASE_DIR, 'promopredictor', 'plots')
os.makedirs(PLOTS_DIR, exist_ok=True)

# ===========================
# Função Principal
# ===========================

def compare_predictions(produto_especifico):
    logger.info(f"Comparando previsões com dados reais para o produto {produto_especifico}")

    # Carregar dados reais de transação e diário
    df_transacao_real = load_transaction_data(produto_especifico)
    df_diario_real = load_daily_data(produto_especifico)

    # Carregar previsões
    df_pred_unit_price = load_predictions(produto_especifico, tipo='unit_price')
    df_pred_quantity = load_predictions(produto_especifico, tipo='quantity')

    if df_pred_unit_price.empty or df_pred_quantity.empty:
        logger.error("Previsões não encontradas.")
        return

    # Comparar preço unitário
    compare_unit_price(df_transacao_real, df_pred_unit_price, produto_especifico)

    # Comparar quantidade vendida
    compare_quantity_sold(df_diario_real, df_pred_quantity, produto_especifico)

# ===========================
# Funções Auxiliares
# ===========================

def load_transaction_data(produto_especifico):
    data_path = os.path.join(DATA_DIR, f'dados_transacao_{produto_especifico}.csv')
    if not os.path.exists(data_path):
        logger.error(f"Dados de transação não encontrados em {data_path}")
        return pd.DataFrame()
    df = pd.read_csv(data_path, parse_dates=['Data', 'DataHora'])
    df = df[(df['Data'] >= FUTURE_START_DATE) & (df['Data'] <= FUTURE_END_DATE)]
    return df

def load_daily_data(produto_especifico):
    data_path = os.path.join(DATA_DIR, f'dados_agrupados_{produto_especifico}.csv')
    if not os.path.exists(data_path):
        logger.error(f"Dados diários não encontrados em {data_path}")
        return pd.DataFrame()
    df = pd.read_csv(data_path, parse_dates=['Data'])
    df = df[(df['Data'] >= FUTURE_START_DATE) & (df['Data'] <= FUTURE_END_DATE)]
    return df

def load_predictions(produto_especifico, tipo='unit_price'):
    if tipo == 'unit_price':
        predictions_path = os.path.join(PREDICTIONS_DIR, f'predictions_unit_price_{produto_especifico}.csv')
    else:
        predictions_path = os.path.join(PREDICTIONS_DIR, f'predictions_quantity_{produto_especifico}.csv')
    if not os.path.exists(predictions_path):
        logger.error(f"Previsões não encontradas em {predictions_path}")
        return pd.DataFrame()
    df = pd.read_csv(predictions_path, parse_dates=['Data'])
    return df

def compare_unit_price(df_real, df_pred, produto_especifico):
    logger.info("Comparando preço unitário")

    # Agrupar dados reais por data e calcular média do preço unitário
    df_real_grouped = df_real.groupby('Data').agg({'ValorUnitario': 'mean'}).reset_index()
    df_real_grouped.rename(columns={'ValorUnitario': 'ValorUnitarioReal'}, inplace=True)

    # Unir dados reais e previsões
    df_compare = pd.merge(df_pred, df_real_grouped, on='Data', how='inner')
    if df_compare.empty:
        logger.error("Nenhum dado para comparar preço unitário.")
        return

    # Calcular métricas
    mae = mean_absolute_error(df_compare['ValorUnitarioReal'], df_compare['ValorUnitarioPrevisto'])
    mse = mean_squared_error(df_compare['ValorUnitarioReal'], df_compare['ValorUnitarioPrevisto'])
    rmse = np.sqrt(mse)
    logger.info(f"MAE Preço Unitário: {mae}")
    logger.info(f"RMSE Preço Unitário: {rmse}")

    # Plotar gráfico
    plt.figure(figsize=(12, 6))
    plt.plot(df_compare['Data'], df_compare['ValorUnitarioReal'], label='Valor Unitário Real')
    plt.plot(df_compare['Data'], df_compare['ValorUnitarioPrevisto'], label='Valor Unitário Previsto')
    plt.title(f'Preço Unitário - Real vs Previsto - Produto {produto_especifico}')
    plt.xlabel('Data')
    plt.ylabel('Valor Unitário')
    plt.legend()
    plt.tight_layout()
    plot_path = os.path.join(PLOTS_DIR, f'compare_unit_price_{produto_especifico}.png')
    plt.savefig(plot_path)
    plt.close()
    logger.info(f"Gráfico de comparação de preço unitário salvo em {plot_path}")

def compare_quantity_sold(df_real, df_pred, produto_especifico):
    logger.info("Comparando quantidade vendida")

    # Unir dados reais e previsões
    df_compare = pd.merge(df_pred, df_real[['Data', 'QuantidadeLiquida']], on='Data', how='inner')
    if df_compare.empty:
        logger.error("Nenhum dado para comparar quantidade vendida.")
        return

    # Calcular métricas
    mae = mean_absolute_error(df_compare['QuantidadeLiquida'], df_compare['QuantidadePrevista'])
    mse = mean_squared_error(df_compare['QuantidadeLiquida'], df_compare['QuantidadePrevista'])
    rmse = np.sqrt(mse)
    logger.info(f"MAE Quantidade Vendida: {mae}")
    logger.info(f"RMSE Quantidade Vendida: {rmse}")

    # Plotar gráfico
    plt.figure(figsize=(12, 6))
    plt.plot(df_compare['Data'], df_compare['QuantidadeLiquida'], label='Quantidade Real')
    plt.plot(df_compare['Data'], df_compare['QuantidadePrevista'], label='Quantidade Prevista')
    plt.title(f'Quantidade Vendida - Real vs Previsto - Produto {produto_especifico}')
    plt.xlabel('Data')
    plt.ylabel('Quantidade Vendida')
    plt.legend()
    plt.tight_layout()
    plot_path = os.path.join(PLOTS_DIR, f'compare_quantity_{produto_especifico}.png')
    plt.savefig(plot_path)
    plt.close()
    logger.info(f"Gráfico de comparação de quantidade vendida salvo em {plot_path}")

# ===========================
# Execução Principal
# ===========================

if __name__ == "__main__":
    produto_especifico = 26173  # Substitua pelo código do produto desejado
    compare_predictions(produto_especifico)
