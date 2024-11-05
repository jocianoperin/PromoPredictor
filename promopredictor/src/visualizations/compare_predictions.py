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

# Período para comparação - Ajuste conforme seus dados reais
FUTURE_START_DATE = '2023-01-01'  # Altere para uma data apropriada
FUTURE_END_DATE = '2023-12-31'    # Altere para uma data apropriada

# Caminhos para os diretórios de dados e modelos
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(SCRIPT_DIR)))
DATA_DIR = os.path.join(BASE_DIR, 'promopredictor', 'data')
PREDICTIONS_DIR = os.path.join(DATA_DIR, 'predictions')
PLOTS_DIR = os.path.join(BASE_DIR, 'promopredictor', 'plots')
os.makedirs(PLOTS_DIR, exist_ok=True)

logger.info(f"PLOTS_DIR está definido como: {PLOTS_DIR}")
logger.info(f"PREDICTIONS_DIR está definido como: {PREDICTIONS_DIR}")

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
        logger.error("Previsões não encontradas ou estão vazias.")
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
    if df.empty:
        logger.warning("Dados de transação estão vazios no período especificado.")
    return df

def load_daily_data(produto_especifico):
    data_path = os.path.join(DATA_DIR, f'dados_agrupados_{produto_especifico}.csv')
    if not os.path.exists(data_path):
        logger.error(f"Dados diários não encontrados em {data_path}")
        return pd.DataFrame()
    df = pd.read_csv(data_path, parse_dates=['Data'])
    df = df[(df['Data'] >= FUTURE_START_DATE) & (df['Data'] <= FUTURE_END_DATE)]
    if df.empty:
        logger.warning("Dados diários estão vazios no período especificado.")
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
    if df.empty:
        logger.warning(f"Dados de previsão para {tipo} estão vazios.")
    return df

def compare_unit_price(df_real, df_pred, produto_especifico):
    logger.info("Comparando preço unitário")

    if df_real.empty:
        logger.error("Dados reais de preço unitário estão vazios.")
        return

    if df_pred.empty:
        logger.error("Dados previstos de preço unitário estão vazios.")
        return

    # Agrupar dados reais por data e calcular média do preço unitário
    df_real_grouped = df_real.groupby('Data').agg({'ValorUnitario': 'mean'}).reset_index()
    df_real_grouped.rename(columns={'ValorUnitario': 'ValorUnitarioReal'}, inplace=True)

    # Unir dados reais e previsões
    df_compare = pd.merge(df_pred, df_real_grouped, on='Data', how='inner')
    if df_compare.empty:
        logger.error("Nenhum dado para comparar preço unitário após a mesclagem.")
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
    try:
        plt.savefig(plot_path)
        logger.info(f"Gráfico de comparação de preço unitário salvo em {plot_path}")
    except Exception as e:
        logger.error(f"Erro ao salvar o gráfico de preço unitário: {e}")
    plt.close()

def compare_quantity_sold(df_real, df_pred, produto_especifico):
    logger.info("Comparando quantidade vendida")

    if df_real.empty:
        logger.error("Dados reais de quantidade vendida estão vazios.")
        return

    if df_pred.empty:
        logger.error("Dados previstos de quantidade vendida estão vazios.")
        return

    # Unir dados reais e previsões
    df_compare = pd.merge(df_pred, df_real[['Data', 'QuantidadeLiquida']], on='Data', how='inner')
    if df_compare.empty:
        logger.error("Nenhum dado para comparar quantidade vendida após a mesclagem.")
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
    try:
        plt.savefig(plot_path)
        logger.info(f"Gráfico de comparação de quantidade vendida salvo em {plot_path}")
    except Exception as e:
        logger.error(f"Erro ao salvar o gráfico de quantidade vendida: {e}")
    plt.close()

# ===========================
# Execução Principal
# ===========================

if __name__ == "__main__":
    produto_especifico = 26173  # Substitua pelo código do produto desejado
    compare_predictions(produto_especifico)
