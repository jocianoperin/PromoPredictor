# src/visualizations/compare_predictions.py

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# ===========================
# Hiperparâmetros e Configurações
# ===========================

# Período para comparação - Ajustado para 2024
FUTURE_START_DATE = '2024-01-01'  # Ajustado para iniciar em 01/01/2024
FUTURE_END_DATE = '2024-03-31'    # Mantido até 31/03/2024

# Caminhos para os diretórios de dados e modelos
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(SCRIPT_DIR)))
DATA_DIR = os.path.join(BASE_DIR, 'promopredictor', 'data')
PREDICTIONS_DIR = os.path.join(DATA_DIR, 'predictions')
PLOTS_DIR = os.path.join(BASE_DIR, 'promopredictor', 'plots')
COMPARISON_DIR = os.path.join(BASE_DIR, 'promopredictor', 'comparisons')
os.makedirs(PLOTS_DIR, exist_ok=True)
os.makedirs(COMPARISON_DIR, exist_ok=True)

logger.info(f"PLOTS_DIR está definido como: {PLOTS_DIR}")
logger.info(f"PREDICTIONS_DIR está definido como: {PREDICTIONS_DIR}")
logger.info(f"COMPARISON_DIR está definido como: {COMPARISON_DIR}")

# ===========================
# Função Principal
# ===========================

def compare_predictions(produto_especifico):
    logger.info(f"Comparando previsões com valores reais para o produto {produto_especifico}")

    # Caminhos para os arquivos de previsão e dados reais
    predictions_path_unit_price = os.path.join('promopredictor', 'data', 'predictions', f'predictions_unit_price_{produto_especifico}.csv')
    predictions_path_quantity = os.path.join('promopredictor', 'data', 'predictions', f'predictions_quantity_{produto_especifico}.csv')
    real_data_path = os.path.join('promopredictor', 'data', f'dados_agrupados_{produto_especifico}.csv')

    if not os.path.exists(predictions_path_unit_price) or not os.path.exists(predictions_path_quantity) or not os.path.exists(real_data_path):
        logger.error("Arquivos de previsão ou dados reais não encontrados.")
        return

    # Carregar dados
    df_pred_unit = pd.read_csv(predictions_path_unit_price, parse_dates=['Data'])
    df_pred_quantity = pd.read_csv(predictions_path_quantity, parse_dates=['Data'])
    df_real = pd.read_csv(real_data_path, parse_dates=['Data'])

    # Merge das previsões com os dados reais
    df_compare_unit_price = pd.merge(df_pred_unit, df_real, on='Data', how='inner')
    df_compare_quantity = pd.merge(df_pred_quantity, df_real, on='Data', how='inner')

    if df_compare_unit_price.empty or df_compare_quantity.empty:
        logger.error("Dados de comparação vazios após o merge.")
        return

    # Plotar comparações
    plt.figure(figsize=(14, 7))

    # Preço Unitário
    plt.subplot(2, 1, 1)
    plt.plot(df_compare_unit_price['Data'], df_compare_unit_price['ValorUnitario'], label='ValorUnitario Real')
    plt.plot(df_compare_unit_price['Data'], df_compare_unit_price['ValorUnitarioPrevisto'], label='ValorUnitario Previsto')
    plt.title(f'Comparação do Valor Unitário - Produto {produto_especifico}')
    plt.xlabel('Data')
    plt.ylabel('Valor Unitário')
    plt.legend()

    # Quantidade Vendida
    plt.subplot(2, 1, 2)
    plt.plot(df_compare_quantity['Data'], df_compare_quantity['QuantidadeLiquida'], label='QuantidadeVendida Real')
    plt.plot(df_compare_quantity['Data'], df_compare_quantity['QuantidadePrevista'], label='QuantidadeVendida Prevista')
    plt.title(f'Comparação da Quantidade Vendida - Produto {produto_especifico}')
    plt.xlabel('Data')
    plt.ylabel('Quantidade Vendida')
    plt.legend()

    # Salvar o gráfico
    plot_path = os.path.join('promopredictor', 'plots', f'comparison_{produto_especifico}.png')
    plt.tight_layout()
    plt.savefig(plot_path)
    plt.close()
    logger.info(f"Gráfico de comparação salvo em {plot_path}")

# ===========================
# Funções Auxiliares
# ===========================

def load_transaction_data(produto_especifico):
    data_path = os.path.join(DATA_DIR, f'dados_transacao_{produto_especifico}.csv')
    if not os.path.exists(data_path):
        logger.error(f"Dados de transação não encontrados em {data_path}")
        return pd.DataFrame()
    df = pd.read_csv(data_path, parse_dates=['Data', 'DataHora'])
    # Filtrar pelo período de comparação
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
    # Filtrar pelo período de comparação
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
        return None

    if df_pred.empty:
        logger.error("Dados previstos de preço unitário estão vazios.")
        return None

    # Agrupar dados reais por data e calcular média do preço unitário
    df_real_grouped = df_real.groupby('Data').agg({'ValorUnitario': 'mean'}).reset_index()
    df_real_grouped.rename(columns={'ValorUnitario': 'ValorUnitarioReal'}, inplace=True)

    # Unir dados reais e previsões
    df_compare = pd.merge(df_pred, df_real_grouped, on='Data', how='inner')
    if df_compare.empty:
        logger.error("Nenhum dado para comparar preço unitário após a mesclagem.")
        return None

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

    # Retornar DataFrame para combinar na comparação final
    return df_compare[['Data', 'CodigoProduto', 'ValorUnitarioReal', 'ValorUnitarioPrevisto']]

def compare_quantity_sold(df_real, df_pred, produto_especifico):
    logger.info("Comparando quantidade vendida")

    if df_real.empty:
        logger.error("Dados reais de quantidade vendida estão vazios.")
        return None

    if df_pred.empty:
        logger.error("Dados previstos de quantidade vendida estão vazios.")
        return None

    # Unir dados reais e previsões
    df_compare = pd.merge(df_pred, df_real[['Data', 'QuantidadeLiquida']], on='Data', how='inner')
    if df_compare.empty:
        logger.error("Nenhum dado para comparar quantidade vendida após a mesclagem.")
        return None

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

    # Retornar DataFrame para combinar na comparação final
    return df_compare[['Data', 'CodigoProduto', 'QuantidadeLiquida', 'QuantidadePrevista']]

# ===========================
# Execução Principal
# ===========================

if __name__ == "__main__":
    produto_especifico = 26173  # Substitua pelo código do produto desejado
    compare_predictions(produto_especifico)
