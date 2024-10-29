# src/visualizations/general_forecast_analysis.py

import pandas as pd
import matplotlib.pyplot as plt
import os
from src.services.database import db_manager
from src.utils.logging_config import get_logger
from sklearn.metrics import mean_absolute_error

logger = get_logger(__name__)

def fetch_actual_and_predicted_data(produto_especifico):
    """
    Busca os dados reais e previstos para análise comparativa.
    """
    try:
        # Buscar dados reais
        query_real = f"""
            SELECT DATA, TotalUNVendidas, ValorTotalVendido
            FROM indicadores_vendas_produtos_resumo
            WHERE CodigoProduto = '{produto_especifico}'
              AND DATA BETWEEN '2024-01-01' AND '2024-03-31'
            ORDER BY DATA;
        """
        result_real = db_manager.execute_query(query_real)
        df_real = pd.DataFrame(result_real['data'], columns=result_real['columns'])
        df_real['DATA'] = pd.to_datetime(df_real['DATA'])
        df_real['TotalUNVendidas'] = df_real['TotalUNVendidas'].astype(float)
        df_real['ValorTotalVendido'] = df_real['ValorTotalVendido'].astype(float)

        # Buscar dados previstos
        query_pred = f"""
            SELECT DATA, TotalUNVendidas AS TotalUNVendidas_pred, ValorTotalVendido AS ValorTotalVendido_pred
            FROM indicadores_vendas_produtos_previsoes
            WHERE CodigoProduto = '{produto_especifico}'
            ORDER BY DATA;
        """
        result_pred = db_manager.execute_query(query_pred)
        df_pred = pd.DataFrame(result_pred['data'], columns=result_pred['columns'])
        df_pred['DATA'] = pd.to_datetime(df_pred['DATA'])
        df_pred['TotalUNVendidas_pred'] = df_pred['TotalUNVendidas_pred'].astype(float)
        df_pred['ValorTotalVendido_pred'] = df_pred['ValorTotalVendido_pred'].astype(float)

        return df_real, df_pred
    except Exception as e:
        logger.error(f"Erro ao buscar dados para análise: {e}")
        return None, None

def plot_forecast_analysis(produto_especifico):
    """
    Plota a comparação entre os valores reais e previstos das vendas.
    """
    df_real, df_pred = fetch_actual_and_predicted_data(produto_especifico)
    if df_real is None or df_pred is None:
        logger.error("Dados insuficientes para plotagem.")
        return

    try:
        # Combinar os dados reais e previstos
        df_combined = pd.merge(df_real, df_pred, on='DATA', how='inner')
        df_combined.sort_values(by='DATA', inplace=True)

        # Verificar se há dados para avaliar
        if df_combined.empty:
            logger.warning("Não há dados sobrepostos para avaliação.")
            return

        # Calcular o MAE nos dados onde temos ambos real e previsto
        mae_un = mean_absolute_error(df_combined['TotalUNVendidas'], df_combined['TotalUNVendidas_pred'])
        mae_valor = mean_absolute_error(df_combined['ValorTotalVendido'], df_combined['ValorTotalVendido_pred'])
        logger.info(f"MAE entre valores reais e previstos - TotalUNVendidas: {mae_un}")
        logger.info(f"MAE entre valores reais e previstos - ValorTotalVendido: {mae_valor}")

        # Plotar Unidades Vendidas
        plt.figure(figsize=(12, 6))
        plt.plot(df_combined['DATA'], df_combined['TotalUNVendidas'], label='Unidades Reais', marker='o')
        plt.plot(df_combined['DATA'], df_combined['TotalUNVendidas_pred'], label='Unidades Previstas', marker='x')
        plt.xlabel('Data')
        plt.ylabel('Total de Unidades Vendidas')
        plt.title(f'Análise de Previsão de Unidades Vendidas para o Produto {produto_especifico}')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.show()

        # Plotar Valor Total Vendido
        plt.figure(figsize=(12, 6))
        plt.plot(df_combined['DATA'], df_combined['ValorTotalVendido'], label='Valor Real', marker='o')
        plt.plot(df_combined['DATA'], df_combined['ValorTotalVendido_pred'], label='Valor Previsto', marker='x')
        plt.xlabel('Data')
        plt.ylabel('Valor Total Vendido')
        plt.title(f'Análise de Previsão de Valor Vendido para o Produto {produto_especifico}')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.show()

    except Exception as e:
        logger.error(f"Erro ao plotar a análise de previsão: {e}")

if __name__ == "__main__":
    produto_especifico = 'codigo_do_produto_desejado'  # Substitua pelo código do produto que deseja analisar
    plot_forecast_analysis(produto_especifico)
