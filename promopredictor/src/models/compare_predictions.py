import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def fetch_predictions():
    """
    Busca as previsões de vendas no período de 01/01/2024 a 30/03/2024
    da tabela 'indicadores_vendas_produtos_previsoes'.
    """
    query = """
        SELECT DATA, CodigoProduto, TotalUNVendidas AS TotalUNVendidas_Previsto,
               ValorTotalVendido AS ValorTotalVendido_Previsto, Promocao AS Promocao_Previsto
        FROM indicadores_vendas_produtos_previsoes
        WHERE DATA BETWEEN '2024-01-01' AND '2024-03-30';
    """
    try:
        result = db_manager.execute_query(query)
        if result and 'data' in result and 'columns' in result:
            df_pred = pd.DataFrame(result['data'], columns=result['columns'])
            logger.info(f"Número de registros de previsões obtidos: {len(df_pred)}")
            return df_pred
        else:
            logger.warning("Nenhum dado de previsões foi retornado pela query.")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Erro ao buscar previsões: {e}")
        return pd.DataFrame()

def fetch_actuals():
    """
    Busca os valores reais de vendas no período de 01/01/2024 a 30/03/2024
    da tabela 'indicadores_vendas_produtos_resumo'.
    """
    query = """
        SELECT DATA, CodigoProduto, TotalUNVendidas AS TotalUNVendidas_Real,
               ValorTotalVendido AS ValorTotalVendido_Real, Promocao AS Promocao_Real
        FROM indicadores_vendas_produtos_resumo
        WHERE DATA BETWEEN '2024-01-01' AND '2024-03-30';
    """
    try:
        result = db_manager.execute_query(query)
        if result and 'data' in result and 'columns' in result:
            df_actual = pd.DataFrame(result['data'], columns=result['columns'])
            logger.info(f"Número de registros reais obtidos: {len(df_actual)}")
            return df_actual
        else:
            logger.warning("Nenhum dado real foi retornado pela query.")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Erro ao buscar dados reais: {e}")
        return pd.DataFrame()

def compare_predictions():
    """
    Compara as previsões com os valores reais, calcula métricas de erro e gera gráficos.
    """
    logger.info("Iniciando comparação das previsões com os valores reais...")

    # Buscar previsões e valores reais
    df_pred = fetch_predictions()
    df_actual = fetch_actuals()

    if df_pred.empty or df_actual.empty:
        logger.error("Dados insuficientes para comparação. Abortando o processo.")
        return

    try:
        # Converter a coluna DATA para datetime
        df_pred['DATA'] = pd.to_datetime(df_pred['DATA'])
        df_actual['DATA'] = pd.to_datetime(df_actual['DATA'])

        # Juntar os DataFrames nas colunas DATA e CodigoProduto
        df_merged = pd.merge(df_pred, df_actual, on=['DATA', 'CodigoProduto'], how='inner')
        logger.info(f"Número de registros após junção: {len(df_merged)}")

        if df_merged.empty:
            logger.warning("Nenhum registro corresponde entre previsões e valores reais.")
            return

        # Calcular diferenças e erros
        df_merged['Erro_TotalUNVendidas'] = df_merged['TotalUNVendidas_Real'] - df_merged['TotalUNVendidas_Previsto']
        df_merged['Erro_ValorTotalVendido'] = df_merged['ValorTotalVendido_Real'] - df_merged['ValorTotalVendido_Previsto']

        # Calcular métricas de erro (MAE, MAPE)
        df_merged['AE_TotalUNVendidas'] = df_merged['Erro_TotalUNVendidas'].abs()
        df_merged['AE_ValorTotalVendido'] = df_merged['Erro_ValorTotalVendido'].abs()

        # Evitar divisão por zero no MAPE
        df_merged['APE_TotalUNVendidas'] = df_merged.apply(
            lambda row: (row['AE_TotalUNVendidas'] / row['TotalUNVendidas_Real']) * 100 if row['TotalUNVendidas_Real'] != 0 else 0,
            axis=1
        )
        df_merged['APE_ValorTotalVendido'] = df_merged.apply(
            lambda row: (row['AE_ValorTotalVendido'] / row['ValorTotalVendido_Real']) * 100 if row['ValorTotalVendido_Real'] != 0 else 0,
            axis=1
        )

        # Métricas agregadas
        mae_un = df_merged['AE_TotalUNVendidas'].mean()
        mae_valor = df_merged['AE_ValorTotalVendido'].mean()
        mape_un = df_merged['APE_TotalUNVendidas'].mean()
        mape_valor = df_merged['APE_ValorTotalVendido'].mean()

        logger.info(f"MAE TotalUNVendidas: {mae_un:.2f}")
        logger.info(f"MAE ValorTotalVendido: {mae_valor:.2f}")
        logger.info(f"MAPE TotalUNVendidas: {mape_un:.2f}%")
        logger.info(f"MAPE ValorTotalVendido: {mape_valor:.2f}%")

        # Gerar gráficos de comparação para alguns produtos selecionados
        generate_comparison_plots(df_merged)

        # Salvar o DataFrame com as comparações para análise adicional (opcional)
        output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../outputs'))
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        output_path = os.path.join(output_dir, 'comparacao_previsoes.xlsx')
        df_merged.to_excel(output_path, index=False)
        logger.info(f"Resultado da comparação salvo em: {output_path}")

    except Exception as e:
        logger.error(f"Erro ao comparar previsões com valores reais: {e}")

def generate_comparison_plots(df_merged):
    """
    Gera gráficos comparando as previsões com os valores reais para alguns produtos.
    """
    logger.info("Gerando gráficos de comparação...")
    try:
        # Selecionar produtos com base na frequência de vendas
        produtos_top = df_merged['CodigoProduto'].value_counts().head(5).index.tolist()

        output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../outputs'))
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        for produto in produtos_top:
            df_produto = df_merged[df_merged['CodigoProduto'] == produto]

            plt.figure(figsize=(12, 6))
            sns.lineplot(x='DATA', y='TotalUNVendidas_Real', data=df_produto, label='Real')
            sns.lineplot(x='DATA', y='TotalUNVendidas_Previsto', data=df_produto, label='Previsto')
            plt.title(f'Comparação de Vendas Totais para o Produto {produto}')
            plt.xlabel('Data')
            plt.ylabel('Total de Unidades Vendidas')
            plt.legend()
            plt.tight_layout()
            plot_path = os.path.join(output_dir, f'comparacao_totalunvendidas_produto_{produto}.png')
            plt.savefig(plot_path)
            plt.close()
            logger.info(f"Gráfico salvo em: {plot_path}")

            plt.figure(figsize=(12, 6))
            sns.lineplot(x='DATA', y='ValorTotalVendido_Real', data=df_produto, label='Real')
            sns.lineplot(x='DATA', y='ValorTotalVendido_Previsto', data=df_produto, label='Previsto')
            plt.title(f'Comparação de Valor Total Vendido para o Produto {produto}')
            plt.xlabel('Data')
            plt.ylabel('Valor Total Vendido')
            plt.legend()
            plt.tight_layout()
            plot_path = os.path.join(output_dir, f'comparacao_valortotalvendido_produto_{produto}.png')
            plt.savefig(plot_path)
            plt.close()
            logger.info(f"Gráfico salvo em: {plot_path}")

    except Exception as e:
        logger.error(f"Erro ao gerar gráficos de comparação: {e}")

if __name__ == "__main__":
    compare_predictions()
