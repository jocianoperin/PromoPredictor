import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from ..services.database import db_manager

def calculate_metrics(df):
    """
    Calcula métricas como MAPE e RMSE para avaliar a precisão das previsões.
    Lida com casos onde o valor real é zero para evitar divisões por zero.
    """
    # Inicializar a coluna PercentageError com NaN
    df['PercentageError'] = np.nan
    
    # Filtrar registros onde o valor realizado não é zero
    df_non_zero = df[df['UNRealizada'] != 0].copy()
    
    if not df_non_zero.empty:
        # Calcular o erro percentual apenas para os registros onde o valor realizado não é zero
        df_non_zero['PercentageError'] = np.abs((df_non_zero['UNRealizada'] - df_non_zero['UNPrevista']) / df_non_zero['UNRealizada']) * 100
        mape = df_non_zero['PercentageError'].mean()  # Mean Absolute Percentage Error
    else:
        mape = np.nan  # Ou outro valor indicativo de que o MAPE não pode ser calculado
    
    # Combinar de volta os dados ao DataFrame original para garantir que a coluna exista
    df.update(df_non_zero)
    
    # Calcular RMSE incluindo todos os dados, mesmo com valores zero
    df['SquaredError'] = (df['UNRealizada'] - df['UNPrevista']) ** 2
    rmse = np.sqrt(df['SquaredError'].mean())  # Root Mean Square Error
    
    return mape, rmse

def generate_general_report():
    """
    Gera um relatório geral de todas as predições e realizações no período especificado.
    """
    query = """
        SELECT ivpp.DATA, 
               ivpp.CodigoProduto, 
               ivpp.TotalUNVendidas AS UNPrevista, 
               ivpr.TotalUNVendidas AS UNRealizada,
               ivpp.ValorTotalVendido AS TotalPrevisto,
               ivpr.ValorTotalVendido AS TotalRealizado
        FROM indicadores_vendas_produtos_resumo ivpr
        INNER JOIN indicadores_vendas_produtos_previsoes ivpp
        ON ivpr.DATA = ivpp.DATA AND ivpr.CodigoProduto = ivpp.CodigoProduto
        WHERE ivpp.DATA BETWEEN '2024-01-01' AND '2024-03-31';
    """

    # Executar a consulta e converter para DataFrame
    result = db_manager.execute_query(query)
    df = pd.DataFrame(result['data'], columns=result['columns'])

    # Verificar se há dados
    if df.empty:
        print("Nenhum dado encontrado no período especificado.")
        return

    # Converter a coluna 'DATA' para o tipo datetime
    df['DATA'] = pd.to_datetime(df['DATA'])

    # Calcular métricas
    mape, rmse = calculate_metrics(df)
    print(f"MAPE (Mean Absolute Percentage Error): {mape:.2f}%")
    print(f"RMSE (Root Mean Square Error): {rmse:.2f}")

    # Agrupar por produto e calcular média de erro percentual por produto
    df_grouped = df.groupby('CodigoProduto').mean()

    # Plotando o erro médio percentual por produto
    plt.figure(figsize=(14, 7))
    plt.bar(df_grouped.index, df_grouped['PercentageError'])
    plt.title('Erro Médio Percentual por Produto')
    plt.xlabel('Código do Produto')
    plt.ylabel('Erro Médio Percentual (%)')
    plt.grid(True)
    plt.show()

    # Retornar DataFrame resumido para análise adicional, se necessário
    return df
