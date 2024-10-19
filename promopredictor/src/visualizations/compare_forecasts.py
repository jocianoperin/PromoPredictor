import pandas as pd
import matplotlib.pyplot as plt
from ..services.database import db_manager

def plot_comparison(codigo_produto):
    """
    Gera um gráfico comparando os valores previstos e realizados para um produto específico.
    """
    # Consulta SQL para buscar os dados do produto especificado
    query = f"""
        SELECT ivpp.DATA, 
               ivpp.CodigoProduto, 
               ivpp.TotalUNVendidas AS UNPrevista, 
               ivpr.TotalUNVendidas AS UNRealizada,
               ivpp.ValorTotalVendido AS TotalPrevisto,
               ivpr.ValorTotalVendido AS TotalRealizado
        FROM indicadores_vendas_produtos_resumo ivpr
        INNER JOIN indicadores_vendas_produtos_previsoes ivpp
        ON ivpr.DATA = ivpp.DATA AND ivpr.CodigoProduto = ivpp.CodigoProduto
        WHERE ivpp.CodigoProduto = {codigo_produto}
        AND ivpp.DATA BETWEEN '2024-01-01' AND '2024-03-31';
    """

    # Executar a consulta e converter para DataFrame
    result = db_manager.execute_query(query)
    df = pd.DataFrame(result['data'], columns=result['columns'])

    # Verificar se há dados para o produto especificado
    if df.empty:
        print(f"Nenhum dado encontrado para o produto {codigo_produto}")
        return

    # Converter a coluna 'DATA' para o tipo datetime
    df['DATA'] = pd.to_datetime(df['DATA'])

    # Plotando as quantidades vendidas previstas vs realizadas
    plt.figure(figsize=(14, 7))
    plt.plot(df['DATA'], df['UNPrevista'], label='Unidades Previstas', linestyle='--')
    plt.plot(df['DATA'], df['UNRealizada'], label='Unidades Realizadas', marker='o')
    plt.title(f'Comparação de Unidades Vendidas - Produto {codigo_produto}')
    plt.xlabel('Data')
    plt.ylabel('Unidades')
    plt.legend()
    plt.grid(True)
    plt.show()

    # Plotando os valores totais vendidos previstos vs realizados
    plt.figure(figsize=(14, 7))
    plt.plot(df['DATA'], df['TotalPrevisto'], label='Valor Total Previsto (R$)', linestyle='--')
    plt.plot(df['DATA'], df['TotalRealizado'], label='Valor Total Realizado (R$)', marker='o')
    plt.title(f'Comparação de Valor Total Vendido - Produto {codigo_produto}')
    plt.xlabel('Data')
    plt.ylabel('Valor Total (R$)')
    plt.legend()
    plt.grid(True)
    plt.show()

# Exemplo de uso
plot_comparison(10018)  # Substitua pelo código do produto desejado
