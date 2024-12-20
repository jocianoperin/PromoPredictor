import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

BASE_DATA_DIR = Path(__file__).parent.parent.parent / "data"


def generate_reports(model_type):
    """
    Gera gráficos comparando valores reais e previstos para o modelo especificado.

    Parâmetros:
        model_type (str): Tipo do modelo ('quantity' ou 'unit_price').
    """
    file_path = BASE_DATA_DIR / f"predictions/produto_26173_{model_type}_predictions.csv"
    logger.info(f"Lendo dados de predições de {file_path}.")

    if not file_path.exists():
        logger.error(f"Arquivo de predições não encontrado: {file_path}")
        return

    df = pd.read_csv(file_path)

    # Determinar as colunas de comparação
    if model_type == 'quantity':
        real_column = 'Quantidade'
        predicted_column = 'Predicted_Quantidade'
        ylabel = 'Quantidade Vendida'
    elif model_type == 'unit_price':
        real_column = 'ValorUnitarioMedio'
        predicted_column = 'Predicted_ValorUnitarioMedio'
        ylabel = 'Valor Unitário Médio'
    else:
        logger.error(f"Tipo de modelo desconhecido: {model_type}")
        raise ValueError(f"Tipo de modelo desconhecido: {model_type}")

    # Comparação entre valores reais e preditos
    plt.figure(figsize=(12, 6))
    plt.plot(df['Data'], df[real_column], label=f'{ylabel} Real', marker='o')
    plt.plot(df['Data'], df[predicted_column], label=f'{ylabel} Predito', marker='x')
    plt.legend()
    plt.title(f'Comparação de Valores Reais e Preditos ({ylabel})')
    plt.xlabel('Data')
    plt.ylabel(ylabel)
    plt.grid(True)

    # Salvar gráfico
    output_path = BASE_DATA_DIR / f"reports/comparison_chart_{model_type}.png"
    plt.savefig(output_path)
    logger.info(f"Gráfico salvo em {output_path}.")
    plt.close()

if __name__ == "__main__":
    generate_reports('quantity')  # Ou use 'unit_price' para o relatório de valor unitário médio
