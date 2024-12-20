import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

BASE_DATA_DIR = Path(__file__).parent.parent / "data"

def generate_reports():
    """
    Gera gráficos comparando valores reais e previstos.
    """
    file_path = BASE_DATA_DIR / "predictions/produto_26173_predictions.csv"
    logger.info(f"Lendo dados de predições de {file_path}.")
    
    df = pd.read_csv(file_path)
    
    # Comparação entre valores reais e preditos
    plt.figure(figsize=(12, 6))
    plt.plot(df['Data'], df['Quantidade'], label='Quantidade Real', marker='o')
    plt.plot(df['Data'], df['Predicted_Quantidade'], label='Quantidade Predita', marker='x')
    plt.legend()
    plt.title('Comparação de Valores Reais e Preditos')
    plt.xlabel('Data')
    plt.ylabel('Quantidade')
    plt.grid(True)
    
    # Salvar gráfico
    output_path = BASE_DATA_DIR / "reports/comparison_chart.png"
    plt.savefig(output_path)
    logger.info(f"Gráfico salvo em {output_path}.")
    plt.show()

if __name__ == "__main__":
    generate_reports()
