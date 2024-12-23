import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

BASE_DATA_DIR = Path(__file__).parent.parent.parent / "data"

def generate_reports():
    """
    Gera gráficos comparando valores reais e previstos e salva como imagens.
    Também calcula e registra indicadores de assertividade e erro no log.
    """
    # Caminho do arquivo de predições
    file_path = BASE_DATA_DIR / "predictions/produto_26173_predictions.csv"
    logger.info(f"Lendo dados de predições de {file_path}.")
    
    try:
        # Ler os dados de predição
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        logger.error(f"Arquivo de predições não encontrado: {file_path}")
        return
    
    # Verificar se as colunas necessárias estão presentes
    required_columns = ['Quantidade', 'Predicted_Quantidade', 'Data']
    if not all(col in df.columns for col in required_columns):
        logger.error(f"As colunas necessárias estão ausentes no arquivo de predições: {required_columns}")
        return

    # Calcular indicadores de erro
    df['Erro'] = df['Quantidade'] - df['Predicted_Quantidade']
    df['Erro_Absoluto'] = abs(df['Erro'])
    df['Erro_Relativo'] = abs(df['Erro'] / df['Quantidade']) * 100

    mae = df['Erro_Absoluto'].mean()  # Mean Absolute Error
    mape = df['Erro_Relativo'].mean()  # Mean Absolute Percentage Error

    logger.info(f"Indicadores de erro calculados:")
    logger.info(f"- MAE (Mean Absolute Error): {mae:.4f}")
    logger.info(f"- MAPE (Mean Absolute Percentage Error): {mape:.2f}%")

    # Criar o diretório para salvar os relatórios, se necessário
    reports_dir = BASE_DATA_DIR / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

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
    output_path = reports_dir / "comparison_chart.png"
    plt.savefig(output_path)
    logger.info(f"Gráfico salvo em {output_path}.")
    
    # Fechar a figura para evitar consumo excessivo de memória em execuções repetidas
    plt.close()

if __name__ == "__main__":
    generate_reports()
