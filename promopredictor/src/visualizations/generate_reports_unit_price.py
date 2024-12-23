import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

BASE_DATA_DIR = Path(__file__).parent.parent.parent / "data"

def generate_reports_unit_price():
    """
    Gera relatório de ValorUnitarioMedio vs Predicted_ValorUnitario.
    """
    file_path = BASE_DATA_DIR / "predictions" / "produto_26173_unit_price_predictions_v2.csv"
    logger.info(f"Lendo dados de predições de valor unitário em {file_path}.")

    try:
        df = pd.read_csv(file_path, parse_dates=['Data'])
    except FileNotFoundError:
        logger.error(f"Arquivo não encontrado: {file_path}")
        return

    # Precisamos da coluna 'ValorUnitarioMedio' original.
    # Mas no CSV de previsão talvez só tenha a parte do futuro. 
    # Se você quiser comparar com a real, precisa ter esse dado real.
    # Se no pipeline futuro não existe valor real, só "Data" e as features...
    # Você pode comparar com outra fonte ou com um "ground truth" posterior.
    # Se estiver no mesmo CSV, ok. Se não, adapte aqui.

    if 'ValorUnitarioMedio' not in df.columns:
        logger.warning("Não existe ValorUnitarioMedio real para comparar (coluna ausente).")
        # Caso não tenha o real, você só tem as previsões.
        return

    required_cols = ['ValorUnitarioMedio', 'Predicted_ValorUnitario']
    if not all(c in df.columns for c in required_cols):
        logger.error(f"As colunas necessárias estão ausentes: {required_cols}")
        return

    # Calcular erro
    df['Erro'] = df['ValorUnitarioMedio'] - df['Predicted_ValorUnitario']
    df['Erro_Absoluto'] = abs(df['Erro'])
    # Evitar divisões por zero
    df['Erro_Relativo_%'] = np.where(df['ValorUnitarioMedio'] == 0, 0, (df['Erro_Absoluto'] / df['ValorUnitarioMedio']) * 100)

    mae = df['Erro_Absoluto'].mean()
    mape = df['Erro_Relativo_%'].mean()

    logger.info(f"MAE (ValorUnitario): {mae:.4f}")
    logger.info(f"MAPE (ValorUnitario): {mape:.2f}%")

    # Plotar
    plt.figure(figsize=(12,6))
    plt.plot(df['Data'], df['ValorUnitarioMedio'], label='Real', marker='o')
    plt.plot(df['Data'], df['Predicted_ValorUnitario'], label='Previsto', marker='x')
    plt.title('Comparação Valor Unitário Médio - Real vs Previsto')
    plt.legend()
    plt.grid(True)

    output_path = BASE_DATA_DIR / "reports" / "comparison_chart_unit_price_v2.png"
    plt.savefig(output_path)
    plt.close()

    logger.info(f"Relatório de valor unitário salvo em {output_path}")

if __name__ == "__main__":
    generate_reports_unit_price()
