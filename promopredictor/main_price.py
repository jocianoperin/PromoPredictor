import os
from pathlib import Path
from src.data_processing.process_raw_data import create_db_connection, extract_raw_data, save_raw_data
from src.data_processing.price_data_pipeline import run_price_pipeline
from src.models.train_model_unit_price import train_model_unit_price
from src.models.predict_model_unit_price import predict_price
from src.visualizations.generate_reports_unit_price import generate_reports_unit_price
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
BASE_DATA_DIR = Path(__file__).parent / "data"

def main():
    produto = 26173
    logger.info("Iniciando pipeline de preço do produto %s", produto)

    # 1) Criar diretórios se necessário
    os.makedirs(BASE_DATA_DIR / "raw", exist_ok=True)
    os.makedirs(DATA_DIR / "cleaned", exist_ok=True)
    os.makedirs(DATA_DIR / "models", exist_ok=True)
    os.makedirs(DATA_DIR / "predictions", exist_ok=True)
    os.makedirs(DATA_DIR / "reports", exist_ok=True)

    # 2) Conectar ao banco e extrair dados brutos
    connection = create_db_connection()
    if not connection:
        logger.error("Falha ao estabelecer conexão com o banco de dados.")
        return

    try:
        # Extraindo dados brutos para o produto
        df_raw = extract_raw_data(connection, produto)

        if df_raw.empty:
            logger.warning(f"Nenhum dado encontrado para o produto {produto}. Encerrando pipeline.")
            return
        else:
            # Salvando CSV bruto em data/raw
            save_raw_data(df_raw, produto, BASE_DATA_DIR / "raw")
    finally:
        connection.dispose()

    # 3) Rodar pipeline de dados de preço
    raw_file_path = BASE_DATA_DIR / "raw" / f"produto_{produto}.csv"
    price_file_path = BASE_DATA_DIR / "cleaned" / f"produto_{produto}_price.csv"
    run_price_pipeline(raw_file_path, price_file_path)

    # 4) Treinar modelo de valor unitário
    train_model_unit_price()

    # 5) Prever valor unitário
    predict_price()

    # 6) Gerar relatórios de valor unitário
    generate_reports_unit_price()

    logger.info("Pipeline de preço (com extração) finalizado com sucesso.")

if __name__ == "__main__":
    main()
