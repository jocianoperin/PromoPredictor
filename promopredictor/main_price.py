import os
from pathlib import Path
from src.data_processing.price_data_pipeline import run_price_pipeline
from src.models.train_model_unit_price import train_model_unit_price
from src.models.predict_model_unit_price import predict_price
from src.visualizations.generate_reports_unit_price import generate_reports_unit_price
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

def main():
    produto = 26173
    logger.info("Iniciando pipeline de preço do produto %s", produto)

    # 1) Criar diretórios se necessário
    os.makedirs(DATA_DIR / "cleaned", exist_ok=True)
    os.makedirs(DATA_DIR / "models", exist_ok=True)
    os.makedirs(DATA_DIR / "predictions", exist_ok=True)
    os.makedirs(DATA_DIR / "reports", exist_ok=True)

    # 2) Rodar pipeline de dados
    raw_file = DATA_DIR / "raw" / f"produto_{produto}.csv"
    price_file = DATA_DIR / "cleaned" / f"produto_{produto}_price.csv"
    run_price_pipeline(raw_file, price_file)

    # 3) Treinar modelo
    train_model_unit_price()

    # 4) Prever
    predict_price()

    # 5) Relatório
    generate_reports_unit_price()

    logger.info("Pipeline de preço finalizado com sucesso.")

if __name__ == "__main__":
    main()
