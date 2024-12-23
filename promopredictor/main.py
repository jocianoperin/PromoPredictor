from src.services.database import DatabaseManager
from src.data_processing.process_raw_data import extract_raw_data, save_raw_data
from src.data_processing.price_data_pipeline import run_price_pipeline
from src.data_processing.clean_data import process_clean_data
from src.models.train_model_unit_price import train_model_unit_price
from src.models.train_model_quantity import train_model
from src.models.predict_model_unit_price import predict_price
from src.models.predict_model_quantity import predict
from src.visualizations.generate_reports_unit_price import generate_reports_unit_price
from src.visualizations.generate_reports import generate_reports
from src.utils.logging_config import get_logger
import os
from pathlib import Path

logger = get_logger(__name__)
BASE_DATA_DIR = Path(__file__).parent / "data"

def get_produtos_mais_vendidos(db_manager):
    """
    Obtém a lista de produtos mais vendidos do banco de dados.

    Parâmetros:
        db_manager (DatabaseManager): Gerenciador do banco de dados.

    Retorna:
        list: Lista de códigos de produtos mais vendidos.
    """
    query = "SELECT CodigoProduto FROM produtosmaisvendidos WHERE CodigoProduto = 26173"

    try:
        result = db_manager.execute_query(query)
        produtos = [row[0] for row in result['data']]
        logger.info(f"Produtos mais vendidos obtidos: {produtos}")
        return produtos
    except Exception as e:
        logger.error(f"Erro ao obter produtos mais vendidos: {e}")
        return []

def main():
    """
    Orquestra os pipelines de quantidade e valor unitário utilizando o DatabaseManager.
    """
    logger.info("Iniciando pipeline unificado.")

    # Criar diretórios necessários
    os.makedirs(BASE_DATA_DIR / "raw", exist_ok=True)
    os.makedirs(BASE_DATA_DIR / "cleaned", exist_ok=True)
    os.makedirs(BASE_DATA_DIR / "models", exist_ok=True)
    os.makedirs(BASE_DATA_DIR / "predictions", exist_ok=True)
    os.makedirs(BASE_DATA_DIR / "reports", exist_ok=True)

    db_manager = DatabaseManager()  # Instancia o DatabaseManager

    try:
        # Obter a lista de produtos mais vendidos
        produtos = get_produtos_mais_vendidos(db_manager)
        if not produtos:
            logger.warning("Nenhum produto encontrado na consulta de produtos mais vendidos.")
            return

        for produto in produtos:
            logger.info(f"Processando o produto {produto}.")

            # Etapa 1: Extrair Dados Brutos
            df_raw = extract_raw_data(db_manager, produto)
            if df_raw.empty:
                logger.warning(f"Nenhum dado encontrado para o produto {produto}.")
                continue
            save_raw_data(df_raw, produto, BASE_DATA_DIR / "raw")

            # Etapa 2: Pipeline de preço
            raw_file_path = BASE_DATA_DIR / "raw" / f"produto_{produto}.csv"
            price_file_path = BASE_DATA_DIR / "cleaned" / f"produto_{produto}_price.csv"
            logger.info(f"Rodando pipeline de preço para o produto {produto}.")
            run_price_pipeline(raw_file_path, price_file_path)

            # Etapa 3: Pipeline de quantidade
            logger.info(f"Rodando pipeline de quantidade para o produto {produto}.")
            process_clean_data(produto, BASE_DATA_DIR)

            # Etapa 4: Treinamento do modelo para preço
            logger.info(f"Treinando modelo de preço para o produto {produto}.")
            train_model_unit_price(produto, window_size=7)

            # Etapa 5: Treinamento do modelo para quantidade
            logger.info(f"Treinando modelo de quantidade para o produto {produto}.")
            train_model(produto, window_size=7)

            # Etapa 6: Predição para preço
            logger.info(f"Realizando predições de preço para o produto {produto}.")
            predict_price(produto)

            # Etapa 7: Predição para quantidade
            logger.info(f"Realizando predições de quantidade para o produto {produto}.")
            predict(produto)

            # Etapa 8: Geração de Relatórios
            logger.info(f"Gerando relatórios para o produto {produto}.")
            generate_reports_unit_price(produto)
            generate_reports(produto)

        logger.info("Pipeline unificado concluído com sucesso.")
    except Exception as e:
        logger.error(f"Erro durante o pipeline: {e}")
    finally:
        db_manager.engine.dispose()  # Fecha as conexões com o banco

if __name__ == "__main__":
    main()
