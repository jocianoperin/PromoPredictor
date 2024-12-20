import os
from pathlib import Path
from src.data_processing.process_raw_data import create_db_connection, extract_raw_data, save_raw_data
from src.data_processing.clean_data import process_clean_data
from promopredictor.src.models.train_model_quantity import train_model
from src.models.predict_model import predict
from src.visualizations.generate_reports import generate_reports
from src.utils.logging_config import get_logger

# Configuração de logging
logger = get_logger(__name__)

# Definir o caminho base para os dados
BASE_DATA_DIR = Path(__file__).parent / "data"

def main():
    """
    Orquestra o pipeline completo:
    1. Processa os dados (extração, limpeza e engenharia de recursos).
    2. Treina o modelo usando os dados preparados.
    3. Faz predições para o período especificado.
    4. Gera relatórios e gráficos com os resultados.
    """
    logger.info("Iniciando o pipeline de processamento e análise.")

    # Lista de códigos de produtos para processamento
    produtos = [26173]  # Substitua por uma lista de produtos reais

    # Criar diretórios base, se não existirem
    os.makedirs(BASE_DATA_DIR / "raw", exist_ok=True)
    os.makedirs(BASE_DATA_DIR / "cleaned", exist_ok=True)
    os.makedirs(BASE_DATA_DIR / "models", exist_ok=True)
    os.makedirs(BASE_DATA_DIR / "predictions", exist_ok=True)
    os.makedirs(BASE_DATA_DIR / "reports", exist_ok=True)

    """# Etapa 1: Processamento de Dados
    connection = create_db_connection()
    if connection:
        try:
            for produto in produtos:
                logger.info(f"Processando o produto {produto}.")

                # Extração de dados brutos
                df_raw = extract_raw_data(connection, produto)

                # Salvamento de dados brutos
                if not df_raw.empty:
                    save_raw_data(df_raw, produto, BASE_DATA_DIR / "raw")
                else:
                    logger.warning(f"Nenhum dado encontrado para o produto {produto}.")
                    continue  # Pula para o próximo produto se não houver dados

                # Limpeza e engenharia de recursos
                process_clean_data(produto, BASE_DATA_DIR)
        finally:
            connection.dispose()
    else:
        logger.error("Falha ao estabelecer conexão com o banco de dados.")
        return

    # Etapa 2: Treinamento do Modelo
    logger.info("Iniciando o treinamento do modelo.")
    train_model()

    # Etapa 3: Predição
    logger.info("Iniciando a geração de predições.")
    predict()"""

    # Etapa 4: Geração de Relatórios
    logger.info("Gerando relatórios e gráficos.")
    generate_reports()

    logger.info("Pipeline completo concluído com sucesso.")

if __name__ == "__main__":
    main()
