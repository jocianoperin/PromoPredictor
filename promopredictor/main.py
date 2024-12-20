import os
import pandas as pd
from pathlib import Path
from src.data_processing.process_raw_data import create_db_connection, extract_raw_data, save_raw_data
from src.data_processing.clean_data import process_clean_data
from src.models.train_model_quantity import train_model as train_model_quantity
from src.models.train_model_unit_price import train_model as train_model_unit_price
from src.models.predict_model import predict
from src.visualizations.generate_reports import generate_reports
from src.utils.logging_config import get_logger

# Configuração de logging
logger = get_logger(__name__)

# Definir o caminho base para os dados
BASE_DATA_DIR = Path(__file__).parent / "data"

def fetch_top_products(connection, limit=100):
    """
    Seleciona os produtos mais vendidos de categorias distintas.

    Parâmetros:
        connection: Conexão ativa com o banco de dados.
        limit (int): Número máximo de produtos a selecionar.

    Retorna:
        list: Lista de códigos de produtos selecionados.
    """
    query = """
    SELECT DISTINCT CodigoProduto
    FROM produtosmaisvendidos
    WHERE CodigoSecao IS NOT NULL
    ORDER BY QuantidadeTotalVendida DESC
    LIMIT %(limit)s
    """
    logger.info("Buscando os produtos mais vendidos no banco de dados.")
    df = pd.read_sql(query, connection, params={"limit": limit})
    return df["CodigoProduto"].tolist()

def main():
    """
    Orquestra o pipeline completo:
    1. Seleciona os produtos mais vendidos de categorias distintas.
    2. Processa os dados (extração, limpeza e engenharia de recursos).
    3. Treina os modelos usando os dados preparados.
    4. Faz predições para o período especificado.
    5. Gera relatórios e gráficos com os resultados.
    """
    logger.info("Iniciando o pipeline de processamento e análise.")

    # Criar diretórios base, se não existirem
    os.makedirs(BASE_DATA_DIR / "raw", exist_ok=True)
    os.makedirs(BASE_DATA_DIR / "cleaned", exist_ok=True)
    os.makedirs(BASE_DATA_DIR / "models", exist_ok=True)
    os.makedirs(BASE_DATA_DIR / "predictions", exist_ok=True)
    os.makedirs(BASE_DATA_DIR / "reports", exist_ok=True)

    # Etapa 1: Seleção de Produtos
    connection = create_db_connection()
    if connection:
        try:
            produtos = fetch_top_products(connection, limit=100)
        finally:
            connection.dispose()
    else:
        logger.error("Falha ao estabelecer conexão com o banco de dados.")
        return

    if not produtos:
        logger.error("Nenhum produto encontrado para processamento.")
        return

    # Etapa 2: Processamento de Dados
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
                    logger.warning(f"Não foram encontrados dados para o produto {produto}.")
                    continue  # Pula para o próximo produto se não houver dados

                # Limpeza e engenharia de recursos
                process_clean_data(produto, BASE_DATA_DIR)
        finally:
            connection.dispose()
    else:
        logger.error("Falha ao estabelecer conexão com o banco de dados.")
        return

    # Etapa 3: Treinamento dos Modelos
    logger.info("Iniciando o treinamento dos modelos.")
    train_model_quantity()
    train_model_unit_price()

    # Etapa 4: Predição
    logger.info("Iniciando a geração de predições.")
    predict('quantity')
    predict('unit_price')

    # Etapa 5: Geração de Relatórios
    logger.info("Gerando relatórios e gráficos.")
    generate_reports('quantity')
    generate_reports('unit_price')

    logger.info("Pipeline completo concluído com sucesso.")

if __name__ == "__main__":
    main()
