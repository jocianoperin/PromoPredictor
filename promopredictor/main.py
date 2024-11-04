# promopredictor/main.py

from src.models.train_model import train_and_evaluate_models
from src.models.predict_sales import predict_future_sales
from src.visualizations.compare_predictions import compare_predictions
from src.utils.logging_config import get_logger
from src.services.data_processing import (
    extract_raw_data,
    clean_data,
    feature_engineering
)
import os

logger = get_logger(__name__)

def main():
    logger.info("==== Início do processo ====")
    try:
        # Lista de produtos para processar
        produtos_especificos = [26173]  # Substitua pelos códigos dos produtos desejados
        #produtos_especificos = [26173, 12345, 67890]  # Substitua pelos códigos dos produtos desejados

        for produto_especifico in produtos_especificos:
            logger.info(f"Processando o produto {produto_especifico}")

            # Etapa 1: Extração e Processamento de Dados
            logger.info("1. Extraindo e processando dados...")
            df_raw = extract_raw_data(produto_especifico)

            if not df_raw.empty:
                df_cleaned = clean_data(df_raw)
                df_processed = feature_engineering(df_cleaned)

                # Criar o diretório 'data' dentro de 'promopredictor' se não existir
                data_dir = os.path.join('promopredictor', 'data')
                os.makedirs(data_dir, exist_ok=True)

                # Salvar o dataframe processado em um arquivo CSV por produto
                df_processed.to_csv(os.path.join(data_dir, f'dados_processados_{produto_especifico}.csv'), index=False, sep=',')
                logger.info(f'Dados processados salvos em promopredictor/data/dados_processados_{produto_especifico}.csv.')

                # Verificar se há valores nulos
                logger.info("Valores nulos por coluna:")
                logger.info(df_processed.isnull().sum())

                # Exibir as primeiras linhas do DataFrame com as colunas
                logger.info("Primeiras linhas do DataFrame:")
                logger.info(df_processed.head())
            else:
                logger.error("Não foi possível extrair os dados. Pulando para o próximo produto.")
                continue

            # Etapa 2: Treinamento e Salvamento do Modelo
            logger.info("2. Treinando e salvando o modelo para o produto específico...")
            train_and_evaluate_models(produto_especifico)
            logger.info("Modelo treinado e salvo com sucesso.")

            # Etapa 3: Previsão e Inserção dos Dados Previstos
            logger.info("3. Realizando previsões e inserindo dados previstos...")
            predict_future_sales(produto_especifico)
            logger.info("Previsões realizadas e salvas com sucesso.")

            # Etapa 4: Comparação das Previsões com os Valores Reais
            logger.info("4. Comparando previsões com valores reais e gerando gráfico...")
            compare_predictions(produto_especifico)
            logger.info("Comparação concluída e gráfico gerado com sucesso.")

        logger.info("==== Processo finalizado com sucesso para todos os produtos! ====")

    except Exception as e:
        logger.error(f"Erro durante a execução do pipeline: {e}")

if __name__ == "__main__":
    main()
