# promopredictor/main.py

from src.models.train_model import train_and_evaluate_models
from src.models.predict_sales import predict_future_sales
from src.visualizations.general_forecast_analysis import plot_forecast_analysis
from src.utils.logging_config import get_logger
from src.visualizations.compare_predictions import compare_predictions
import matplotlib.pyplot as plt

logger = get_logger(__name__)

def main():
    logger.info("==== Início do processo ====")
    try:
        produto_especifico = 26173  # Substitua pelo código do produto desejado

        # Treinamento e Salvamento do Modelo
        logger.info("1. Treinando e salvando o modelo para o produto específico...")
        train_and_evaluate_models(produto_especifico)  # Descomente se precisar treinar os modelos
        logger.info("Modelo treinado e salvo com sucesso.")

        # Previsão e Inserção dos Dados Previstos
        logger.info("2. Realizando previsões e inserindo dados previstos...")
        predict_future_sales(produto_especifico)
        logger.info("Previsões realizadas e inseridas com sucesso.")

        # Comparação das Previsões com os Valores Reais
        logger.info("3. Comparando previsões com valores reais e gerando gráfico...")
        #plot_forecast_analysis(produto_especifico)
        #plt.savefig('quantidade_unidades_vendidas.png')
        logger.info("Comparação concluída e gráfico gerado com sucesso.")

        # Visualização comparativa
        compare_predictions(produto_especifico)

        logger.info("==== Processo finalizado com sucesso! ====")

    except Exception as e:
        logger.error(f"Erro durante a execução do pipeline: {e}")

if __name__ == "__main__":
    main()
