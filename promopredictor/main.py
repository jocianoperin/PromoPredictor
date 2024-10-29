from src.utils.logging_config import get_logger
from src.services.tables_manager import create_tables, drop_tables, insert_data, configure_indexes
from src.services.data_cleaner import remove_invalid_records
from src.services.indicadores_resumo_batch import process_data_and_insert as process_resumo
from src.services.indicadores_vendas_produtos import process_data_and_insert as process_indicadores_vendas
from src.services.data_formatter import check_data_types, standardize_formatting
from src.models.train_model import train_and_evaluate_model
from src.models.predict_sales import predict_future_sales
from src.visualizations.general_forecast_analysis import plot_forecast_analysis

logger = get_logger(__name__)

def main():
    """
    Função principal para execução do pipeline completo, incluindo criação de tabelas, inserção de dados,
    validação, limpeza, treinamento, previsão e comparação dos resultados, com logs detalhados.
    """
    logger.info("==== Início do processo ====")

    try:
        # Definir o código do produto específico
        produto_especifico = 26173  # Substitua pelo código do produto desejado

        # 1. Dropar tabelas antigas (comentado, caso não seja necessário)
        """
        logger.info("1. Excluindo tabelas antigas...")
        drop_tables()
        logger.info("Tabelas antigas excluídas com sucesso.")

        # 2. Criar as tabelas
        logger.info("2. Criando novas tabelas...")
        create_tables()
        logger.info("Novas tabelas criadas com sucesso.")

        # 3. Inserir os dados nas tabelas vendas_auxiliar, vendasprodutos_auxiliar etc.
        logger.info("3. Inserindo dados nas tabelas principais...")
        insert_data()
        logger.info("Dados inseridos com sucesso.")

        # 4. Validações e Limpeza de Dados
        logger.info("4. Iniciando validações de dados...")

        # Remover registros inválidos da tabela vendas_auxiliar
        logger.info("Removendo registros inválidos da tabela 'vendas_auxiliar'...")
        remove_invalid_records("vendas_auxiliar", ["DATA IS NULL"])
        logger.info("Registros inválidos removidos de 'vendas_auxiliar'.")

        # Remover registros inválidos da tabela vendasprodutos_auxiliar
        logger.info("Removendo registros inválidos da tabela 'vendasprodutos_auxiliar'...")
        remove_invalid_records("vendasprodutos_auxiliar", [
            "Quantidade IS NULL OR Quantidade <= 0",
            "ValorTotal IS NULL OR ValorTotal <= 0",
            "Promocao IS NULL OR Promocao < 0"
        ])
        logger.info("Registros inválidos removidos de 'vendasprodutos_auxiliar'.")

        # Verificação de Tipos de Dados
        logger.info("Verificando tipos de dados nas tabelas...")
        check_data_types('vendasprodutos_auxiliar', {'Quantidade': 'DOUBLE', 'ValorTotal': 'DOUBLE', 'Promocao': 'DECIMAL(3,0)'})
        check_data_types('vendas_auxiliar', {'DATA': 'DATE'})
        check_data_types('indicadores_vendas_produtos', {'Quantidade': 'DOUBLE', 'ValorTotal': 'DOUBLE', 'Promocao': 'DECIMAL(3,0)'})
        logger.info("Tipos de dados verificados e ajustados.")
        

        # 5. Inserir os dados na tabela indicadores_vendas_produtos
        logger.info("5. Inserindo dados na tabela indicadores_vendas_produtos...")
        process_indicadores_vendas()
        logger.info("Dados inseridos na tabela indicadores_vendas_produtos com sucesso.")

        # 6. Configurar os índices nas tabelas
        logger.info("6. Configurando índices nas tabelas...")
        configure_indexes()
        logger.info("Índices configurados com sucesso.")

        # 7. Processamento dos dados para indicadores de resumo
        logger.info("7. Processando dados de indicadores de vendas para resumo...")
        process_resumo()
        logger.info("Dados de indicadores de vendas processados e inseridos com sucesso.")
        """
        # 8. Treinamento e Salvamento do Modelo
        logger.info("1. Treinando e salvando o modelo para o produto específico...")
        train_and_evaluate_model(produto_especifico)
        logger.info("Modelo treinado e salvo com sucesso.")

        # 9. Previsão e Inserção dos Dados Previstos
        logger.info("2. Realizando previsões e inserindo dados previstos...")
        predict_future_sales(produto_especifico)
        logger.info("Previsões realizadas e inseridas com sucesso.")

        # 10. Comparação das Previsões com os Valores Reais
        logger.info("3. Comparando previsões com valores reais e gerando gráfico...")
        plot_forecast_analysis(produto_especifico)
        logger.info("Comparação concluída e gráfico gerado com sucesso.")

        logger.info("==== Processo finalizado com sucesso! ====")
    
    except Exception as e:
        logger.error(f"Erro durante a execução do pipeline: {e}")

if __name__ == "__main__":
    main()
