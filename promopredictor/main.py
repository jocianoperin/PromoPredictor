from src.services.tables_manager import create_tables, drop_tables, insert_data, configure_indexes
from src.services.index_manager import create_indexes
#from src.data.promotion_processor import fetch_all_products, process_chunks
#from src.data.promotion_sales_processor import process_promotions_in_chunks
from src.utils.logging_config import get_logger
from src.data.missing_value_imputer import imput_null_values

logger = get_logger(__name__)

def setup_database():
    """
    Limpa, cria tabelas e índices no banco de dados, se necessário.
    """
    drop_tables()
    logger.info("Banco de dados limpo com sucesso.")

    create_tables()
    logger.info("Tabelas criadas/atualizadas com sucesso.")

    insert_data()
    logger.info("Tabelas criadas/atualizadas com sucesso.")

def clean_and_process_data():
    """
    Limpa e processa os dados nas tabelas de vendas, removendo registros inválidos, duplicados e padronizando formatações.
    """
    logger.info("Iniciando a limpeza e processamento dos dados...")

    #Funções ocultas que ficaram na data_cleaner.py
        #remove_invalid_records("vendasexport", ["TotalPedido <= 0", "TotalPedido IS NULL"])
        #remove_invalid_records("vendasprodutosexport", ["ValorTotal <= 0", "Quantidade <= 0", "ValorCusto <= 0"])

        # Substituindo pela imputação por ARIMA
        #clean_null_values("vendasprodutosexport", ["ValorCusto", "ValorUnitario", "Quantidade"])
        #clean_null_values("vendasexport", ["TotalPedido", "TotalCusto"])

    # Chamada para imputar valores nulos usando ARIMA
    imput_null_values('vendasprodutosexport', 'CodigoProduto', 'Data', ['ValorCusto', 'ValorUnitario'])

    #remove_duplicates("vendasexport")
    #remove_duplicates("vendasprodutosexport")

    # Analisar possíveis outliers da vendasexport
    #identify_and_treat_outliers("vendasprodutosexport", "ValorCusto,ValorUnitario")
    
    logger.info("Limpeza e processamento de dados concluídos com sucesso.")

'''def process_promotions():
    """
    Processa as promoções identificadas nos produtos.
    """
    logger.info("Iniciando o processamento de promoções...")
    products = fetch_all_products()
    if not products.empty:
        process_chunks(products)
    else:
        logger.info("Nenhum produto para processar.")
    process_promotions_in_chunks()
    logger.info("Processamento de promoções concluído com sucesso.")'''

def main():
    """
    Função principal que organiza a sequência de operações para inicialização do projeto.
    """
    try:
        logger.info("Iniciando o processo de inicialização do projeto...")

        # Dropar, criar e inserir dados nas tabelas necessárias
        #setup_database()

        # Criar indexes para otimização de consultas
        #configure_indexes()
        logger.info("Índices criados/atualizados com sucesso.")
        
        clean_and_process_data()

        #process_promotions()
        #logger.info("process_promotions")

        ##train_and_test_model()
        logger.info("Processo de inicialização do projeto concluído com sucesso.")
    except Exception as e:
        logger.error(f"Erro durante o processo de inicialização do projeto: {e}")

if __name__ == "__main__":
    main()
