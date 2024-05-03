from src.data.create_tables import create_table_if_not_exists
from src.data.data_cleaner import delete_data, update_data, clean_null_values, remove_duplicates, remove_invalid_records, standardize_formatting
from src.data.index_manager import create_indexes
#from src.data.promotion_processor import fetch_all_products, process_chunks
#from src.data.promotion_sales_processor import process_promotions_in_chunks
from src.utils.logging_config import get_logger
from src.services.database_reset import drop_tables
from src.data.outlier_treatment import identify_and_treat_outliers
from src.data.data_conversion import convert_data_types
from src.data.data_exploration import explore_data
from src.models.train_model import train_model
from src.models.predict_model import make_prediction
from src.data.missing_value_imputer import imput_null_values

logger = get_logger(__name__)

def setup_database():
    """
    Limpa, cria tabelas e índices no banco de dados, se necessário.
    """
    logger.info("Iniciando a limpeza do banco de dados...")
    drop_tables()
    logger.info("Banco de dados limpo com sucesso.")

    logger.info("Iniciando a configuração do banco de dados...")
    create_table_if_not_exists()
    logger.info("Tabelas criadas/atualizadas com sucesso.")

    configure_indexes()
    logger.info("Índices criados/atualizados com sucesso.")

def configure_indexes():
    """
    Configura os índices nas tabelas de vendas.
    """
    indexes_vendasexport = [
        ("idx_codigo", "vendasexport", "Codigo"),
        ("idx_data", "vendasexport", "Data"),
        ("idx_codigocliente", "vendasexport", "CodigoCliente"),
        ("idx_data_codigocliente", "vendasexport", "Data, CodigoCliente"),
        ("idx_totalpedido", "vendasexport", "TotalPedido"),
    ]
    indexes_vendasprodutosexport = [
        ("idx_vendasprodutosexport_codigovenda", "vendasprodutosexport", "CodigoVenda"),
        ("idx_vendasprodutosexport_codigoproduto", "vendasprodutosexport", "CodigoProduto"),
        ("idx_vendasprodutosexport_codigosecao", "vendasprodutosexport", "CodigoSecao"),
        ("idx_vendasprodutosexport_codigogrupo", "vendasprodutosexport", "CodigoGrupo"),
        ("idx_vendasprodutosexport_codigosubgrupo", "vendasprodutosexport", "CodigoSubGrupo"),
        ("idx_vendasprodutosexport_secaogrupo", "vendasprodutosexport", "CodigoSecao, CodigoGrupo"),
        ("idx_vendasprodutosexport_valorunitario", "vendasprodutosexport", "ValorUnitario"),
        ("idx_vendasprodutosexport_quantidade", "vendasprodutosexport", "Quantidade"),
        ("idx_vendasprodutosexport_desconto", "vendasprodutosexport", "Desconto"),
        ("idx_vendasprodutosexport_precoempromocao", "vendasprodutosexport", "PrecoemPromocao"),
    ]
    create_indexes(indexes_vendasprodutosexport + indexes_vendasexport)

def clean_and_process_data():
    """
    Limpa e processa os dados nas tabelas de vendas, removendo registros inválidos, duplicados e padronizando formatações.
    """
    logger.info("Iniciando a limpeza e processamento dos dados...")
    remove_invalid_records("vendasexport", ["TotalPedido <= 0", "TotalPedido IS NULL"])
    remove_invalid_records("vendasprodutosexport", ["ValorTotal <= 0", "Quantidade <= 0", "ValorCusto <= 0"])

    clean_null_values("vendasprodutosexport", ["ValorCusto", "ValorUnitario", "Quantidade"])
    clean_null_values("vendasexport", ["TotalPedido", "TotalCusto"])

    # Chamada para imputar valores nulos usando ARIMA
    imput_null_values('vendasprodutosexport', 'CodigoProduto', 'Data', ['ValorCusto', 'ValorUnitario'])

    remove_duplicates("vendasexport")
    remove_duplicates("vendasprodutosexport")

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

'''def train_and_test_model():
    """
    Treina o modelo de machine learning e realiza uma predição de teste.
    """
    logger.info("Iniciando o treinamento do modelo...")
    train_model()
    logger.info("Treinamento concluído com sucesso.")

    logger.info("Realizando uma predição de teste...")
    make_prediction()
    logger.info("Predição de teste concluída.")'''

def main():
    """
    Função principal que organiza a sequência de operações para inicialização do projeto.
    """
    try:
        logger.info("Iniciando o processo de inicialização do projeto...")

        setup_database()

        #clean_and_process_data()

        #process_promotions()
        #logger.info("process_promotions")

        ##train_and_test_model()
        logger.info("Processo de inicialização do projeto concluído com sucesso.")
    except Exception as e:
        logger.error(f"Erro durante o processo de inicialização do projeto: {e}")

if __name__ == "__main__":
    main()
