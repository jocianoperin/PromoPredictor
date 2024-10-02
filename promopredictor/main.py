from src.services.tables_manager import create_tables, drop_tables, insert_data, configure_indexes
from src.services.data_cleaner import clean_null_values, remove_invalid_records, remove_duplicates
from src.services.data_formatter import standardize_formatting, check_data_types
from src.services.outlier_detection import detect_and_remove_outliers
from src.services.indicadores_resumo_batch import process_data_and_insert
from src.utils.logging_config import get_logger
from src.services.promotion_detector import detect_promotions
from src.services.promotion_indicators_processor import calculate_promotion_indicators

logger = get_logger(__name__)

def clean_and_prepare_database():
    """
    Limpa o banco de dados e prepara as tabelas necessárias.
    """
    try:
        drop_tables()
        logger.info("Banco de dados limpo com sucesso.")

        create_tables()
        logger.info("Tabelas criadas com sucesso.")

        insert_data()
        logger.info("Dados inseridos nas tabelas com sucesso.")

        configure_indexes()
        logger.info("Índices configurados com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao limpar e preparar o banco de dados: {e}")


def clean_data():
    """
    Remove registros inválidos e realiza a verificação de tipos de dados.
    """
    try:
        remove_invalid_records("vendasexport", [
            "totalpedido IS NULL OR totalpedido <= 0",
            "totalcusto IS NULL OR totalcusto <= 0",
            "data IS NULL",
            "hora IS NULL"
        ])
        remove_invalid_records("vendasprodutosexport", [
            "valortabela IS NULL OR valortabela <= 0",
            "valorunitario IS NULL OR valorunitario <= 0",
            "valorcusto IS NULL OR valorcusto <= 0"
        ])

        # Verificação de tipos de dados
        column_types = {'valorunitario': 'DECIMAL(10,2)'}
        check_data_types('vendasprodutosexport', column_types)
        column_types = {'data': 'DATE'}
        check_data_types('vendasexport', column_types)

        # Remoção de duplicatas (desativada por enquanto)
        #remove_duplicates("vendasexport")
        #remove_duplicates("vendasprodutosexport")
        #Difícil localizar um padrão de dados duplicados

        # Detecção e remoção de outliers (desativada por enquanto)
        #detect_and_remove_outliers('vendasexport', ['totalpedido', 'totalcusto'])
        #detect_and_remove_outliers('vendasprodutosexport', ['valortabela', 'valorunitario', 'valorcusto'])

        logger.info("Dados limpos com sucesso.")

        # Detectar promoções
        detect_promotions()

        # Calcular indicadores da promoção
        calculate_promotion_indicators()

        logger.info("Processo de inicialização do projeto concluído com sucesso.")
    except Exception as e:
        logger.error(f"Erro durante o processo de limpeza de dados: {e}")

def main():
    """
    Função principal para iniciar o processamento do projeto.
    """
    logger.info("Iniciando o processo de limpeza e preparação do banco de dados.")
    clean_and_prepare_database()

    logger.info("Iniciando a limpeza e processamento dos dados.")
    clean_data()

if __name__ == "__main__":
    main()
