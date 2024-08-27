from src.services.tables_manager import create_tables, drop_tables, insert_data, configure_indexes
from src.services.data_cleaner import clean_null_values, remove_invalid_records, remove_duplicates
from src.services.data_formatter import standardize_formatting, check_data_types
from src.services.outlier_detection import detect_and_remove_outliers
from src.utils.logging_config import get_logger
from src.services.promotion_detector import detect_promotions
from src.services.promotion_indicators_processor import calculate_promotion_indicators

logger = get_logger(__name__)

def clean_and_prepare_database():
    """
    Limpa o banco de dados e prepara as tabelas necessárias.
    """
    drop_tables()
    logger.info("Banco de dados limpo com sucesso.")

    create_tables()
    logger.info("Tabelas criadas com sucesso.")

    insert_data()
    logger.info("Dados inseridos nas tabelas com sucesso.")

    configure_indexes()
    logger.info("Índices configurados com sucesso.")

def clean_data():
    """
    Remove registros inválidos e realiza a verificação de tipos de dados.
    """
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

    check_data_types('vendasprodutosexport', {'valorunitario': 'DECIMAL(10,2)'})
    check_data_types('vendasexport', {'data': 'DATE'})

    logger.info("Dados limpos e tipos de dados verificados com sucesso.")

def detect_and_process_promotions():
    """
    Detecta promoções e calcula os indicadores correspondentes.
    """
    detect_promotions()
    calculate_promotion_indicators()
    logger.info("Promoções detectadas e indicadores calculados com sucesso.")

def main():
    """
    Função principal que organiza a sequência de operações para inicialização do projeto.
    """
    try:
        logger.info("Iniciando o processo de inicialização do projeto...")

        clean_and_prepare_database()
        
        logger.info("Iniciando a limpeza e processamento dos dados...")

        clean_data()

        # Detecção e processamento de promoções
        detect_and_process_promotions()

        logger.info("Processo de inicialização do projeto concluído com sucesso.")
    except Exception as e:
        logger.error(f"Erro durante o processo de inicialização do projeto: {e}")

if __name__ == "__main__":
    main()
