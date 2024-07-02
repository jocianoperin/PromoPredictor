from src.services.tables_manager import create_tables, drop_tables, insert_data, configure_indexes
from src.services.data_cleaner import clean_null_values, remove_invalid_records, remove_duplicates
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def setup_database():
    """
    Limpa, cria tabelas e índices no banco de dados, se necessário.
    """
    drop_tables()
    logger.info("Banco de dados limpo com sucesso.")

    create_tables()
    logger.info("Tabelas criadas com sucesso.")

    insert_data()
    logger.info("Tabelas atualizadas com sucesso.")

    configure_indexes()
    logger.info("Índices criados com sucesso.")

def clean_and_process_data():
    """
    Limpa e processa os dados nas tabelas de vendasexport e vendasprodutosexport.
    """
    logger.info("Iniciando a limpeza e processamento dos dados...")

    # Remoção de registros inválidos
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


    # Remoção de duplicatas
    remove_duplicates("vendasexport")
    remove_duplicates("vendasprodutosexport")

    logger.info("Dados limpos com sucesso.")

def main():
    """
    Função principal que organiza a sequência de operações para inicialização do projeto.
    """
    try:
        logger.info("Iniciando o processo de inicialização do projeto...")

        # Dropar, criar e inserir dados nas tabelas necessárias
        setup_database()
        
        clean_and_process_data()

        logger.info("Processo de inicialização do projeto concluído com sucesso.")
    except Exception as e:
        logger.error(f"Erro durante o processo de inicialização do projeto: {e}")

if __name__ == "__main__":
    main()
