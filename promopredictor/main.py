from src.utils.logging_config import get_logger
from src.services.tables_manager import create_tables, drop_tables, insert_data, configure_indexes
from src.services.data_cleaner import remove_invalid_records
from src.services.indicadores_resumo_batch import process_data_and_insert as process_resumo
from src.services.indicadores_vendas_produtos import process_data_and_insert as process_indicadores_vendas
from src.services.data_formatter import check_data_types, standardize_formatting
from src.models.predict import make_predictions  # Adicionar a previsão ao pipeline

logger = get_logger(__name__)

def main():
    """
    Função principal para execução do pipeline de criação de tabelas, inserção de dados,
    validação e limpeza de registros, com logs detalhados.
    """

    """# 1. Dropar tabelas antigas
    logger.info("==== Início do processo ====")
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
    logger.info("Dados inseridos com sucesso.")"""

    # 4. Inserir os dados na tabela indicadores_vendas_produtos
    logger.info("4. Inserindo dados na tabela indicadores_vendas_produtos...")
    process_indicadores_vendas()
    logger.info("Dados inseridos na tabela indicadores_vendas_produtos com sucesso.")

    # 5. Configurar os índices nas tabelas
    logger.info("5. Configurando índices nas tabelas...")
    configure_indexes()
    logger.info("Índices configurados com sucesso.")

    """# 6. Validações e Limpeza de Dados
    logger.info("6. Iniciando validações de dados...")

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

    # 7. Processamento dos dados para indicadores de resumo
    logger.info("7. Processando dados de indicadores de vendas para resumo...")
    process_resumo()
    logger.info("Dados de indicadores de vendas processados e inseridos com sucesso.")

    # 8. Previsão e Inserção dos Dados Previstos
    logger.info("8. Realizando previsões e inserindo dados previstos...")
    make_predictions()  # Chamando a função de previsão para realizar e inserir previsões
    logger.info("Previsões realizadas e inseridas com sucesso.")"""

    logger.info("==== Processo finalizado com sucesso! ====")

if __name__ == "__main__":
    main()
