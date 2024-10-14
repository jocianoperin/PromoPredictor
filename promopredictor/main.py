from src.services.database import db_manager
from src.utils.logging_config import get_logger
from src.services.tables_manager import create_tables, drop_tables, insert_data, configure_indexes
from src.services.data_cleaner import remove_invalid_records, clean_null_values, remove_duplicates
from src.services.indicadores_resumo_batch import process_data_and_insert as process_resumo
from src.services.indicadores_vendas_produtos import process_data_and_insert as process_indicadores_vendas
from src.services.data_formatter import check_data_types, standardize_formatting

logger = get_logger(__name__)

def main():
    """
    Função principal para execução do pipeline de criação de tabelas, inserção de dados,
    validação e limpeza de registros, com logs detalhados.
    """

    # 1. Dropar tabelas antigas
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
    logger.info("Dados inseridos com sucesso.")

    # 4. Inserir os dados na tabela indicadores_vendas_produtos
    logger.info("4. Inserindo dados na tabela indicadores_vendas_produtos...")
    process_indicadores_vendas()
    logger.info("Dados inseridos na tabela indicadores_vendas_produtos com sucesso.")

    # 5. Configurar os índices nas tabelas
    logger.info("5. Configurando índices nas tabelas...")
    configure_indexes()
    logger.info("Índices configurados com sucesso.")

    # 6. Validações e Limpeza de Dados
    logger.info("6. Iniciando validações de dados...")

    # Remover registros inválidos da tabela vendas_auxiliar
    logger.info("Removendo registros inválidos da tabela 'vendas_auxiliar'...")
    remove_invalid_records("vendas_auxiliar", [
        "DATA IS NULL"
    ])
    logger.info("Registros inválidos removidos de 'vendas_auxiliar'.")

    # Remover registros inválidos da tabela vendasprodutos_auxiliar
    logger.info("Removendo registros inválidos da tabela 'vendasprodutos_auxiliar'...")
    remove_invalid_records("vendasprodutos_auxiliar", [
        "Quantidade IS NULL OR Quantidade <= 0",
        "ValorTotal IS NULL OR ValorTotal <= 0",
        "Promocao IS NULL OR Promocao < 0"
    ])
    logger.info("Registros inválidos removidos de 'vendasprodutos_auxiliar'.")

    # Remover registros inválidos da tabela produtosmaisvendidos
    logger.info("Removendo registros inválidos da tabela 'produtosmaisvendidos'...")
    remove_invalid_records("produtosmaisvendidos", [
        "QuantidadeTotalVendida IS NULL OR QuantidadeTotalVendida <= 0"
    ])
    logger.info("Registros inválidos removidos de 'produtosmaisvendidos'.")

    # Remover registros inválidos da tabela indicadores_vendas_produtos
    logger.info("Removendo registros inválidos da tabela 'indicadores_vendas_produtos'...")
    remove_invalid_records("indicadores_vendas_produtos", [
        "Quantidade IS NULL OR Quantidade <= 0",
        "ValorTotal IS NULL OR ValorTotal <= 0"
    ])
    logger.info("Registros inválidos removidos de 'indicadores_vendas_produtos'.")

    # 7. Verificação de Tipos de Dados
    logger.info("7. Verificando tipos de dados nas tabelas...")

    # Verificação de tipos de dados para vendasprodutos_auxiliar
    logger.info("Verificando tipos de dados da tabela 'vendasprodutos_auxiliar'...")
    column_types_vendasprodutos_auxiliar = {
        'Quantidade': 'DOUBLE',
        'ValorTotal': 'DOUBLE',
        'Promocao': 'DECIMAL(3,0)'
    }
    check_data_types('vendasprodutos_auxiliar', column_types_vendasprodutos_auxiliar)
    logger.info("Tipos de dados da tabela 'vendasprodutos_auxiliar' verificados e ajustados.")

    # Verificação de tipos de dados para vendas_auxiliar
    logger.info("Verificando tipos de dados da tabela 'vendas_auxiliar'...")
    column_types_vendas_auxiliar = {
        'DATA': 'DATE'
    }
    check_data_types('vendas_auxiliar', column_types_vendas_auxiliar)
    logger.info("Tipos de dados da tabela 'vendas_auxiliar' verificados e ajustados.")

    # Verificação de tipos de dados para produtosmaisvendidos
    logger.info("Verificando tipos de dados da tabela 'produtosmaisvendidos'...")
    column_types_produtosmaisvendidos = {
        'QuantidadeTotalVendida': 'DOUBLE'
    }
    check_data_types('produtosmaisvendidos', column_types_produtosmaisvendidos)
    logger.info("Tipos de dados da tabela 'produtosmaisvendidos' verificados e ajustados.")

    # Verificação de tipos de dados para indicadores_vendas_produtos
    logger.info("Verificando tipos de dados da tabela 'indicadores_vendas_produtos'...")
    column_types_indicadores_vendas_produtos = {
        'Quantidade': 'DOUBLE',
        'ValorTotal': 'DOUBLE',
        'Promocao': 'DECIMAL(3,0)'
    }
    check_data_types('indicadores_vendas_produtos', column_types_indicadores_vendas_produtos)
    logger.info("Tipos de dados da tabela 'indicadores_vendas_produtos' verificados e ajustados.")

    # 8. Padronização de Formatação
    logger.info("8. Padronizando formatação de dados...")

    # Padronizar a formatação da tabela vendasprodutos_auxiliar
    logger.info("Padronizando formatação da tabela 'vendasprodutos_auxiliar'...")
    formatting_rules_vendasprodutos_auxiliar = {
        'Promocao': 'ROUND'
    }
    standardize_formatting('vendasprodutos_auxiliar', formatting_rules_vendasprodutos_auxiliar)
    logger.info("Formatação padronizada na tabela 'vendasprodutos_auxiliar'.")

    # 9. Remoção de Duplicatas
    logger.info("9. Removendo registros duplicados...")

    logger.info("Removendo duplicatas da tabela 'vendasprodutos_auxiliar'...")
    remove_duplicates('vendasprodutos_auxiliar')
    logger.info("Duplicatas removidas da tabela 'vendasprodutos_auxiliar'.")

    logger.info("Removendo duplicatas da tabela 'indicadores_vendas_produtos'...")
    remove_duplicates('indicadores_vendas_produtos')
    logger.info("Duplicatas removidas da tabela 'indicadores_vendas_produtos'.")

    logger.info("Removendo duplicatas da tabela 'produtosmaisvendidos'...")
    remove_duplicates('produtosmaisvendidos')
    logger.info("Duplicatas removidas da tabela 'produtosmaisvendidos'.")

    logger.info("==== Processo finalizado com sucesso! ====")

    # 10. Processamento dos dados para indicadores de resumo
    logger.info("10. Processando dados de indicadores de vendas para resumo...")
    process_resumo()
    logger.info("Dados de indicadores de vendas processados e inseridos com sucesso.")

    logger.info("==== Processo finalizado com sucesso! ====")


if __name__ == "__main__":
    main()
