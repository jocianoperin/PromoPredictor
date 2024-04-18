from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def create_table_if_not_exists():
    """
    Cria tabelas no banco de dados se elas não existirem.
    Utiliza a abstração DatabaseManager para gerenciar a conexão e execução de consultas SQL.
    """
    tables_created = []

    try:
        # Criando tabelas usando a abstração do DatabaseManager
        db_manager.execute_query("""
            CREATE TABLE IF NOT EXISTS vendasprodutosexport AS 
            SELECT CodigoVenda, CodigoProduto, UNVenda, Quantidade, ValorTabela, ValorUnitario, ValorTotal, Desconto, CodigoSecao, CodigoGrupo, CodigoSubGrupo, CodigoFabricante, ValorCusto, ValorCustoGerencial, Cancelada, PrecoemPromocao FROM vendasprodutos;
        """)
        tables_created.append("vendasprodutosexport")

        db_manager.execute_query("""
            CREATE TABLE IF NOT EXISTS vendasexport AS 
            SELECT Codigo, Data, Hora, CodigoCliente, Status, TotalPedido, Endereco, Numero, Bairro, Cidade, UF, CEP, TotalCusto, Rentabilidade FROM vendas;
        """)
        tables_created.append("vendasexport")

        db_manager.execute_query("""
            CREATE TABLE IF NOT EXISTS promotions_identified (
                id INT AUTO_INCREMENT PRIMARY KEY,
                CodigoProduto INT NOT NULL,
                DataInicioPromocao DATE NOT NULL,
                DataFimPromocao DATE NOT NULL,
                ValorUnitario DECIMAL(10, 2) NOT NULL,
                ValorTabela DECIMAL(10, 2) NOT NULL,
                UNIQUE KEY unique_promocao (CodigoProduto)
            );
        """)
        tables_created.append("promotions_identified")

        db_manager.execute_query("""
            CREATE TABLE IF NOT EXISTS sales_indicators (
                id INT AUTO_INCREMENT PRIMARY KEY,
                CodigoProduto INT NOT NULL,
                DataInicioPromocao DATE NOT NULL,
                DataFimPromocao DATE NOT NULL,
                QuantidadeTotal INT,
                ValorTotalVendido DECIMAL(10, 2),
                UNIQUE KEY unique_indicator (CodigoProduto, DataInicioPromocao, DataFimPromocao)
            );
        """)
        tables_created.append("sales_indicators")

        logger.info(f"Tabelas verificadas/criadas com sucesso: {', '.join(tables_created)}.")

    except Exception as e:
        logger.error(f"Erro ao criar tabelas: {e}")

if __name__ == "__main__":
    create_table_if_not_exists()