from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def create_tables():
    """
    Cria tabelas no banco de dados se elas não existirem.
    """
    tables_created = []

    try:
        # Criando a tabela vendasprodutosexport
        db_manager.execute_query("""
            CREATE TABLE IF NOT EXISTS vendasprodutosexport (
                ExportID INT AUTO_INCREMENT PRIMARY KEY,
                CodigoVenda INT,
                CodigoProduto INT,
                UNVenda VARCHAR(10),
                Quantidade DOUBLE,
                ValorTabela DECIMAL(15, 2),
                ValorUnitario DECIMAL(15, 2),
                ValorTotal DECIMAL(15, 2),
                Desconto DOUBLE,
                CodigoSecao INT,
                CodigoGrupo INT,
                CodigoSubGrupo INT,
                CodigoFabricante INT,
                ValorCusto DECIMAL(15, 2),
                ValorCustoGerencial DECIMAL(15, 2),
                Cancelada BOOLEAN,
                PrecoemPromocao BOOLEAN
            );
        """)
        tables_created.append("vendasprodutosexport")

        # Criando a tabela vendasexport
        db_manager.execute_query("""
            CREATE TABLE IF NOT EXISTS vendasexport (
                ExportID INT AUTO_INCREMENT PRIMARY KEY,
                Codigo INT,
                Data DATE,
                Hora TIME,
                CodigoCliente INT,
                Status VARCHAR(10),
                TotalPedido DECIMAL(15, 2),
                Endereco VARCHAR(255),
                Numero VARCHAR(50),
                Bairro VARCHAR(100),
                Cidade VARCHAR(100),
                UF VARCHAR(2),
                CEP VARCHAR(10),
                TotalCusto DECIMAL(15, 2),
                Rentabilidade DECIMAL(15, 2)
            );
        """)
        tables_created.append("vendasexport")

        # Criando a tabela promotions_identified
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

        # Criando a tabela sales_indicators
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

        # Criando a tabela price_forecasts
        db_manager.execute_query("""
            CREATE TABLE IF NOT EXISTS price_forecasts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                CodigoProduto INT NOT NULL,
                Data DATE NOT NULL,
                ValorUnitario DECIMAL(10, 2) NOT NULL,
                PrevisaoARIMA DECIMAL(10, 2),
                PrevisaoRNN DECIMAL(10, 2),
                UNIQUE KEY unique_forecast (CodigoProduto, Data)
            );
        """)
        tables_created.append("price_forecasts")

        # Criando a tabela model_config para armazenar configurações de diferentes modelos
        db_manager.execute_query("""
            CREATE TABLE IF NOT EXISTS model_config (
                config_id INT AUTO_INCREMENT PRIMARY KEY,
                product_id INT NOT NULL,
                export_id INT,
                value_column VARCHAR(255),
                model_type VARCHAR(50),
                parameters TEXT,
                aic FLOAT,
                bic FLOAT,
                date_executed DATETIME,
                INDEX idx_product (product_id),
                INDEX idx_export (export_id)
            );
        """)
        tables_created.append("model_config")

        logger.info(f"Tabelas verificadas/criadas com sucesso: {', '.join(tables_created)}.")

    except Exception as e:
        logger.error(f"Erro ao criar tabelas: {e}")

def drop_tables():
    """
    Exclui todas as tabelas no banco de dados.
    """
    try:
        db_manager.execute_query("DROP TABLE IF EXISTS sales_indicators")
        logger.info("Tabela sales_indicators excluída com sucesso.")

        db_manager.execute_query("DROP TABLE IF EXISTS promotions_identified")
        logger.info("Tabela promotions_identified excluída com sucesso.")

        db_manager.execute_query("DROP TABLE IF EXISTS vendasexport")
        logger.info("Tabela vendasexport excluída com sucesso.")

        db_manager.execute_query("DROP TABLE IF EXISTS vendasprodutosexport")
        logger.info("Tabela vendasprodutosexport excluída com sucesso.")

        db_manager.execute_query("DROP TABLE IF EXISTS price_forecasts")
        logger.info("Tabela price_forecasts excluída com sucesso.")

        db_manager.execute_query("DROP TABLE IF EXISTS model_config")
        logger.info("Tabela model_config excluída com sucesso.")

    except Exception as e:
        logger.error(f"Erro ao excluir tabelas: {e}")

def insert_data():
    """
    Insere dados nas tabelas vendasprodutosexport e vendasexport.
    """
    try:
        # Populando a tabela vendasprodutosexport com dados
        db_manager.execute_query("""
            INSERT INTO vendasprodutosexport (CodigoVenda, CodigoProduto, UNVenda, Quantidade, ValorTabela, ValorUnitario, ValorTotal, Desconto, CodigoSecao, CodigoGrupo, CodigoSubGrupo, CodigoFabricante, ValorCusto, ValorCustoGerencial, Cancelada, PrecoemPromocao)
            SELECT CodigoVenda, CodigoProduto, UNVenda, Quantidade, LEAST(ValorTabela, 999999.99), LEAST(ValorUnitario, 999999.99), LEAST(ValorTotal, 9999999.99), LEAST(Desconto, 9999999.99), CodigoSecao, CodigoGrupo, CodigoSubGrupo, CodigoFabricante, LEAST(ValorCusto, 999999.99), LEAST(ValorCustoGerencial, 999999.99), Cancelada, PrecoemPromocao FROM vendasprodutos;
        """)
        logger.info("Dados inseridos com sucesso na tabela vendasprodutosexport.")

        # Populando a tabela vendasexport com dados
        db_manager.execute_query("""
            INSERT INTO vendasexport (Codigo, Data, Hora, CodigoCliente, Status, TotalPedido, Endereco, Numero, Bairro, Cidade, UF, CEP, TotalCusto, Rentabilidade)
            SELECT Codigo, Data, Hora, CodigoCliente, Status, LEAST(TotalPedido, 9999999.99), Endereco, Numero, Bairro, Cidade, UF, CEP, LEAST(TotalCusto, 999999.99), LEAST(Rentabilidade, 999999.99) FROM vendas;
        """)
        logger.info("Dados inseridos com sucesso na tabela vendasexport.")

    except Exception as e:
        logger.error(f"Erro ao inserir dados: {e}")

if __name__ == "__main__":
   # Exemplo de uso
   create_tables()
   insert_data()
   # drop_tables()