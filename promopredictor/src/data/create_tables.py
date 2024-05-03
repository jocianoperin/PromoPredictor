from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def create_table_if_not_exists():
    """
    Cria tabelas no banco de dados se elas não existirem, verificando e informando o
    sucesso da criação de cada uma.
    """
    tables_created = []

    try:
        # Criando a tabela vendasprodutosexport com chave primária
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

        # Populando a tabela vendasprodutosexport com dados
        db_manager.execute_query("""
            INSERT INTO vendasprodutosexport (CodigoVenda, CodigoProduto, UNVenda, Quantidade, ValorTabela, ValorUnitario, ValorTotal, Desconto, CodigoSecao, CodigoGrupo, CodigoSubGrupo, CodigoFabricante, ValorCusto, ValorCustoGerencial, Cancelada, PrecoemPromocao)
            SELECT CodigoVenda, CodigoProduto, UNVenda, Quantidade, LEAST(ValorTabela, 999999.99), LEAST(ValorUnitario, 999999.99), LEAST(ValorTotal, 9999999.99), LEAST(Desconto, 9999999.99), CodigoSecao, CodigoGrupo, CodigoSubGrupo, CodigoFabricante, LEAST(ValorCusto, 999999.99), LEAST(ValorCustoGerencial, 999999.99), Cancelada, PrecoemPromocao FROM vendasprodutos;
        """)

        # Criando a tabela vendasexport com chave primária
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

        # Populando a tabela vendasexport com dados
        db_manager.execute_query("""
            INSERT INTO vendasexport (Codigo, Data, Hora, CodigoCliente, Status, TotalPedido, Endereco, Numero, Bairro, Cidade, UF, CEP, TotalCusto, Rentabilidade)
            SELECT Codigo, Data, Hora, CodigoCliente, Status, LEAST(TotalPedido, 9999999.99), Endereco, Numero, Bairro, Cidade, UF, CEP, LEAST(TotalCusto, 999999.99), LEAST(Rentabilidade, 999999.99) FROM vendas;
        """)

        # As outras tabelas já têm chaves primárias e são criadas corretamente.
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

        # Criando a tabela de configurações do modelo ARIMA
        db_manager.execute_query("""
            CREATE TABLE IF NOT EXISTS arima_model_config (
                model_id INT AUTO_INCREMENT PRIMARY KEY,
                product_id INT,
                value_column VARCHAR(255),
                p INT,
                d INT,
                q INT,
                aic FLOAT,
                bic FLOAT,
                date_executed DATETIME
            );
        """)
        tables_created.append("arima_model_config")

        logger.info(f"Tabelas verificadas/criadas com sucesso: {', '.join(tables_created)}.")

    except Exception as e:
        logger.error(f"Erro ao criar tabelas: {e}")

if __name__ == "__main__":
    create_table_if_not_exists()