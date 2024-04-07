# src/data/create_tables.py
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def create_table_if_not_exists():
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS vendasprodutosexport AS 
                SELECT CodigoVenda, CodigoProduto, UNVenda, Quantidade, ValorTabela, ValorUnitario, ValorTotal, Desconto, CodigoSecao, CodigoGrupo, CodigoSubGrupo, CodigoFabricante, ValorCusto, ValorCustoGerencial, Cancelada, PrecoemPromocao FROM vendasprodutos;
            """)
            logger.info("Tabela 'vendasprodutosexport' verificada/criada com sucesso.")

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS vendasexport AS 
                SELECT Codigo, Data, Hora, CodigoCliente, Status, TotalPedido, Endereco, Numero, Bairro, Cidade, UF, CEP, TotalCusto, Rentabilidade FROM vendas;
            """)
            logger.info("Tabela 'vendasexport' verificada/criada com sucesso.")

            cursor.execute("""
                    CREATE TABLE IF NOT EXISTS promotions_identified (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        CodigoProduto INT NOT NULL,
                        Data DATE NOT NULL,
                        ValorUnitario DECIMAL(10, 2) NOT NULL,
                        ValorTabela DECIMAL(10, 2) NOT NULL,
                        UNIQUE KEY unique_promocao (CodigoProduto, Data)
                    );
                """)
            logger.info("Tabela 'promotions_identified' verificada/criada com sucesso.")

            cursor.execute("""
                    CREATE TABLE IF NOT EXISTS sales_indicators (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        CodigoProduto INT NOT NULL,
                        QuantidadeTotal INT,
                        ValorTotalVendido DECIMAL(10, 2),
                        UNIQUE KEY unique_indicator (CodigoProduto)
                    );
                """)
            logger.info("Tabela 'sales_indicators' criada com sucesso.")

            connection.commit()
        except Exception as e:
            logger.error(f"Erro ao criar tabelas: {e}")
            connection.rollback()
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                logger.info("Conexão com o banco de dados fechada.")
    else:
        logger.error("Falha ao estabelecer conexão com o banco de dados para criar tabelas.")

if __name__ == "__main__":
    create_table_if_not_exists()
