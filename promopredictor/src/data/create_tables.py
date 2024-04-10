# src/data/create_tables.py
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def create_table_if_not_exists():
    try:
        connection = get_db_connection()
        if connection:
            with connection.cursor() as cursor:
                tables_created = []
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS vendasprodutosexport AS 
                    SELECT CodigoVenda, CodigoProduto, UNVenda, Quantidade, ValorTabela, ValorUnitario, ValorTotal, Desconto, CodigoSecao, CodigoGrupo, CodigoSubGrupo, CodigoFabricante, ValorCusto, ValorCustoGerencial, Cancelada, PrecoemPromocao FROM vendasprodutos;
                """)
                tables_created.append("vendasprodutosexport")

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS vendasexport AS 
                    SELECT Codigo, Data, Hora, CodigoCliente, Status, TotalPedido, Endereco, Numero, Bairro, Cidade, UF, CEP, TotalCusto, Rentabilidade FROM vendas;
                """)
                tables_created.append("vendasexport")

                cursor.execute("""
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

                cursor.execute("""
                        CREATE TABLE IF NOT EXISTS sales_indicators (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            CodigoProduto INT NOT NULL,
                            QuantidadeTotal INT,
                            DataInicioPromocao DATE NOT NULL,
                            DataFimPromocao DATE NOT NULL,
                            ValorTotalVendido DECIMAL(10, 2),
                            UNIQUE KEY unique_indicator (CodigoProduto)
                        );
                    """)
                tables_created.append("sales_indicators")

                connection.commit()
                logger.info(f"Tabelas verificadas/criadas com sucesso: {', '.join(tables_created)}.")

    except Exception as e:
        logger.error(f"Erro ao criar tabelas: {e}")
        if connection:
            connection.rollback()
    finally:
        if connection:
            connection.close()
            logger.debug("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    create_table_if_not_exists()