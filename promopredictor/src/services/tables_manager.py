from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def create_tables():
    """
    Cria tabelas no banco de dados se elas não existirem.
    """
    tables_to_create = [
        {
            "name": "vendasprodutosexport",
            "query": """
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
                    ValorCusto DECIMAL(15, 2),
                    ValorCustoGerencial DECIMAL(15, 2),
                    Cancelada BOOLEAN,
                    PrecoemPromocao BOOLEAN
                );
            """
        },
        {
            "name": "vendasexport",
            "query": """
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
            """
        },
        {
            "name": "promotions_identified",
            "query": """
                CREATE TABLE IF NOT EXISTS promotions_identified (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    CodigoProduto INT NOT NULL,
                    DataInicioPromocao DATE NOT NULL,
                    DataFimPromocao DATE NOT NULL,
                    ValorUnitario DECIMAL(10, 2) NOT NULL,
                    ValorCusto DECIMAL(10, 2) NOT NULL,
                    ValorTabela DECIMAL(10, 2) NOT NULL,
                    UNIQUE KEY unique_promocao (CodigoProduto, DataInicioPromocao, DataFimPromocao)
                );
            """
        },
        {
            "name": "sales_indicators",
            "query": """
                CREATE TABLE IF NOT EXISTS sales_indicators (
                    id SERIAL PRIMARY KEY,
                    PromotionId INT NOT NULL,
                    CodigoProduto INT NOT NULL,
                    DataInicioPromocao DATE NOT NULL,
                    DataFimPromocao DATE NOT NULL,
                    QuantidadeTotal INT NOT NULL,
                    ValorTotalVendido DECIMAL(10, 2) NOT NULL,
                    ValorCusto DECIMAL(10, 2) NOT NULL,
                    TotalVendaCompleta DECIMAL(15, 2) NOT NULL,
                    ValorTabela DECIMAL(10, 2) NOT NULL,
                    ValorUnitarioVendido DECIMAL(10, 2) NOT NULL,
                    TicketMedio DECIMAL(10, 2) NOT NULL,
                    MargemLucro DECIMAL(10, 2) NOT NULL,
                    PercentualDescontoMedio DECIMAL(5, 2) NOT NULL,
                    ElasticidadePrecoDemanda DECIMAL(10, 2) DEFAULT NULL,
                    EstoqueMedioAntesPromocao DOUBLE DEFAULT NULL,
                    EstoqueNoDiaPromocao DOUBLE DEFAULT NULL,
                    ImpactoEmOutrasCategorias DECIMAL(10, 2) DEFAULT NULL,
                    VolumeVendasPosPromocao DOUBLE DEFAULT NULL,
                    ComparacaoComPromocoesPassadas DOUBLE DEFAULT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (PromotionId) REFERENCES promotions_identified(id)
                );
            """
        },
        {
            "name": "produtosexport",
            "query": """
                CREATE TABLE IF NOT EXISTS produtosexport (
                    Codigo INT(10) UNSIGNED PRIMARY KEY,
                    CodigoBarras VARCHAR(15),
                    Referencia VARCHAR(30),
                    DataCadastro DATE,
                    Descricao VARCHAR(120),
                    CodigoFabricante INT(11),
                    Fabricante VARCHAR(30),
                    Detalhamento VARCHAR(250),
                    UNVenda VARCHAR(6),
                    UNCompra VARCHAR(6),
                    QuantidadeCX DOUBLE,
                    EstoqueMinimo DOUBLE,
                    EstoqueIdeal DOUBLE,
                    ValorCusto DOUBLE,
                    OutrosCustos DOUBLE,
                    CustoFinal DOUBLE,
                    PercentualT1 DOUBLE,
                    VendaT1 DOUBLE,
                    CodigoCadeiaPreco INT(10) UNSIGNED,
                    CadeiaPreco VARCHAR(75),
                    CodigoSecao INT(10) UNSIGNED,
                    Secao VARCHAR(30),
                    CodigoGrupo INT(10) UNSIGNED,
                    Grupo VARCHAR(30),
                    CodigoSubGrupo INT(10) UNSIGNED,
                    SubGrupo VARCHAR(30),
                    Status VARCHAR(1),
                    NComercializavel TINYINT(1) UNSIGNED
                );
            """
        },
        {
            "name": "auditoriaestoquexport",
            "query": """
                CREATE TABLE IF NOT EXISTS auditoriaestoquexport (
                    ID INT AUTO_INCREMENT PRIMARY KEY,
                    DataHora DATETIME,
                    CodigoProduto INT(10) UNSIGNED,
                    EstoqueAtual DOUBLE
                );
            """
        }
    ]

    for table in tables_to_create:
        try:
            db_manager.execute_query(table["query"])
            logger.info(f"Tabela '{table['name']}' verificada/criada com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao criar/verificar a tabela '{table['name']}': {e}")


def drop_tables():
    """
    Exclui todas as tabelas no banco de dados.
    """
    tables_to_drop = [
        "sales_indicators",
        "promotions_identified",
        "vendasexport",
        "vendasprodutosexport",
        "produtosexport",
        "auditoriaestoquexport"
    ]

    for table in tables_to_drop:
        try:
            db_manager.execute_query(f"DROP TABLE IF EXISTS {table}")
            logger.info(f"Tabela '{table}' excluída com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao excluir a tabela '{table}': {e}")


def insert_data():
    """
    Insere dados nas tabelas vendasprodutosexport e vendasexport.
    """
    insert_queries = [
        {
            "table": "vendasprodutosexport",
            "query": """
                INSERT INTO vendasprodutosexport (
                    CodigoVenda, CodigoProduto, UNVenda, Quantidade, ValorTabela, 
                    ValorUnitario, ValorTotal, Desconto, ValorCusto, ValorCustoGerencial, 
                    Cancelada, PrecoemPromocao
                ) 
                SELECT 
                    CodigoVenda, CodigoProduto, UNVenda, Quantidade, 
                    LEAST(ValorTabela, 999999.99), LEAST(ValorUnitario, 999999.99), 
                    LEAST(ValorTotal, 9999999.99), LEAST(Desconto, 9999999.99), 
                    LEAST(ValorCusto, 999999.99), LEAST(ValorCustoGerencial, 999999.99), 
                    Cancelada, PrecoemPromocao 
                FROM vendasprodutos;
            """
        },
        {
            "table": "vendasexport",
            "query": """
                INSERT INTO vendasexport (
                    Codigo, Data, Hora, CodigoCliente, Status, 
                    TotalPedido, Endereco, Numero, Bairro, Cidade, 
                    UF, CEP, TotalCusto, Rentabilidade
                ) 
                SELECT 
                    Codigo, Data, Hora, CodigoCliente, Status, 
                    LEAST(TotalPedido, 9999999.99), Endereco, Numero, Bairro, Cidade, 
                    UF, CEP, LEAST(TotalCusto, 999999.99), LEAST(Rentabilidade, 999999.99) 
                FROM vendas;
            """
        },
        {
            "table": "produtosexport",
            "query": """
                INSERT INTO produtosexport (
                    Codigo, CodigoBarras, Referencia, DataCadastro, Descricao, 
                    CodigoFabricante, Fabricante, Detalhamento, UNVenda, UNCompra, 
                    QuantidadeCX, EstoqueMinimo, EstoqueIdeal, ValorCusto, OutrosCustos, 
                    CustoFinal, PercentualT1, VendaT1, CodigoCadeiaPreco, CadeiaPreco, 
                    CodigoSecao, Secao, CodigoGrupo, Grupo, CodigoSubGrupo, SubGrupo, 
                    Status, NComercializavel
                )
                SELECT 
                    Codigo, CodigoBarras, Referencia, DataCadastro, Descricao, 
                    CodigoFabricante, Fabricante, Detalhamento, UNVenda, UNCompra, 
                    QuantidadeCX, EstoqueMinimo, EstoqueIdeal, ValorCusto, OutrosCustos, 
                    CustoFinal, PercentualT1, VendaT1, CodigoCadeiaPreco, CadeiaPreco, 
                    CodigoSecao, Secao, CodigoGrupo, Grupo, CodigoSubGrupo, SubGrupo, 
                    Status, NComercializavel
                FROM produtos;
            """
        },
        {
            "table": "auditoriaestoquexport",
            "query": """
                INSERT INTO auditoriaestoquexport (
                    DataHora, CodigoProduto, EstoqueAtual
                )
                SELECT 
                    DataHora, CodigoProduto, EstoqueAtual
                FROM auditoriaestoque;
            """
        }
    ]

    for query in insert_queries:
        try:
            db_manager.execute_query(query["query"])
            logger.info(f"Dados inseridos com sucesso na tabela '{query['table']}'.")
        except Exception as e:
            logger.error(f"Erro ao inserir dados na tabela '{query['table']}': {e}")


def configure_indexes():
    """
    Configura os índices nas tabelas de vendas.
    """
    indexes = [
        {"name": "idx_codigo", "table": "vendasexport", "columns": "Codigo"},
        {"name": "idx_data", "table": "vendasexport", "columns": "Data"},
        {"name": "idx_codigocliente", "table": "vendasexport", "columns": "CodigoCliente"},
        {"name": "idx_data_codigocliente", "table": "vendasexport", "columns": "Data, CodigoCliente"},
        {"name": "idx_totalpedido", "table": "vendasexport", "columns": "TotalPedido"},
        {"name": "idx_vendasprodutosexport_codigovenda", "table": "vendasprodutosexport", "columns": "CodigoVenda"},
        {"name": "idx_vendasprodutosexport_codigoproduto", "table": "vendasprodutosexport", "columns": "CodigoProduto"},
        {"name": "idx_vendasprodutosexport_valorunitario", "table": "vendasprodutosexport", "columns": "ValorUnitario"},
        {"name": "idx_vendasprodutosexport_quantidade", "table": "vendasprodutosexport", "columns": "Quantidade"},
        {"name": "idx_vendasprodutosexport_desconto", "table": "vendasprodutosexport", "columns": "Desconto"},
        {"name": "idx_vendasprodutosexport_precoempromocao", "table": "vendasprodutosexport", "columns": "PrecoemPromocao"},
        {"name": "idx_codigo_produtosexport", "table": "produtosexport", "columns": "Codigo"},
        {"name": "idx_codigoproduto_auditoriaestoquexport", "table": "auditoriaestoquexport", "columns": "CodigoProduto"},
        {"name": "idx_data_auditoriaestoquexport", "table": "auditoriaestoquexport", "columns": "DataHora"},
        {"name": "idx_vendasprodutosexport_codigoproduto_codigovenda", "table": "vendasprodutosexport", "columns": "CodigoProduto, CodigoVenda"},
        {"name": "idx_auditoriaestoquexport_codigoproduto_datahora", "table": "auditoriaestoquexport", "columns": "CodigoProduto, DataHora"},
        {"name": "idx_sales_indicators_codigoproduto_datafimpromocao", "table": "sales_indicators", "columns": "CodigoProduto, DataFimPromocao"}
    ]

    for index in indexes:
        try:
            db_manager.execute_query(f"CREATE INDEX IF NOT EXISTS {index['name']} ON {index['table']} ({index['columns']})")
            logger.info(f"Índice '{index['name']}' criado com sucesso na tabela '{index['table']}'.")
        except Exception as e:
            logger.error(f"Erro ao criar índice '{index['name']}' na tabela '{index['table']}': {e}")