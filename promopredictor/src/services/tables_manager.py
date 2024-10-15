from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def create_tables():
    """
    Cria tabelas no banco de dados se elas não existirem.
    """
    tables_to_create = [
        {
            "name": "calendario",
            "query": """
                CREATE TABLE IF NOT EXISTS calendario (
                    data DATE PRIMARY KEY
                );
            """
        },
        {
            "name": "vendas_auxiliar",
            "query": """
                CREATE TABLE IF NOT EXISTS vendas_auxiliar (
                    Codigo INT(10) UNSIGNED,
                    DATA DATE,
                    PRIMARY KEY (Codigo)
                );
            """
        },
        {
            "name": "vendasprodutos_auxiliar",
            "query": """
                CREATE TABLE IF NOT EXISTS vendasprodutos_auxiliar (
                    Sequencia INT(10) UNSIGNED,
                    CodigoVenda INT(10) UNSIGNED,
                    CodigoProduto INT(10) UNSIGNED,
                    CodigoSecao INT(10) UNSIGNED,
                    CodigoGrupo INT(10) UNSIGNED,
                    CodigoSubGrupo INT(10) UNSIGNED,
                    CodigoSupermercado INT(1),
                    Quantidade DOUBLE,
                    ValorTotal DOUBLE,
                    Promocao DECIMAL(3,0),
                    PRIMARY KEY (Sequencia)
                );
            """
        },
        {
            "name": "produtosmaisvendidos",
            "query": """
                CREATE TABLE IF NOT EXISTS produtosmaisvendidos (
                    CodigoProduto INT(10) UNSIGNED PRIMARY KEY,
                    QuantidadeTotalVendida DOUBLE DEFAULT 0
                );
            """
        },
        {
            "name": "indicadores_vendas_produtos",
            "query": """
                CREATE TABLE IF NOT EXISTS indicadores_vendas_produtos (
                    DATA DATE,
                    CodigoVenda INT(10) UNSIGNED,
                    CodigoProduto INT(10) UNSIGNED,
                    CodigoSecao INT(10) UNSIGNED,
                    CodigoGrupo INT(10) UNSIGNED,
                    CodigoSubGrupo INT(10) UNSIGNED,
                    CodigoSupermercado INT(1),
                    Quantidade DOUBLE,
                    ValorTotal DOUBLE,
                    Promocao DECIMAL(3,0),
                    PRIMARY KEY (DATA, CodigoProduto, CodigoVenda)
                );
            """
        },
        {
            "name": "indicadores_vendas_produtos_resumo",
            "query": """
                CREATE TABLE IF NOT EXISTS indicadores_vendas_produtos_resumo (
                    DATA DATE NOT NULL,
                    CodigoProduto INT(10) UNSIGNED NOT NULL,
                    CodigoSecao INT(10) UNSIGNED NULL,
                    CodigoGrupo INT(10) UNSIGNED NULL,
                    CodigoSubGrupo INT(10) UNSIGNED NULL,
                    CodigoSupermercado INT(1) NULL,
                    TotalUNVendidas DOUBLE DEFAULT 0,
                    ValorTotalVendido DOUBLE DEFAULT 0,
                    Promocao DECIMAL(3,0) DEFAULT 0,
                    PRIMARY KEY (DATA, CodigoProduto)
                );
            """
        },
        {
            "name": "indicadores_vendas_produtos_previsoes",
            "query": """
                CREATE TABLE IF NOT EXISTS indicadores_vendas_produtos_previsoes (
                    DATA DATE NOT NULL,
                    CodigoProduto INT(10) UNSIGNED NOT NULL,
                    TotalUNVendidas DOUBLE,
                    ValorTotalVendido DOUBLE,
                    Promocao DECIMAL(3, 0),
                    PRIMARY KEY (DATA, CodigoProduto)
                );
            """
        }
    ]

    for table in tables_to_create:
        try:
            db_manager.execute_query(table["query"])
            logger.info(f"Tabela '{table['name']}' criada com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao criar tabela '{table['name']}': {e}")


def drop_tables():
    """
    Exclui todas as tabelas no banco de dados.
    """
    tables_to_drop = [
        "calendario",
        "vendas_auxiliar",
        "vendasprodutos_auxiliar",
        "produtosmaisvendidos",
        "indicadores_vendas_produtos",
        "indicadores_vendas_produtos_resumo",
        "indicadores_vendas_produtos_previsoes"
    ]

    for table in tables_to_drop:
        try:
            db_manager.execute_query(f"DROP TABLE IF EXISTS {table}")
            logger.info(f"Tabela '{table}' excluída com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao excluir tabela '{table}': {e}")


def insert_data():
    """
    Insere dados nas tabelas vendasprodutosexport e vendasexport.
    """
    insert_queries = [
        
        {
        "table": "calendario",
            "query": """
                INSERT INTO calendario (data)
                SELECT DATE_ADD('2019-01-01', INTERVAL t4.num * 10000 + t3.num * 1000 + t2.num * 100 + t1.num * 10 + t0.num DAY)
                FROM 
                  (SELECT 0 AS num UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t0,
                  (SELECT 0 AS num UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t1,
                  (SELECT 0 AS num UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t2,
                  (SELECT 0 AS num UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t3,
                  (SELECT 0 AS num UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t4
                WHERE DATE_ADD('2019-01-01', INTERVAL t4.num * 10000 + t3.num * 1000 + t2.num * 100 + t1.num * 10 + t0.num DAY) < '2024-07-01';
            """
        },
        {
            "table": "vendas_auxiliar",
            "query": """
                INSERT INTO vendas_auxiliar (Codigo, DATA)
                SELECT Codigo, DATA FROM vendas;
            """
        },
        {
            "table": "vendasprodutos_auxiliar",
            "query": """
                INSERT INTO vendasprodutos_auxiliar (
                    Sequencia, CodigoVenda, CodigoProduto, CodigoSecao, CodigoGrupo, 
                    CodigoSubGrupo, CodigoSupermercado, Quantidade, ValorTotal, Promocao
                )
                SELECT 
                    vp.Sequencia, vp.CodigoVenda, vp.CodigoProduto, vp.CodigoSecao, 
                    vp.CodigoGrupo, vp.CodigoSubGrupo, 1 AS CodigoSupermercado, 
                    vp.Quantidade, vp.ValorTotal, IFNULL(vp.precoempromocao, 0) AS Promocao
                FROM vendasprodutos vp;
            """
        },
        {
            "table": "produtosmaisvendidos",
            "query": """
                INSERT INTO produtosmaisvendidos (CodigoProduto, QuantidadeTotalVendida)
                SELECT CodigoProduto, SUM(Quantidade) AS QuantidadeTotalVendida 
                FROM vendasprodutos_auxiliar 
                GROUP BY CodigoProduto 
                ORDER BY QuantidadeTotalVendida DESC 
                LIMIT 2000;
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
        {"name": "idx_vendas_auxiliar", "table": "vendas_auxiliar", "columns": "Codigo"},
        {"name": "idx_vendas_auxiliar_data", "table": "vendas_auxiliar", "columns": "DATA"},
        {"name": "idx_vendasprodutos_auxiliar", "table": "vendasprodutos_auxiliar", "columns": "Sequencia"},
        {"name": "idx_vendasprodutos_auxiliar_CodigoProduto", "table": "vendasprodutos_auxiliar", "columns": "CodigoProduto"},
        {"name": "idx_vendasprodutos_auxiliar_CodigoVenda", "table": "vendasprodutos_auxiliar", "columns": "CodigoVenda"},
        {"name": "idx_produtosmaisvendidos", "table": "produtosmaisvendidos", "columns": "CodigoProduto"},
        {"name": "idx_indicadores_vendas_produtos_codigo", "table": "indicadores_vendas_produtos", "columns": "CodigoProduto"},
        {"name": "idx_indicadores_vendas_produtos_data", "table": "indicadores_vendas_produtos", "columns": "DATA"},
        {"name": "idx_indicadores_vendas_produtos_DATA_CodigoProduto", "table": "indicadores_vendas_produtos", "columns": "DATA, CodigoProduto"},
        {"name": "idx_indicadores_vendas_produtos_resumo_codigo", "table": "indicadores_vendas_produtos_resumo", "columns": "CodigoProduto"},
        {"name": "idx_indicadores_vendas_produtos_resumo_data", "table": "indicadores_vendas_produtos_resumo", "columns": "DATA"},
        {"name": "idx_indicadores_vendas_produtos_resumo", "table": "indicadores_vendas_produtos_resumo", "columns": "CodigoProduto, DATA"}
    ]

    for index in indexes:
        try:
            db_manager.execute_query(f"CREATE INDEX IF NOT EXISTS {index['name']} ON {index['table']} ({index['columns']})")
            logger.info(f"Índice '{index['name']}' criado com sucesso na tabela '{index['table']}'.")
        except Exception as e:
            logger.error(f"Erro ao criar índice '{index['name']}' na tabela '{index['table']}': {e}")