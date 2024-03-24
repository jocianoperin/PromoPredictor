import mysql.connector
from ...db.db_config import get_db_connection
from ..logging_config import get_logger

# Criação do logger
logger = get_logger(__name__)

def identify_promotions(conn):
    """
    Identifica promoções com base em critérios específicos diretamente no banco de dados.
    """
    cursor = conn.cursor(dictionary=True)
    promotions = []
    try:
        query = """
        SELECT vp.CodigoProduto, vp.ValorUnitario, vp.ValorTabela, v.Data
        FROM vendasprodutosexport vp
        JOIN vendasexport v ON vp.CodigoVenda = v.Codigo
        WHERE vp.ValorUnitario < vp.ValorTabela AND vp.PrecoemPromocao IS NOT NULL;
        """
        cursor.execute(query)
        for row in cursor:
            promotions.append(row)
        logger.info(f"{len(promotions)} promoções identificadas.")
    except mysql.connector.Error as e:
        logger.error(f"Erro ao consultar o banco de dados: {e}")
    finally:
        cursor.close()
    return promotions

def create_promotions_table_if_not_exists(conn):
    """
    Cria a tabela de promoções se ela não existir.
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS promocoes_identificadas (
                CodigoProduto INT NOT NULL,
                DataPromocao DATE NOT NULL,
                ValorUnitario DECIMAL(10, 2) NOT NULL,
                ValorTabela DECIMAL(10, 2) NOT NULL,
                PRIMARY KEY (CodigoProduto, DataPromocao)
            );
        """)
        conn.commit()
        logger.info("Tabela 'promocoes_identificadas' verificada/criada com sucesso.")
    except mysql.connector.Error as e:
        logger.error(f"Erro ao criar a tabela 'promocoes_identificadas': {e}")
        conn.rollback()
    finally:
        cursor.close()

def save_promotions_to_db(conn, promotions):
    """
    Salva as promoções identificadas na tabela 'promocoes_identificadas'.
    """
    cursor = conn.cursor()
    try:
        for promo in promotions:
            insert_query = """
            INSERT INTO promocoes_identificadas (CodigoProduto, DataPromocao, ValorUnitario, ValorTabela)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE ValorUnitario=VALUES(ValorUnitario), ValorTabela=VALUES(ValorTabela);
            """
            cursor.execute(insert_query, (promo['CodigoProduto'], promo['Data'], promo['ValorUnitario'], promo['ValorTabela']))
        conn.commit()
        logger.info(f"{cursor.rowcount} promoções salvas/atualizadas no banco de dados.")
    except mysql.connector.Error as e:
        logger.error(f"Erro ao inserir/atualizar promoções no banco de dados: {e}")
        conn.rollback()
    finally:
        cursor.close()


def main():
    conn = None
    try:
        conn = get_db_connection()
        logger.info("Conexão com o banco de dados estabelecida.")
        
        # Verifica e cria a tabela, se necessário
        create_promotions_table_if_not_exists(conn)
        
        promotions = identify_promotions(conn)
        if promotions:
            save_promotions_to_db(conn, promotions)

    except mysql.connector.Error as e:
        logger.error(f"Erro de conexão com o banco de dados: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            logger.info("Conexão com o banco de dados fechada.")


if __name__ == "__main__":
    main()