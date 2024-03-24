from ...db.db_config import get_db_connection
from ..logging_config import get_logger

# Criação do logger
logger = get_logger(__name__)


def clean_data(conn):
    cursor = conn.cursor()
    try:
        # Aqui vão os comandos SQL para limpeza dos dados.
        cursor.execute("DELETE FROM vendasexport WHERE TotalPedido <= 0")
        logger.info(f"Limpeza na tabela 'vendas': {cursor.rowcount} linhas removidas.")

        cursor.execute("DELETE FROM vendasprodutosexport WHERE ValorTotal <= 0 OR Quantidade <= 0")
        logger.info(f"Limpeza na tabela 'vendasprodutos': {cursor.rowcount} linhas removidas.")

        conn.commit()
    except Exception as e:
        logger.error(f"Erro durante a limpeza dos dados: {e}")
        conn.rollback()
    finally:
        cursor.close()

def main():
    conn = None
    try:
        conn = get_db_connection()
        logger.info("Conexão com o banco de dados estabelecida.")
        
        clean_data(conn)

    except Exception as e:
        logger.error("Erro na conexão com o banco de dados.")
        logger.exception(e)
    finally:
        if conn:
            conn.close()
            logger.info("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    main()