import sys
from pathlib import Path
import mysql.connector

# Adiciona o diretório 'src' ao path para importar os módulos de configuração
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root / 'src'))

from logging_config import get_logger
from db.db_config import get_db_connection

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
        FROM vendasprodutos vp
        JOIN vendas v ON vp.CodigoVenda = v.Codigo
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

def main():
    conn = None
    try:
        conn = get_db_connection()
        logger.info("Conexão com o banco de dados estabelecida.")
        
        promotions = identify_promotions(conn)
        # Aqui você pode salvar as promoções identificadas onde precisar,
        # como em uma nova tabela no banco de dados ou em um arquivo, etc.

    except mysql.connector.Error as e:
        logger.error(f"Erro de conexão com o banco de dados: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            logger.info("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    main()