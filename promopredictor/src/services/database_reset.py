# src/services/database_reset.py
from src.services.database_connection import get_db_connection
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def drop_tables():
    connection = get_db_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                # Lista de tabelas para serem deletadas
                tables_to_drop = [
                    "sales_indicators",
                    "promotions_identified",
                    #"vendasprodutosexport",
                    #"vendasexport"
                ]
                
                # Executando DROP TABLE para cada tabela
                for table in tables_to_drop:
                    cursor.execute(f"TRUNCATE TABLE IF EXISTS {table};")
                    logger.info(f"Tabela {table} excluída com sucesso.")
                
                connection.commit()
                
        except Exception as e:
            logger.error(f"Erro ao tentar excluir tabelas: {e}")
            if connection:
                connection.rollback()
        finally:
            connection.close()
            logger.info("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    drop_tables()
