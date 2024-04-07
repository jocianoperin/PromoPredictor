# src/services/database_connection.py
import mysql.connector
from mysql.connector import Error
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def get_db_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='1',
            database='atena'
        )
        return connection
    except Error as e:
        logger.error(f"Erro ao conectar ao banco de dados: {e}")
        return None
