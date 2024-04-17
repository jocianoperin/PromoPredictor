# src/services/database_connection.py
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def get_db_connection():
    try:
        # Atualize com suas credenciais e host corretos
        connection_string = "mysql+mysqlconnector://root:1@localhost/atena"
        engine = create_engine(connection_string)
        connection = engine.connect()
        return connection
    except SQLAlchemyError as e:
        logger.error(f"Erro ao conectar ao banco de dados: {e}")
        return None
