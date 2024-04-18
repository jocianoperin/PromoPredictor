from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

class DatabaseConnection:
    """
    Gerencia a conexão com o banco de dados usando SQLAlchemy.
    """
    def __init__(self):
        """
        Inicializa a conexão com o banco de dados usando SQLAlchemy com a string de conexão.
        """
        self.connection_string = "mysql+mysqlconnector://root:1@localhost/atena"
        self.engine = create_engine(self.connection_string, echo=True)
        self.Session = sessionmaker(bind=self.engine)

    def get_session(self):
        """
        Obtém uma sessão do SQLAlchemy para realizar operações no banco de dados.
        Retorna None em caso de erro.
        """
        try:
            return self.Session()
        except SQLAlchemyError as e:
            logger.error(f"Erro ao conectar ao banco de dados: {e}")
            return None
