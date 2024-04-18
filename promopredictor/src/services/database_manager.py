from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from mysql.connector import connect, Error as MySQLError
from src.utils.logging_config import get_logger
import threading

lock = threading.Lock()

logger = get_logger(__name__)

class DatabaseManager:
    """
    Gerencia as operações de banco de dados usando SQLAlchemy ou MySQL Connector,
    dependendo da configuração escolhida.
    """
    def __init__(self, use_sqlalchemy=False):
        """
        Inicializa o DatabaseManager com a opção de usar SQLAlchemy ou MySQL Connector.
        Args:
            use_sqlalchemy (bool): Define se SQLAlchemy será usado para as operações de banco de dados.
        """
        self.use_sqlalchemy = use_sqlalchemy
        if self.use_sqlalchemy:
            self.connection_string = "mysql+mysqlconnector://root:1@localhost/atena"
            self.engine = create_engine(self.connection_string, echo=False)
            self.Session = sessionmaker(bind=self.engine)
        else:
            self.connection_params = {
                'host': 'localhost',
                'user': 'root',
                'password': '1',
                'database': 'atena'
            }

    def execute_query(self, query, params=None):
        """Executa uma consulta SQL no banco de dados configurado.

        Args:
            query (str): Consulta SQL a ser executada.
            params (dict, optional): Parâmetros para a consulta SQL.

        Returns:
            Retorna o número de linhas afetadas ou os resultados da consulta, dependendo do tipo de consulta.
        """
        with lock:
            if self.use_sqlalchemy:
                session = self.Session()
                try:
                    with session.begin():
                        result = session.execute(text(query), params)
                        return result.rowcount if not query.strip().lower().startswith('select') else result.fetchall()
                except SQLAlchemyError as e:
                    logger.error(f"Erro ao executar query com SQLAlchemy: {e}")
                    session.rollback()
                finally:
                    session.close()
            else:
                connection = connect(**self.connection_params)
                cursor = connection.cursor()
                try:
                    cursor.execute(query, params)
                    if query.strip().lower().startswith("select"):
                        result = cursor.fetchall()
                    else:
                        connection.commit()
                        result = cursor.rowcount
                    return result
                except MySQLError as e:
                    logger.error(f"Erro ao executar query com MySQL Connector: {e}")
                    connection.rollback()
                finally:
                    cursor.close()
                    connection.close()