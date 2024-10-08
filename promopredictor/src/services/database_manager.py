# Este módulo define a classe DatabaseManager que encapsula as operações de banco de dados,
# permitindo alternar entre diferentes conectores de banco de dados com facilidade.

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from mysql.connector import connect, Error as MySQLError
from src.utils.logging_config import get_logger
import threading

# Carregar as variáveis do arquivo .env
load_dotenv()

lock = threading.Lock()

logger = get_logger(__name__)

class DatabaseManager:
    """
    Gerencia as operações de banco de dados usando SQLAlchemy ou MySQL Connector,
    dependendo da configuração escolhida. Essa abordagem modular permite uma fácil
    adaptação e manutenção do acesso ao banco de dados em diferentes ambientes ou
    preferências de tecnologia.
    """
    def __init__(self, use_sqlalchemy=None):
        """
        Inicializa o DatabaseManager. Pode configurar para usar SQLAlchemy, que é mais
        abstrato e suporta várias bases de dados SQL, ou MySQL Connector, que é específico
        para MySQL, podendo ser mais direto e performático em cenários específicos.
        
        Args:
            use_sqlalchemy (bool): Se True, usa SQLAlchemy. Se False, usa MySQL Connector.
                                   Se None, usa o valor da variável de ambiente USE_SQLALCHEMY.
        """
        # Se use_sqlalchemy for None, usa a variável de ambiente
        if use_sqlalchemy is None:
            self.use_sqlalchemy = os.getenv("USE_SQLALCHEMY", "False").lower() == "true"
        else:
            self.use_sqlalchemy = use_sqlalchemy

        if self.use_sqlalchemy:
            # Construir a string de conexão com base nas variáveis de ambiente
            self.connection_string = (
                f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
                f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
            )
            self.engine = create_engine(self.connection_string, echo=False)
            self.Session = sessionmaker(bind=self.engine)
            self.placeholder = ":{}"  # Placeholder para estilo SQLAlchemy
        else:
            # Configurar parâmetros de conexão para o MySQL Connector
            self.connection_params = {
                'host': os.getenv('DB_HOST'),
                'user': os.getenv('DB_USER'),
                'password': os.getenv('DB_PASSWORD'),
                'database': os.getenv('DB_NAME'),
                'port': os.getenv('DB_PORT', 3306)
            }
            self.placeholder = "%s"   # Placeholder para estilo MySQL Connector

    def get_connection(self):
        """
        Retorna uma conexão ativa ao banco de dados.
        Para SQLAlchemy, retorna uma conexão da engine; para MySQL Connector, retorna uma conexão MySQL.
        """
        if self.use_sqlalchemy:
            return self.engine.connect()
        else:
            return connect(**self.connection_params)
            
    def execute_query(self, query, params=None):
        """
        Executa uma consulta SQL, gerenciando a conexão e a execução de forma segura,
        lidando com as especificidades de cada conector de banco de dados.
        
        Args:
            query (str): A consulta SQL a ser executada.
            params (dict, optional): Parâmetros a serem substituídos na consulta, para prevenção de SQL Injection.
        
        Returns:
            dict: Um dicionário contendo 'data' e 'columns' para SELECTs ou 'rows_affected' para outras operações.
        """
        with lock:
            if self.use_sqlalchemy:
                session = self.Session()
                try:
                    with session.begin():
                        result = session.execute(text(query), params)
                        if query.strip().lower().startswith('select'):
                            data = [row for row in result.fetchall()]
                            columns = result.keys()
                            return {'data': data, 'columns': columns}
                        else:
                            return {'rows_affected': result.rowcount}
                except SQLAlchemyError as e:
                    logger.error(f"Erro ao executar query com SQLAlchemy: {e}")
                    session.rollback()
                finally:
                    session.close()
            else:
                connection = connect(**self.connection_params, buffered=True)
                cursor = connection.cursor()
                try:
                    cursor.execute(query, params)
                    if query.strip().lower().startswith("select"):
                        data = cursor.fetchall()
                        columns = cursor.column_names
                        return {'data': data, 'columns': columns}
                    else:
                        connection.commit()
                        return {'rows_affected': cursor.rowcount}
                except MySQLError as e:
                    logger.error(f"Erro ao executar query com MySQL Connector: {e}")
                    connection.rollback()
                finally:
                    cursor.close()
                    connection.close()

    def begin_transaction(self):
        if self.use_sqlalchemy:
            self.session = self.Session()
            self.transaction = self.session.begin_nested()
        else:
            self.connection = connect(**self.connection_params)
            self.cursor = self.connection.cursor()
            self.connection.start_transaction()

    def commit_transaction(self):
        if self.use_sqlalchemy:
            self.session.commit()
        else:
            self.connection.commit()

    def rollback_transaction(self):
        if self.use_sqlalchemy:
            self.session.rollback()
        else:
            self.connection.rollback()

    def close(self):
        if self.use_sqlalchemy:
            self.session.close()
        else:
            self.cursor.close()
            self.connection.close()