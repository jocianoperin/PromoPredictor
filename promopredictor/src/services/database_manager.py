# Este módulo define a classe DatabaseManager que encapsula as operações de banco de dados,
# permitindo alternar entre diferentes conectores de banco de dados com facilidade.

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from src.utils.logging_config import get_logger

# Carregar as variáveis do arquivo .env
load_dotenv()

logger = get_logger(__name__)

class DatabaseManager:
    """
    Gerencia as operações de banco de dados usando SQLAlchemy.
    """
    def __init__(self, use_sqlalchemy=True):
        """
        Inicializa o DatabaseManager usando SQLAlchemy.

        Args:
            use_sqlalchemy (bool): Deve ser True para usar SQLAlchemy.
        """
        self.use_sqlalchemy = use_sqlalchemy

        if self.use_sqlalchemy:
            # Configuração para SQLAlchemy usando o driver pymysql
            self.connection_string = (
                f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
                f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
            )
            self.engine = create_engine(self.connection_string, echo=False, future=True)
        else:
            raise NotImplementedError("Somente o SQLAlchemy é suportado nesta configuração.")

    def get_connection(self):
        """
        Retorna uma conexão ativa ao banco de dados.
        """
        if self.use_sqlalchemy:
            return self.engine.connect()
        else:
            raise NotImplementedError("Somente o SQLAlchemy é suportado nesta configuração.")

    def execute_query(self, query, params=None):
        """
        Executa uma consulta SQL.

        Args:
            query (str): A consulta SQL a ser executada.
            params (dict, optional): Parâmetros para a consulta.

        Returns:
            dict: Resultado da consulta.
        """
        if self.use_sqlalchemy:
            connection = self.get_connection()
            try:
                logger.debug(f"Executando query: {query}")
                result = connection.execute(text(query), params)
                if query.strip().lower().startswith('select'):
                    data = result.fetchall()
                    columns = result.keys()
                    return {'data': data, 'columns': columns}
                else:
                    # Commit após operações de modificação
                    connection.commit()
                    return {'rows_affected': result.rowcount}
            except SQLAlchemyError as e:
                logger.error(f"Erro ao executar query com SQLAlchemy: {e}")
                connection.rollback()
                raise
            finally:
                connection.close()
        else:
            raise NotImplementedError("Somente o SQLAlchemy é suportado nesta configuração.")
