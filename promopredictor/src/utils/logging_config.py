# src/logging_config.py
import logging
from logging.handlers import RotatingFileHandler
from sqlalchemy import event
from sqlalchemy.engine import Engine
import os
from pathlib import Path
import time

def setup_sqlalchemy_logging():
    # Configura o logging para o SQLAlchemy
    logging.basicConfig()
    # Você pode ajustar o nível de logging conforme necessário, por exemplo, para DEBUG
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    # Listener para logar as consultas SQL executadas
    @event.listens_for(Engine, "before_cursor_execute")
    def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        start_time = time.time()
        conn.info.setdefault('query_start_time', []).append(start_time)
        logger = get_logger("sqlalchemy.engine")
        logger.info("Iniciando consulta: %s", statement)
        # Adicione mais detalhes aqui se necessário

    @event.listens_for(Engine, "after_cursor_execute")
    def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        end_time = time.time()
        start_time = conn.info['query_start_time'].pop(-1)  # Remove o último tempo de início registrado
        logger = get_logger("sqlalchemy.engine")
        logger.info("Consulta concluída em %f segundos: %s", end_time - start_time, statement)
        # Adicione mais detalhes aqui se necessário

def get_logger(name):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)  # Ou outro nível conforme necessário

        # Define o caminho absoluto para a pasta de logs na raiz do projeto
        project_root = Path(__file__).resolve().parents[2]
        logs_path = project_root / 'logs'
        os.makedirs(logs_path, exist_ok=True)

        # Define o caminho absoluto para o arquivo de log
        log_file_path = logs_path / 'app.log'

        # Configuração do RotatingFileHandler
        handler = RotatingFileHandler(log_file_path, maxBytes=10485760, backupCount=3)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        logger.addHandler(handler)

    return logger

# Configura o logging ao importar este módulo
setup_sqlalchemy_logging()
