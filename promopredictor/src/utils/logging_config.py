import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)  # Ou outro nível conforme necessário

    # Define o caminho absoluto para a pasta de logs na raiz do projeto
    project_root = Path(__file__).resolve().parents[2]  # Ajusta com base na estrutura do seu projeto
    logs_path = project_root / 'logs'
    os.makedirs(logs_path, exist_ok=True)

    # Define o caminho absoluto para o arquivo de log
    log_file_path = logs_path / 'app.log'

    # Configuração do RotatingFileHandler
    handler = RotatingFileHandler(log_file_path, maxBytes=10485760, backupCount=10)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # Adiciona o handler ao logger se ele ainda não existir
    if not logger.handlers:
        logger.addHandler(handler)

    return logger