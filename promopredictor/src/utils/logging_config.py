import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path

def get_logger(name):
    """
    Configura e retorna um objeto Logger para registrar mensagens de log.

    Args:
        name (str): Nome do logger.

    Returns:
        logging.Logger: Objeto Logger configurado.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)  # Ou outro nível conforme necessário

    # Define o caminho absoluto para a pasta de logs na raiz do projeto
    project_root = Path(__file__).resolve().parents[2]
    logs_path = project_root / 'logs'
    os.makedirs(logs_path, exist_ok=True)

    # Define o caminho absoluto para o arquivo de log
    log_file_path = logs_path / 'app.log'

    # Remove o arquivo de log existente
    if log_file_path.exists():
        log_file_path.unlink()

    # Configuração do RotatingFileHandler
    handler = RotatingFileHandler(log_file_path, maxBytes=10485760, backupCount=3)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # Adiciona o handler ao logger se ele ainda não existir
    if not logger.handlers:
        logger.addHandler(handler)

    return logger