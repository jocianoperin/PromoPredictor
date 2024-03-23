import logging
import os
from pathlib import Path

# Define o caminho do diretório de logs
log_directory = Path(__file__).resolve().parent / 'logs'
os.makedirs(log_directory, exist_ok=True)  # Cria o diretório se não existir

# Configuração básica do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_directory / 'app.log'),
        logging.StreamHandler()  # Adiciona também a saída padrão se desejar
    ]
)

def get_logger(name):
    """
    Retorna um logger configurado com o nome especificado.
    """
    return logging.getLogger(name)
