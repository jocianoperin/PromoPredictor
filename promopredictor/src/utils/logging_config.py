import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path

def get_logger(name):
    """
    Configura e retorna um logger para captura e armazenamento de logs do sistema.
    
    Args:
        name (str): Nome do logger. Geralmente, é o nome do módulo ou componente.
    
    Returns:
        logging.Logger: Instância configurada do logger.
    
    Detalhes:
        - Configura o logger para gravar informações com nível INFO ou superior.
        - Usa RotatingFileHandler para gerenciar o tamanho e a rotação dos arquivos de log.
        - Armazena logs na pasta 'logs' na raiz do projeto, criando a pasta se não existir.
    """
    # Configura o nível de gravidade dos logs que serão capturados
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Ou outro nível conforme necessário

    # Define o caminho absoluto para a pasta de logs na raiz do projeto
    project_root = Path(__file__).resolve().parents[2]  # Ajusta com base na estrutura do seu projeto
    logs_path = project_root / 'logs'
    os.makedirs(logs_path, exist_ok=True)

    # Define o caminho completo para o arquivo de log dentro da pasta de logs
    log_file_path = logs_path / 'app.log'

    # Cria um RotatingFileHandler para gerenciar o tamanho e a rotação dos arquivos de log
    handler = RotatingFileHandler(
        log_file_path, 
        maxBytes=10485760,  # Máximo de 10 MB por arquivo de log
        backupCount=10,  # Mantém backup dos últimos 10 arquivos de log
        encoding='utf-8'  # Define a codificação para UTF-8
    )
    # Define o formato das mensagens de log
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # Adiciona o handler ao logger, garantindo que não haja duplicidade
    if not logger.handlers:
        logger.addHandler(handler)

    return logger