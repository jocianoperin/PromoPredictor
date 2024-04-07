# db/db_config.py
from pathlib import Path
import mysql.connector

# Aqui, você define suas funções de configuração do banco de dados
def get_db_connection():
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='1',
        database='atena'
    )
    return connection
