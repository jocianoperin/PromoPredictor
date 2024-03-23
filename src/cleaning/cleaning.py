import sys
from pathlib import Path
import mysql.connector
from logging_config import get_logger

# Cria um logger para este módulo
logger = get_logger(__name__)

# Adiciona o diretório db ao path para poder importar db_config
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root / 'db'))

import db_config

def clean_data():
    """
    Clean the sales data and products sold data directly in the MariaDB database.
    """
    try:
        # Obtém a conexão do banco de dados usando a função definida em db_config.py
        conn = db_config.get_db_connection()
        cursor = conn.cursor()

        # Exemplo de limpeza: remover vendas com valores negativos ou zero
        cursor.execute("""
            DELETE FROM vendasexport WHERE TotalPedido <= 0;
        """)
        cursor.execute("""
            DELETE FROM vendasprodutosexport WHERE ValorTotal <= 0 OR Quantidade <= 0;
        """)
        
        # Aplica as mudanças no banco de dados
        conn.commit()
        print("Dados de vendas e produtos vendidos limpos com sucesso.")

    except mysql.connector.Error as e:
        print(f"Erro ao acessar o banco de dados MariaDB: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("Conexão ao banco de dados fechada.")

def main():
    clean_data()

if __name__ == "__main__":
    main()
