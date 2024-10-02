import mysql.connector
import pandas as pd
from sqlalchemy import create_engine

# Configuração da conexão ao banco de dados
db_config = {
    'user': 'root',
    'password': '1',
    'host': 'localhost',
    'database': 'ubialli'
}

# Conectar ao banco de dados
def connect_db():
    try:
        conn = mysql.connector.connect(**db_config)
        if conn.is_connected():
            print('Conexão ao MariaDB realizada com sucesso.')
        return conn
    except mysql.connector.Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Função para buscar registros em blocos
def fetch_data_in_batches(conn, query, batch_size=100000):
    return pd.read_sql(query, conn, chunksize=batch_size)

# Inserir os dados processados no banco de dados
def insert_data_in_batches(df, table_name, engine):
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

# Processar os dados e inserir no novo formato
def process_data_and_insert(conn, engine):
    # SQL para buscar e agrupar os dados
    query = """
    SELECT c.DATA, p.CodigoProduto, COALESCE(va2.CodigoSecao, p.CodigoSecao) AS CodigoSecao, 
           COALESCE(va2.CodigoGrupo, p.CodigoGrupo) AS CodigoGrupo,
           COALESCE(va2.CodigoSubGrupo, p.CodigoSubGrupo) AS CodigoSubGrupo, 
           COALESCE(va2.CodigoSupermercado, p.CodigoSupermercado) AS CodigoSupermercado,
           ROUND(IFNULL(SUM(va2.Quantidade), 0), 2) AS TotalUNVendidas, 
           ROUND(IFNULL(SUM(va2.ValorTotal), 0), 2) AS ValorTotalVendido, 
           IFNULL(va2.Promocao, 0) AS Promocao
    FROM calendario c 
    CROSS JOIN (SELECT DISTINCT CodigoProduto, CodigoSecao, CodigoGrupo, CodigoSubGrupo, CodigoSupermercado 
                FROM indicadores_vendas_produtos) p 
    LEFT JOIN indicadores_vendas_produtos va2 
    ON c.DATA = va2.DATA AND p.CodigoProduto = va2.CodigoProduto
    GROUP BY c.DATA, p.CodigoProduto
    ORDER BY p.CodigoProduto, c.DATA;
    """
    
    # Iterar sobre os dados em blocos
    for df_batch in fetch_data_in_batches(conn, query):
        # Inserir o lote de dados processados no banco de destino
        insert_data_in_batches(df_batch, 'indicadores_vendas_produtos_resumo', engine)
        print(f"Lote de {len(df_batch)} registros inserido com sucesso.")

# Função principal para executar todo o processo
def main():
    conn = connect_db()
    if conn is None:
        return

    # Criar engine do SQLAlchemy para usar no pandas.to_sql
    engine = create_engine(f"mysql+mysqlconnector://root:1@localhost/ubialli")

    try:
        process_data_and_insert(conn, engine)
    except Exception as e:
        print(f"Erro ao processar e inserir os dados: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
