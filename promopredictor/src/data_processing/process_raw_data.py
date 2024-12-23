import pandas as pd
from sqlalchemy import create_engine
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def create_db_connection():
    """
    Cria uma conexão com o banco de dados.

    Retorna:
        engine (sqlalchemy.engine.base.Engine): Objeto de conexão ao banco.
    """
    try:
        engine = create_engine('mysql+mysqlconnector://root:123@localhost/ubialli')
        logger.info("Conexão com o banco de dados estabelecida.")
        return engine
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

def extract_raw_data(connection, produto_especifico):
    """
    Extrai dados brutos de vendas do banco de dados.

    Parâmetros:
        connection (sqlalchemy.engine.base.Engine): Objeto de conexão ao banco.
        produto_especifico (int): Código do produto a ser extraído.

    Retorna:
        pandas.DataFrame: Dados extraídos do banco de dados.
    """
    query = """
    SELECT
        vp.CodigoVenda, v.Data, v.Hora, v.Status, v.Cancelada AS VendaCancelada, 
        v.TotalPedido, v.DescontoGeral, v.AcrescimoGeral, v.TotalCusto, vp.CodigoProduto,
        vp.Quantidade, vp.ValorUnitario, vp.ValorTotal, vp.Desconto, vp.Acrescimo, 
        vp.Cancelada AS ItemCancelado, IFNULL(vp.QuantDevolvida, 0) as QuantDevolvida, IFNULL(vp.PrecoemPromocao, 0) as PrecoemPromocao,
        vp.CodigoSecao, vp.CodigoGrupo, vp.CodigoSubGrupo, vp.CodigoFabricante, vp.ValorCusto, 
        vp.ValorCustoGerencial, vp.CodigoFornecedor, vp.CodigoKitPrincipal, vp.ValorKitPrincipal
    FROM vendasprodutos vp
    INNER JOIN vendas v ON vp.CodigoVenda = v.Codigo
    WHERE vp.CodigoProduto = %(produto_especifico)s AND v.Status IN ('f', 'x')
    """
    try:
        df = pd.read_sql(query, connection, params={'produto_especifico': produto_especifico})
        logger.info(f"Dados do produto {produto_especifico} extraídos com sucesso.")
        return df
    except Exception as e:
        logger.error(f"Erro ao extrair dados: {e}")
        return pd.DataFrame()
    
def save_raw_data(df, produto_especifico, output_dir):
    """
    Salva os dados brutos extraídos em um arquivo CSV.

    Parâmetros:
        df (pandas.DataFrame): Dados extraídos.
        produto_especifico (int): Código do produto.
        output_dir (Path): Caminho para salvar o arquivo.
    """
    file_path = output_dir / f'produto_{produto_especifico}.csv'
    try:
        df.to_csv(file_path, index=False, sep=',')
        logger.info(f"Dados brutos salvos em {file_path}.")
    except Exception as e:
        logger.error(f"Erro ao salvar dados brutos: {e}")

def main():
    produto_especifico = 26173
    connection = create_db_connection()

    if connection:
        df_raw = extract_raw_data(connection, produto_especifico)
        connection.dispose()

        if not df_raw.empty:
            save_raw_data(df_raw, produto_especifico)
        else:
            logger.warning("Nenhum dado foi extraído.")

if __name__ == "__main__":
    main()