from src.services.database import DatabaseManager
from src.utils.logging_config import get_logger
import pandas as pd
from pathlib import Path

logger = get_logger(__name__)

def extract_raw_data(db_manager: DatabaseManager, produto_especifico: int) -> pd.DataFrame:
    """
    Extrai dados brutos de vendas do banco de dados.

    Args:
        db_manager (DatabaseManager): Instância do gerenciador de banco de dados.
        produto_especifico (int): Código do produto a ser extraído.

    Returns:
        pd.DataFrame: Dados extraídos do banco de dados.
    """
    query = """
    SELECT
        vp.CodigoVenda, v.Data, v.Hora, v.Status, v.Cancelada AS VendaCancelada, 
        v.TotalPedido, IFNULL(v.DescontoGeral, 0) as DescontoGeral, IFNULL(v.AcrescimoGeral, 0) as AcrescimoGeral, v.TotalCusto, vp.CodigoProduto,
        vp.Quantidade, vp.ValorUnitario, vp.ValorTotal, vp.Desconto, vp.Acrescimo, 
        vp.Cancelada AS ItemCancelado, IFNULL(vp.QuantDevolvida, 0) as QuantDevolvida, IFNULL(vp.PrecoemPromocao, 0) as PrecoemPromocao,
        vp.CodigoSecao, vp.CodigoGrupo, vp.CodigoSubGrupo, vp.CodigoFabricante, vp.ValorCusto, 
        vp.ValorCustoGerencial, vp.CodigoFornecedor, vp.CodigoKitPrincipal, vp.ValorKitPrincipal
    FROM vendasprodutos vp
    INNER JOIN vendas v ON vp.CodigoVenda = v.Codigo
    WHERE vp.CodigoProduto = :produto_especifico AND v.Status IN ('f', 'x')
    """
    try:
        # Executa a query usando o DatabaseManager
        result = db_manager.execute_query(query, params={'produto_especifico': produto_especifico})
        if result['data']:
            df = pd.DataFrame(result['data'], columns=result['columns'])
            logger.info(f"Dados do produto {produto_especifico} extraídos com sucesso.")
            return df
        else:
            logger.warning(f"Nenhum dado encontrado para o produto {produto_especifico}.")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Erro ao extrair dados: {e}")
        return pd.DataFrame()

def save_raw_data(df: pd.DataFrame, produto_especifico: int, output_dir: Path):
    """
    Salva os dados brutos extraídos em um arquivo CSV.

    Args:
        df (pd.DataFrame): Dados extraídos.
        produto_especifico (int): Código do produto.
        output_dir (Path): Caminho para salvar o arquivo.
    """
    file_path = output_dir / f'produto_{produto_especifico}.csv'
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)  # Garantir que o diretório existe
        df.to_csv(file_path, index=False, sep=',')
        logger.info(f"Dados brutos salvos em {file_path}.")
    except Exception as e:
        logger.error(f"Erro ao salvar dados brutos: {e}")

def main():
    produto_especifico = 26173
    output_dir = Path("data/raw")  # Defina o caminho correto para os arquivos de saída
    db_manager = DatabaseManager()  # Inicializar o gerenciador de banco de dados

    try:
        df_raw = extract_raw_data(db_manager, produto_especifico)

        if not df_raw.empty:
            save_raw_data(df_raw, produto_especifico, output_dir)
        else:
            logger.warning("Nenhum dado foi extraído.")
    finally:
        db_manager.engine.dispose()  # Fecha a conexão com o banco de dados

if __name__ == "__main__":
    main()
