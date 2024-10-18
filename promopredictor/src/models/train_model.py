import pandas as pd
import os
from sklearn.model_selection import TimeSeriesSplit
import xgboost as xgb
from sklearn.preprocessing import LabelEncoder
from src.services.database import db_manager
from src.utils.logging_config import get_logger
import joblib

logger = get_logger(__name__)

def fetch_data_for_training(start_date, end_date):
    """
    Busca dados da tabela indicadores_vendas_produtos_resumo para o período especificado.
    """
    query = f"""
        SELECT DATA, CodigoProduto, CodigoSecao, CodigoGrupo, CodigoSubGrupo, CodigoSupermercado,
               TotalUNVendidas, ValorTotalVendido, Promocao
        FROM indicadores_vendas_produtos_resumo
        WHERE DATA BETWEEN '{start_date}' AND '{end_date}';
    """
    try:
        result = db_manager.execute_query(query)
        if 'data' in result and 'columns' in result:
            logger.info(f"Quantidade de registros retornados: {len(result['data'])}")
            df = pd.DataFrame(result['data'], columns=result['columns'])
            return df
        else:
            logger.warning("Nenhum dado foi retornado pela query.")
            return None
    except Exception as e:
        logger.error(f"Erro ao buscar dados: {e}")
        return None

def preprocess_data(df):
    """
    Preprocessa os dados para o treinamento do modelo, agrupando por produto e criando features temporais.
    """
    try:
        logger.info("Iniciando o pré-processamento dos dados...")
        df['DATA'] = pd.to_datetime(df['DATA'])
        df = df.sort_values(by=['CodigoProduto', 'DATA'])

        # Manter coluna original do CódigoProduto
        df['CodigoProdutoOriginal'] = df['CodigoProduto']

        # Converter 'Promocao' para float
        df['Promocao'] = df['Promocao'].astype(float)

        # Features de tempo
        df['dia_da_semana'] = df['DATA'].dt.dayofweek
        df['mes'] = df['DATA'].dt.month
        df['dia'] = df['DATA'].dt.day
        df['ano'] = df['DATA'].dt.year

        # Lags e médias móveis para cada produto
        df['TotalUNVendidas_lag1'] = df.groupby('CodigoProduto')['TotalUNVendidas'].shift(1)
        df['TotalUNVendidas_7d_avg'] = df.groupby('CodigoProduto')['TotalUNVendidas'].transform(lambda x: x.rolling(7, min_periods=1).mean())
        df['ValorTotalVendido_lag1'] = df.groupby('CodigoProduto')['ValorTotalVendido'].shift(1)
        df['ValorTotalVendido_7d_avg'] = df.groupby('CodigoProduto')['ValorTotalVendido'].transform(lambda x: x.rolling(7, min_periods=1).mean())

        # Remover linhas com valores ausentes
        df.dropna(inplace=True)

        logger.info("Pré-processamento concluído.")
        return df
    except Exception as e:
        logger.error(f"Erro durante o pré-processamento: {e}")
        return None

def train_and_save_models():
    """
    Realiza o treinamento do modelo de previsão para cada produto e salva os modelos individualmente.
    """
    logger.info("Iniciando o processo de treinamento dos modelos...")
    df = fetch_data_for_training('2019-01-01', '2023-12-31')
    
    if df is not None:
        # Verificar se a pasta 'trained_models' existe, se não, criar
        models_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../trained_models'))
        if not os.path.exists(models_dir):
            os.makedirs(models_dir)

        # Pré-processar os dados
        df = preprocess_data(df)
        if df is None:
            logger.error("Erro no pré-processamento dos dados.")
            return

        # Remover 'Promocao' das colunas categóricas
        cat_cols = ['CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo', 'CodigoSupermercado']
        le_dict = {}
        for col in cat_cols:
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col])
            le_dict[col] = le

        # Tratar 'Promocao' como numérica
        df['Promocao'] = df['Promocao'].astype(float)

        # Salvar os LabelEncoders para serem usados nas previsões
        joblib.dump(le_dict, os.path.join(models_dir, 'label_encoders.pkl'))

        produtos = df['CodigoProduto'].unique()
        total_produtos = len(produtos)
        logger.info(f"Total de produtos a serem treinados: {total_produtos}")

        success_count = 0
        error_count = 0
        for produto in produtos:
            df_produto = df[df['CodigoProduto'] == produto]
            codigo_produto_original = df_produto['CodigoProdutoOriginal'].iloc[0]  # Recupera o código original

            # Verificar quantidade de dados por produto
            if len(df_produto) < 10:
                logger.warning(f"Produto {codigo_produto_original} possui poucos registros ({len(df_produto)}). Pulando...")
                continue

            X = df_produto.drop(columns=['DATA', 'TotalUNVendidas', 'ValorTotalVendido', 'CodigoProdutoOriginal'])
            y_total_un = df_produto['TotalUNVendidas']
            y_valor_total = df_produto['ValorTotalVendido']

            model_un = xgb.XGBRegressor(tree_method='hist', enable_categorical=False)
            model_valor = xgb.XGBRegressor(tree_method='hist', enable_categorical=False)

            try:
                model_un.fit(X, y_total_un)
                model_valor.fit(X, y_valor_total)

                # Salvar os modelos individualmente na pasta 'trained_models' usando o código original do produto
                joblib.dump(model_un, os.path.join(models_dir, f'model_un_{codigo_produto_original}.pkl'))
                joblib.dump(model_valor, os.path.join(models_dir, f'model_valor_{codigo_produto_original}.pkl'))

                # Salvar as colunas treinadas
                trained_columns = X.columns.tolist()
                joblib.dump(trained_columns, os.path.join(models_dir, f'trained_columns_{codigo_produto_original}.pkl'))

                success_count += 1
            except Exception as e:
                logger.error(f"Erro ao treinar modelo para o produto {codigo_produto_original}: {e}")
                error_count += 1

        # Log do resumo final
        logger.info(f"Treinamento concluído. Modelos treinados com sucesso: {success_count}. Erros: {error_count}.")
    else:
        logger.error("Não foi possível treinar os modelos devido à ausência de dados.")

if __name__ == "__main__":
    train_and_save_models()
