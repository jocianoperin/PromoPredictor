import pandas as pd
from sklearn.model_selection import train_test_split
import xgboost as xgb
from sklearn.metrics import root_mean_squared_error
from src.services.database import db_manager
from src.utils.logging_config import get_logger
import joblib

logger = get_logger(__name__)

def fetch_data_for_training(start_date, end_date):
    """
    Busca dados da tabela indicadores_vendas_produtos_resumo para o período especificado.
    """
    query = f"""
        SELECT DATA, CodigoProduto, TotalUNVendidas, ValorTotalVendido, Promocao
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
    Preprocessa os dados para o treinamento do modelo.
    """
    # Converter 'DATA' para datetime, se necessário
    df['DATA'] = pd.to_datetime(df['DATA'])

    # Converter 'CodigoProduto' para categoria
    df['CodigoProduto'] = df['CodigoProduto'].astype('category')
    df['Promocao'] = df['Promocao'].astype(int)

    # Criar variáveis dummy para 'CodigoProduto'
    df = pd.get_dummies(df, columns=['CodigoProduto'], drop_first=True)

    return df

def train_and_save_model():
    """
    Realiza o treinamento do modelo de previsão com os dados históricos e salva os modelos treinados.
    """
    # Buscar os dados para o período de treinamento (01/01/2019 a 31/12/2023)
    df = fetch_data_for_training('2019-01-01', '2023-12-31')
    
    if df is not None:
        # Preprocessar os dados
        df = preprocess_data(df)

        # Separar features e targets
        X = df.drop(columns=['DATA', 'TotalUNVendidas', 'ValorTotalVendido'])
        y_total_un = df['TotalUNVendidas']
        y_valor_total = df['ValorTotalVendido']
        
        # Dividir em dados de treino e teste
        X_train, X_test, y_train_un, y_test_un = train_test_split(X, y_total_un, test_size=0.2, random_state=42)

        y_train_valor = y_valor_total.loc[y_train_un.index]
        y_test_valor = y_valor_total.loc[y_test_un.index]

        # Instanciar e treinar o modelo para TotalUNVendidas
        model_un = xgb.XGBRegressor(tree_method='hist')
        model_un.fit(X_train, y_train_un)
        
        # Instanciar e treinar o modelo para ValorTotalVendido
        model_valor = xgb.XGBRegressor(tree_method='hist')
        model_valor.fit(X_train, y_train_valor)

        # Avaliar os modelos
        pred_un = model_un.predict(X_test)
        pred_valor = model_valor.predict(X_test)
        
        rmse_un = root_mean_squared_error(y_test_un, pred_un)
        rmse_valor = root_mean_squared_error(y_test_valor, pred_valor)
        logger.info(f"RMSE TotalUNVendidas: {rmse_un}")
        logger.info(f"RMSE ValorTotalVendido: {rmse_valor}")

        # Salvar os modelos treinados
        joblib.dump(model_un, 'model_un.pkl')
        joblib.dump(model_valor, 'model_valor.pkl')
        logger.info("Modelos treinados e salvos com sucesso.")

    else:
        logger.error("Não foi possível treinar o modelo devido à ausência de dados.")
