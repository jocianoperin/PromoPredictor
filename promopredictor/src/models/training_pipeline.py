import pandas as pd
from sklearn.model_selection import train_test_split
import xgboost as xgb
from sklearn.metrics import mean_squared_error
from src.services.database import db_manager
from src.utils.logging_config import get_logger

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

def train_model():
    """
    Realiza o treinamento do modelo de previsão com os dados históricos usando GPU com XGBoost.
    """
    # Buscar os dados para o período de treinamento (01/01/2019 a 31/12/2023)
    df = fetch_data_for_training('2019-01-01', '2023-12-31')
    
    if df is not None:
        # Prepara os dados para o treinamento
        X = df[['CodigoProduto', 'Promocao']]
        y_total_un = df['TotalUNVendidas']
        y_valor_total = df['ValorTotalVendido']
        
        # Dividir em dados de treino e teste
        X_train, X_test, y_train_un, y_test_un = train_test_split(X, y_total_un, test_size=0.2, random_state=42)
        _, _, y_train_valor, y_test_valor = train_test_split(X, y_valor_total, test_size=0.2, random_state=42)

        # Instancia o modelo XGBoost usando GPU
        model_un = xgb.XGBRegressor(tree_method='gpu_hist')
        model_un.fit(X_train, y_train_un)
        
        model_valor = xgb.XGBRegressor(tree_method='gpu_hist')
        model_valor.fit(X_train, y_train_valor)

        # Avaliar os modelos
        pred_un = model_un.predict(X_test)
        pred_valor = model_valor.predict(X_test)
        
        logger.info(f"RMSE TotalUNVendidas: {mean_squared_error(y_test_un, pred_un, squared=False)}")
        logger.info(f"RMSE ValorTotalVendido: {mean_squared_error(y_test_valor, pred_valor, squared=False)}")

        return model_un, model_valor
    else:
        logger.error("Não foi possível treinar o modelo devido à ausência de dados.")
        return None, None
