import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def train_arima(data, p=1, d=1, q=1):
    """
    Treina um modelo ARIMA com os dados de série temporal fornecidos.

    Args:
        data (DataFrame): DataFrame contendo a data e os valores da série temporal.
        p (int): Ordem do componente AR do modelo ARIMA.
        d (int): Ordem do componente de diferenciação.
        q (int): Ordem do componente MA.

    Returns:
        ARIMA ResultsWrapper: Objeto de resultado treinado do modelo ARIMA.
    """
    try:
        # Selecionar apenas a coluna de valores para a análise
        series = data.iloc[:, 3]  # Assumindo que a coluna de valores é a quarta coluna (índice 3)

        # Checar e converter a série para o tipo numérico
        series = pd.to_numeric(series, errors='coerce')

        # Remover valores nulos após a conversão
        series = series.dropna()

        # Ajuste o índice conforme necessário para corresponder à coluna de valores
        model = ARIMA(series, order=(p, d, q))
        arima_result = model.fit()
        logger.info("Modelo ARIMA treinado com sucesso.")
        return arima_result
    except Exception as e:
        logger.error(f"Erro ao treinar o modelo ARIMA: {e}")
        return None

def forecast_arima(model, steps=1):
    """
    Realiza previsões futuras usando o modelo ARIMA treinado.

    Args:
        model (ARIMA ResultsWrapper): Modelo ARIMA treinado.
        steps (int): Número de passos de tempo a prever para frente.

    Returns:
        np.ndarray: Array de previsões.
    """
    try:
        forecast = model.forecast(steps=steps)
        logger.info("Previsões ARIMA realizadas com sucesso.")
        return forecast
    except Exception as e:
        logger.error(f"Erro ao realizar previsões com o modelo ARIMA: {e}")
        return None

def insert_arima_predictions(table, product_column, date_column, value_column, product_id, null_data, forecast):
    """
    Insere as previsões do modelo ARIMA na tabela arima_predictions.

    Args:
        table (str): Nome da tabela original (vendasprodutosexport).
        product_column (str): Coluna que identifica o produto.
        date_column (str): Coluna que identifica a data.
        value_column (str): Coluna cujos valores nulos foram imputados.
        product_id (int): ID do produto.
        null_data (DataFrame): DataFrame contendo os dados com valores nulos.
        forecast (np.ndarray): Array de previsões geradas pelo modelo ARIMA.
    """
    insert_query = """
    INSERT INTO arima_predictions (product_id, export_id, date, value_column, predicted_value)
    VALUES (%s, %s, %s, %s, %s)
    """

    try:
        for idx, value in enumerate(forecast):
            export_id = null_data['ExportID'].iloc[idx]
            date = null_data[date_column].iloc[idx]
            db_manager.execute_query(insert_query, [product_id, export_id, date, value_column, value])
            logger.info(f"Valor {value} previsto com sucesso para o produto {product_id} na coluna {value_column}.")
    except Exception as e:
        logger.error(f"Erro ao inserir previsão para o produto {product_id}: {e}")

def impute_values(table, product_column, value_column, product_id, null_data, forecast):
    """
    Imputa os valores previstos nos dados com valores nulos.

    Args:
        table (str): Nome da tabela onde o valor será imputado.
        product_column (str): Coluna que identifica o produto.
        value_column (str): Coluna que receberá o valor imputado.
        product_id (int): ID do produto.
        null_data (DataFrame): DataFrame contendo os dados com valores nulos.
        forecast (np.ndarray): Array de previsões geradas pelo modelo ARIMA.
    """
    update_query = f"""
    UPDATE {table}
    SET {value_column} = CASE
        WHEN {product_column} = %s AND {value_column} IS NULL THEN %s
        ELSE {value_column}
    END
    WHERE {product_column} = %s;
    """

    try:
        for idx, value in enumerate(forecast):
            db_manager.execute_query(update_query, [product_id, value, product_id])
            logger.info(f"Valor {value} imputado com sucesso para o produto {product_id} na coluna {value_column}.")
    except Exception as e:
        logger.error(f"Erro ao imputar valor para o produto {product_id}: {e}")