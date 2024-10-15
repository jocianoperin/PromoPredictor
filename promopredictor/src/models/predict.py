import pandas as pd
from src.models.training_pipeline import train_model
from src.services.database import db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def fetch_data_for_prediction():
    """
    Busca os produtos e atributos relevantes até 31/12/2023, que serão usados para fazer previsões futuras.
    """
    query = """
        SELECT DISTINCT CodigoProduto, Promocao
        FROM indicadores_vendas_produtos_resumo
        WHERE DATA <= '2023-12-31';
    """
    try:
        result = db_manager.execute_query(query)
        if 'data' in result and 'columns' in result:
            df = pd.DataFrame(result['data'], columns=result['columns'])
            return df
        else:
            logger.warning("Nenhum dado foi retornado pela query.")
            return None
    except Exception as e:
        logger.error(f"Erro ao buscar dados para previsão: {e}")
        return None

def insert_predictions(df_pred):
    """
    Insere as previsões na tabela indicadores_vendas_produtos_previsoes.
    """
    try:
        for _, row in df_pred.iterrows():
            insert_query = f"""
            INSERT INTO indicadores_vendas_produtos_previsoes (DATA, CodigoProduto, TotalUNVendidas, ValorTotalVendido, Promocao)
            VALUES ('2024-01-01', {row['CodigoProduto']}, {row['PredictedTotalUNVendidas']}, 
                    {row['PredictedValorTotalVendido']}, {row['Promocao']});
            """
            db_manager.execute_query(insert_query)
        logger.info(f"Previsões inseridas com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao inserir previsões: {e}")

def make_predictions():
    """
    Faz a previsão para o período de 01/01/2024 a 31/03/2024 com o modelo treinado.
    """
    df_pred = fetch_data_for_prediction()
    
    if df_pred is not None:
        # Carrega os modelos treinados
        model_un, model_valor = train_model()

        if model_un is not None and model_valor is not None:
            # Realiza as previsões
            pred_un = model_un.predict(df_pred[['CodigoProduto', 'Promocao']])
            pred_valor = model_valor.predict(df_pred[['CodigoProduto', 'Promocao']])
            
            df_pred['PredictedTotalUNVendidas'] = pred_un
            df_pred['PredictedValorTotalVendido'] = pred_valor
            
            # Insere as previsões no banco de dados
            insert_predictions(df_pred)
        else:
            logger.error("Os modelos não foram carregados corretamente.")
    else:
        logger.error("Não há dados disponíveis para fazer a previsão.")
