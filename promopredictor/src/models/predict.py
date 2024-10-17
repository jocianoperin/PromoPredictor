import pandas as pd
from src.services.database import db_manager
from src.utils.logging_config import get_logger
import joblib
from datetime import datetime, timedelta

logger = get_logger(__name__)

def fetch_data_for_prediction():
    """
    Busca os produtos e atributos relevantes para fazer previsões futuras.
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

def preprocess_data_for_prediction(df, trained_columns):
    """
    Preprocessa os dados de previsão de acordo com os dados de treinamento.
    """
    df['CodigoProduto'] = df['CodigoProduto'].astype('category')
    df['Promocao'] = df['Promocao'].astype(int)

    # Criar variáveis dummy para 'CodigoProduto' com as mesmas colunas do treinamento
    df = pd.get_dummies(df, columns=['CodigoProduto'])

    # Garantir que todas as colunas do treinamento estejam presentes
    missing_cols = set(trained_columns) - set(df.columns)
    for col in missing_cols:
        df[col] = 0

    # Ordenar as colunas de acordo com o modelo treinado
    df = df[trained_columns]

    return df

def insert_predictions(df_pred):
    """
    Insere as previsões na tabela indicadores_vendas_produtos_previsoes.
    """
    try:
        # Transformar o DataFrame em uma lista de dicionários
        values = df_pred.to_dict(orient='records')

        # Query de inserção com 'ON DUPLICATE KEY UPDATE'
        insert_query = """
        INSERT INTO indicadores_vendas_produtos_previsoes (DATA, CodigoProduto, TotalUNVendidas, ValorTotalVendido, Promocao)
        VALUES (:DATA, :CodigoProduto, :TotalUNVendidas, :ValorTotalVendido, :Promocao)
        ON DUPLICATE KEY UPDATE
            TotalUNVendidas = VALUES(TotalUNVendidas),
            ValorTotalVendido = VALUES(ValorTotalVendido),
            Promocao = VALUES(Promocao)
        """

        # Executar a inserção em lote
        db_manager.execute_query(insert_query, params=values)
        logger.info(f"Previsões inseridas com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao inserir previsões: {e}")

def generate_prediction_dates(start_date, end_date):
    """
    Gera uma lista de datas para o período de previsão.
    """
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    return dates

def make_predictions():
    """
    Faz a previsão para o período de 01/01/2024 a 31/03/2024 com o modelo treinado.
    """
    df_pred = fetch_data_for_prediction()
    
    if df_pred is not None:
        # Carrega os modelos treinados
        try:
            model_un = joblib.load('model_un.pkl')
            model_valor = joblib.load('model_valor.pkl')
        except Exception as e:
            logger.error(f"Erro ao carregar os modelos treinados: {e}")
            return

        # Gerar as datas de previsão
        dates = generate_prediction_dates('2024-01-01', '2024-03-31')
        df_dates = pd.DataFrame({'DATA': dates})

        # Criar um DataFrame com todas as combinações de produtos e datas
        df_pred['key'] = 1
        df_dates['key'] = 1
        df_pred_full = df_pred.merge(df_dates, on='key').drop('key', axis=1)

        # Preprocessar os dados de previsão
        trained_columns = model_un.get_booster().feature_names
        df_pred_processed = preprocess_data_for_prediction(df_pred_full, trained_columns)

        # Realizar as previsões
        pred_un = model_un.predict(df_pred_processed)
        pred_valor = model_valor.predict(df_pred_processed)
        
        # Adicionar as previsões ao DataFrame original
        df_pred_full['TotalUNVendidas'] = pred_un
        df_pred_full['ValorTotalVendido'] = pred_valor

        # Garantir que os valores sejam não negativos
        df_pred_full['TotalUNVendidas'] = df_pred_full['TotalUNVendidas'].clip(lower=0)
        df_pred_full['ValorTotalVendido'] = df_pred_full['ValorTotalVendido'].clip(lower=0)

        # Preparar os dados para inserção
        df_pred_full['DATA'] = df_pred_full['DATA'].dt.strftime('%Y-%m-%d')
        df_pred_full['TotalUNVendidas'] = df_pred_full['TotalUNVendidas'].round().astype(int)
        df_pred_full['ValorTotalVendido'] = df_pred_full['ValorTotalVendido'].round(2)
        
        # Selecionar as colunas necessárias
        df_to_insert = df_pred_full[['DATA', 'CodigoProduto', 'TotalUNVendidas', 'ValorTotalVendido', 'Promocao']]

        # Opcional: Limpar a tabela antes de inserir novas previsões
        clear_predictions_table()

        # Inserir as previsões no banco de dados
        insert_predictions(df_to_insert)
    else:
        logger.error("Não há dados disponíveis para fazer a previsão.")

# Opcional: função para limpar a tabela de previsões
def clear_predictions_table():
    """
    Limpa a tabela de previsões antes de inserir novas previsões.
    """
    try:
        delete_query = "TRUNCATE indicadores_vendas_produtos_previsoes"
        db_manager.execute_query(delete_query)
        logger.info("Tabela de previsões limpa com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao limpar a tabela de previsões: {e}")
