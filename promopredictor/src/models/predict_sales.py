import pandas as pd
from src.services.database import db_manager
from src.utils.logging_config import get_logger
from src.utils.utils import clear_predictions_table, insert_predictions
import joblib
import os

logger = get_logger(__name__)

def fetch_data_for_prediction():
    """
    Busca os produtos e atributos relevantes para fazer previsões futuras.
    """
    query = """
        SELECT DISTINCT CodigoProduto, CodigoSecao, CodigoGrupo, CodigoSubGrupo, CodigoSupermercado, Promocao
        FROM indicadores_vendas_produtos_resumo
        WHERE DATA <= '2023-12-31';
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
        logger.error(f"Erro ao buscar dados para previsão: {e}")
        return None

def preprocess_data_for_prediction(df, le_dict, trained_columns):
    """
    Preprocessa os dados de previsão de acordo com os dados de treinamento.
    """
    try:
        # Aplicar o LabelEncoder salvo para cada coluna categórica
        for col, le in le_dict.items():
            df[col] = le.transform(df[col].astype(str))

        # Criar variáveis dummy para 'CodigoProduto' e outras variáveis categóricas
        df = pd.get_dummies(df, columns=['CodigoProduto', 'CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo', 'CodigoSupermercado'])

        # Garantir que todas as colunas do treinamento estejam presentes
        missing_cols = set(trained_columns) - set(df.columns)
        for col in missing_cols:
            df[col] = 0

        # Ordenar as colunas de acordo com o modelo treinado
        df = df[trained_columns]

        return df
    except Exception as e:
        logger.error(f"Erro no pré-processamento dos dados de previsão: {e}")
        return None

def make_predictions():
    """
    Faz a previsão para o período de 01/01/2024 a 31/03/2024 com o modelo treinado para cada produto.
    """
    logger.info("Iniciando o processo de previsão...")
    df_pred = fetch_data_for_prediction()
    
    if df_pred is not None:
        models_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../trained_models'))

        # Carregar os encoders salvos
        try:
            le_dict = joblib.load(os.path.join(models_dir, 'label_encoders.pkl'))
        except Exception as e:
            logger.error(f"Erro ao carregar os encoders salvos: {e}")
            return

        # Gerar as datas de previsão
        dates = pd.date_range(start='2024-01-01', end='2024-03-31', freq='D')
        df_dates = pd.DataFrame({'DATA': dates})

        # Criar um DataFrame com todas as combinações de produtos e datas
        df_pred['key'] = 1
        df_dates['key'] = 1
        df_pred_full = df_pred.merge(df_dates, on='key').drop('key', axis=1)

        # Iterar sobre cada produto para carregar o modelo correspondente e fazer a previsão
        produtos = df_pred['CodigoProduto'].unique()
        for produto in produtos:
            logger.info(f"Realizando previsões para o produto {produto}...")

            # Filtrar os dados para o produto atual
            df_produto = df_pred_full[df_pred_full['CodigoProduto'] == produto].copy()

            # Carregar os modelos específicos do produto
            try:
                model_un_path = os.path.join(models_dir, f'model_un_{produto}.pkl')
                model_valor_path = os.path.join(models_dir, f'model_valor_{produto}.pkl')
                model_un = joblib.load(model_un_path)
                model_valor = joblib.load(model_valor_path)
            except Exception as e:
                logger.error(f"Erro ao carregar os modelos treinados para o produto {produto}: {e}")
                continue

            # Obter os nomes das features treinadas
            trained_columns = model_un.get_booster().feature_names
            
            # Preprocessar os dados de previsão para o produto
            df_produto_processed = preprocess_data_for_prediction(df_produto, le_dict, trained_columns)

            if df_produto_processed is not None:
                # Realizar previsões
                pred_un = model_un.predict(df_produto_processed)
                pred_valor = model_valor.predict(df_produto_processed)

                # Ajustar as previsões para garantir valores não negativos e formatar corretamente
                df_produto['TotalUNVendidas'] = pred_un.clip(0).round().astype(int)
                df_produto['ValorTotalVendido'] = pred_valor.clip(0).round(2)

                # Preparar os dados para inserção no banco de dados
                df_produto['DATA'] = df_produto['DATA'].dt.strftime('%Y-%m-%d')
                df_to_insert = df_produto[['DATA', 'CodigoProduto', 'TotalUNVendidas', 'ValorTotalVendido', 'Promocao']]

                insert_predictions(df_to_insert)
                logger.info(f"Previsões para o produto {produto} realizadas e inseridas com sucesso.")
            else:
                logger.error(f"Erro no pré-processamento dos dados de previsão para o produto {produto}.")
    else:
        logger.error("Não há dados disponíveis para fazer a previsão.")