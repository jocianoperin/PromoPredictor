import pandas as pd
import numpy as np
import os
import joblib
import gc
import holidays
from src.services.database import db_manager
from src.utils.logging_config import get_logger
from src.utils.utils import clear_predictions_table, insert_predictions
from src.services.data_transformer import add_temporal_features, add_lag_features, add_rolling_average

logger = get_logger(__name__)

def fetch_data_for_prediction():
    """
    Busca os produtos e atributos relevantes para fazer previsões futuras, incluindo
    colunas necessárias para prever vendas com base nos dados históricos.
    """
    query = """
        SELECT DATA, CodigoProduto, CodigoSecao, CodigoGrupo, CodigoSubGrupo, CodigoSupermercado,
               TotalUNVendidas, ValorTotalVendido, Promocao
        FROM indicadores_vendas_produtos_resumo
        WHERE DATA <= '2023-12-31';
    """
    try:
        result = db_manager.execute_query(query)
        if result and 'data' in result and 'columns' in result:
            df = pd.DataFrame(result['data'], columns=result['columns'])
            logger.info(f"Quantidade de registros retornados: {len(df)}")
            return df
        else:
            logger.warning("Nenhum dado foi retornado pela query.")
            return None
    except Exception as e:
        logger.error(f"Erro ao buscar dados para previsão: {e}")
        return None

def preprocess_historical_data(df_historical, oe_dict):
    """
    Pré-processa os dados históricos para treinamento e previsão.
    """
    try:
        df_historical = add_temporal_features(df_historical)

        # Tratar 'Promocao' como numérica
        if 'Promocao' in df_historical.columns:
            df_historical['Promocao'] = df_historical['Promocao'].astype(float)
        else:
            df_historical['Promocao'] = 0.0  # Ou outro valor padrão adequado

        # Ordenar o DataFrame
        df_historical.sort_values(by=['CodigoProduto', 'DATA'], inplace=True)

        # Usar OrdinalEncoder para colunas categóricas com tratamento de valores desconhecidos
        cat_cols = ['CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo', 'CodigoSupermercado']
        for col in cat_cols:
            oe = oe_dict.get(col)
            if oe is not None:
                df_historical[[col]] = oe.transform(df_historical[[col]])
            else:
                df_historical[col] = -1  # Valor padrão para categorias desconhecidas

        # Adicionar lags e médias móveis
        df_historical = add_lag_features(df_historical, 'CodigoProduto', 'TotalUNVendidas', [1, 7])
        df_historical = add_lag_features(df_historical, 'CodigoProduto', 'ValorTotalVendido', [1])
        df_historical = add_rolling_average(df_historical, 'CodigoProduto', 'TotalUNVendidas', [7, 30, 365])
        df_historical = add_rolling_average(df_historical, 'CodigoProduto', 'ValorTotalVendido', [7, 30])

        # Remover linhas com valores ausentes
        df_historical.dropna(inplace=True)

        return df_historical
    except Exception as e:
        logger.error(f"Erro no pré-processamento dos dados históricos: {e}")
        return None

def prepare_future_data(df_future, last_known_values, oe_dict):
    """
    Prepara os dados futuros para previsão, preenchendo as features necessárias.
    """
    try:
        df_future = add_temporal_features(df_future)

        # Tratar 'Promocao' como numérica
        if 'Promocao' in df_future.columns:
            df_future['Promocao'] = df_future['Promocao'].astype(float)
        else:
            df_future['Promocao'] = 0.0  # Ou outro valor padrão adequado

        # Usar OrdinalEncoder para colunas categóricas com tratamento de valores desconhecidos
        cat_cols = ['CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo', 'CodigoSupermercado']
        for col in cat_cols:
            oe = oe_dict.get(col)
            if oe is not None:
                df_future[[col]] = oe.transform(df_future[[col]])
            else:
                df_future[col] = -1  # Valor padrão para categorias desconhecidas

        # Preencher as features de lag com os últimos valores conhecidos
        last_known_values = last_known_values.astype(float).fillna(0)

        df_future['TotalUNVendidas_lag1'] = float(last_known_values['TotalUNVendidas_lag1'])
        df_future['TotalUNVendidas_7d_avg'] = float(last_known_values['TotalUNVendidas_7d_avg'])
        df_future['ValorTotalVendido_lag1'] = float(last_known_values['ValorTotalVendido_lag1'])
        df_future['ValorTotalVendido_7d_avg'] = float(last_known_values['ValorTotalVendido_7d_avg'])

        # Garantir que os tipos de dados estejam corretos
        numeric_cols = ['TotalUNVendidas_lag1', 'TotalUNVendidas_7d_avg',
                        'ValorTotalVendido_lag1', 'ValorTotalVendido_7d_avg', 'Promocao']
        for col in numeric_cols:
            df_future[col] = df_future[col].astype(float)

        return df_future
    except Exception as e:
        logger.error(f"Erro ao preparar os dados futuros: {e}")
        return None

def make_predictions():
    """
    Faz a previsão para o período de 01/01/2024 a 31/03/2024 com o modelo treinado para cada produto,
    processando produto por produto para evitar sobrecarga de memória.
    """
    logger.info("Iniciando o processo de previsão...")

    # Buscar os dados para previsão
    df_pred = fetch_data_for_prediction()
    if df_pred is None or df_pred.empty:
        logger.error("Nenhum dado retornado para previsão. Abortando o processo.")
        return

    try:
        models_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../trained_models'))

        # Carregar os OrdinalEncoders salvos
        logger.debug(f"Carregando os encoders salvos de {models_dir}")
        oe_dict = joblib.load(os.path.join(models_dir, 'ordinal_encoders.pkl'))
        logger.debug("Encoders carregados com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao carregar os encoders salvos: {e}")
        return

    try:
        # Limpar a tabela de previsões antes de inserir novas
        logger.debug("Limpando a tabela de previsões...")
        clear_predictions_table()

        # Converter 'DATA' para pd.Timestamp no df_pred
        df_pred['DATA'] = pd.to_datetime(df_pred['DATA'])

        # Gerar as datas de previsão
        logger.debug("Gerando as datas de previsão de 2024-01-01 a 2024-03-31...")
        dates = pd.date_range(start='2024-01-01', end='2024-03-31', freq='D')
        logger.debug(f"Datas geradas: {dates[:5]}")

        produtos = df_pred['CodigoProduto'].unique()
        total_produtos = len(produtos)
        logger.info(f"Total de produtos a serem processados: {total_produtos}")

        success_count = 0
        error_count = 0

        # Processando cada produto individualmente
        for idx, produto in enumerate(produtos):
            logger.info(f"Processando produto {produto} ({idx + 1}/{total_produtos})...")

            try:
                # Filtrar os dados para o produto atual
                df_produto = df_pred[df_pred['CodigoProduto'] == produto].copy()

                # Carregar as colunas treinadas
                trained_columns_path = os.path.join(models_dir, f'trained_columns_{produto}.pkl')
                if not os.path.exists(trained_columns_path):
                    logger.warning(f"Colunas treinadas não encontradas para o produto {produto}. Pulando...")
                    continue
                trained_columns = joblib.load(trained_columns_path)

                # Pré-processar dados históricos
                df_historical = preprocess_historical_data(df_produto, oe_dict)
                if df_historical is None or df_historical.empty:
                    logger.warning(f"Dados históricos insuficientes para o produto {produto}.")
                    continue

                # Obter os últimos valores conhecidos para as features de lag
                last_known_values = df_historical.iloc[-1][['TotalUNVendidas_lag1', 'TotalUNVendidas_7d_avg',
                                                            'ValorTotalVendido_lag1', 'ValorTotalVendido_7d_avg']]

                # Criar DataFrame para os dados futuros
                df_future = pd.DataFrame({'DATA': dates})
                for col in ['CodigoProduto', 'CodigoSecao', 'CodigoGrupo', 'CodigoSubGrupo', 'CodigoSupermercado', 'Promocao']:
                    df_future[col] = df_produto[col].iloc[-1]

                # Preparar os dados futuros
                df_future = prepare_future_data(df_future, last_known_values, oe_dict)
                if df_future is None or df_future.empty:
                    logger.warning(f"Falha ao preparar dados futuros para o produto {produto}.")
                    continue

                # Concatenar dados históricos pré-processados com dados futuros
                df_combined_processed = pd.concat([df_historical, df_future], ignore_index=True)

                # Selecionar dados futuros para previsão
                df_future_processed = df_combined_processed[df_combined_processed['DATA'] >= pd.Timestamp('2024-01-01')].copy()

                # Verificar se há dados suficientes para previsão
                if df_future_processed.empty:
                    logger.warning(f"Sem dados futuros para previsão do produto {produto}.")
                    continue

                # Carregar os modelos específicos do produto
                model_un_path = os.path.join(models_dir, f'model_un_{produto}.pkl')
                model_valor_path = os.path.join(models_dir, f'model_valor_{produto}.pkl')

                if not os.path.exists(model_un_path) or not os.path.exists(model_valor_path):
                    logger.warning(f"Modelos não encontrados para o produto {produto}. Pulando...")
                    continue

                model_un = joblib.load(model_un_path)
                model_valor = joblib.load(model_valor_path)
                logger.debug(f"Modelos carregados para o produto {produto}.")

                # Selecionar as colunas de características para previsão
                X_future = df_future_processed[trained_columns]

                # Realizar previsões apenas nos dados futuros
                pred_un = model_un.predict(X_future)
                pred_valor = model_valor.predict(X_future)

                # Atribuir previsões ao DataFrame futuro com arredondamento para duas casas decimais
                df_future_processed['TotalUNVendidas'] = pred_un.clip(0).round().astype(int)
                df_future_processed['ValorTotalVendido'] = pred_valor.clip(0).round(2)

                # Definir vendas como zero nos feriados
                df_future_processed.loc[df_future_processed['feriado'] == 1, 'TotalUNVendidas'] = 0
                df_future_processed.loc[df_future_processed['feriado'] == 1, 'ValorTotalVendido'] = 0.00  # Manter duas casas decimais

                # Preparar os dados para inserção no banco de dados
                df_to_insert = df_future_processed[['DATA', 'CodigoProduto', 'TotalUNVendidas', 'ValorTotalVendido', 'Promocao']].copy()
                df_to_insert['DATA'] = df_to_insert['DATA'].dt.strftime('%Y-%m-%d')

                insert_predictions(df_to_insert)
                success_count += 1

                # Liberar memória após o processamento de cada produto
                del df_produto, df_future, df_combined_processed
                gc.collect()

            except Exception as e:
                logger.error(f"Erro ao processar o produto {produto}: {e}")
                error_count += 1
                continue

        # Log do resumo final
        logger.info(f"Previsão concluída. Produtos processados com sucesso: {success_count}. Erros: {error_count}.")
    except Exception as e:
        logger.error(f"Erro durante a execução de previsões: {e}")

if __name__ == "__main__":
    make_predictions()
