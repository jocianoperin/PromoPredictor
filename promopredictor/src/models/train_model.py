from src.services.database import db_manager
from src.utils.logging_config import get_logger
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
import pandas as pd
import joblib


logger = get_logger(__name__)

def fetch_data():
    """
    Extrai dados da tabela 'promotions_identified' para treinamento do modelo.
    Retorna um DataFrame pandas com os dados.
    """
    query = "SELECT * FROM promotions_identified"
    data = db_manager.execute_query(query)
    return pd.DataFrame(data['data'], columns=data['columns'])

def preprocess_data(df):
    """
    Aplica pré-processamento nos dados obtidos: preenchimento de valores nulos e normalização.
    Args:
        df (pd.DataFrame): DataFrame com os dados a serem preprocessados.
    Returns:
        pd.DataFrame: DataFrame com os dados preprocessados.
    """
    # Tratamento de valores nulos
    df.fillna(df.mean(), inplace=True)

    # Normalização das colunas numéricas
    scaler = StandardScaler()
    numerical_cols = ['ValorUnitario', 'ValorCusto', 'Quantidade']
    df[numerical_cols] = scaler.fit_transform(df[numerical_cols])

    return df

def train_model():
    """
    Configura e treina um modelo de RandomForest para classificação de promoções.
    Salva o modelo treinado e exibe um relatório de classificação.
    """
    df = fetch_data()
    if df.empty:
        logger.error("DataFrame está vazio. Treinamento abortado.")
        return

    df = preprocess_data(df)
    X = df.drop('IsPromotion', axis=1)
    y = df['IsPromotion']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Configuração do pipeline
    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('classifier', RandomForestClassifier(random_state=42))
    ])
    pipeline.fit(X_train, y_train)

    # Avaliação do modelo
    predictions = pipeline.predict(X_test)
    logger.info("\n" + classification_report(y_test, predictions))

    # Salvando o modelo treinado
    joblib.dump(pipeline, 'trained_model.pkl')
    logger.info("Modelo treinado salvo com sucesso.")

if __name__ == "__main__":
    train_model()
