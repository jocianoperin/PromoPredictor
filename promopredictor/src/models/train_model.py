from ..services.database_connection import get_db_connection  # Ajuste o import conforme a localização real
from ..utils.logging_config import get_logger
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
    Função para buscar dados da tabela 'promotions_identified'.
    """
    query = """
    SELECT p.*, IF(pi.id IS NULL, 0, 1) AS IsPromotion
    FROM vendasprodutosexport p
    LEFT JOIN promotions_identified pi ON p.CodigoProduto = pi.CodigoProduto
    """
    connection = get_db_connection()
    try:
        df = pd.read_sql_query(query, connection)
        logger.info("Dados carregados com sucesso.")
        return df
    except Exception as e:
        logger.error(f"Erro ao buscar dados: {e}")
        return pd.DataFrame()

def preprocess_data(df):
    """
    Preprocessa os dados: tratamento de valores nulos e normalização.
    """
    # Substituindo valores nulos pela média da coluna
    df.fillna(df.mean(), inplace=True)

    # Normalizando as colunas numéricas
    scaler = StandardScaler()
    numerical_cols = ['ValorUnitario', 'ValorCusto', 'Quantidade']
    df[numerical_cols] = scaler.fit_transform(df[numerical_cols])

    return df

def train_model():
    df = fetch_data()
    if df.empty:
        logger.error("DataFrame está vazio. Treinamento abortado.")
        return

    df = preprocess_data(df)

    X = df.drop('IsPromotion', axis=1)
    y = df['IsPromotion']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Pipeline com pré-processamento e modelo
    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('classifier', RandomForestClassifier(random_state=42))
    ])

    pipeline.fit(X_train, y_train)

    # Avaliação
    predictions = pipeline.predict(X_test)
    logger.info("\n", classification_report(y_test, predictions))

    # Salvar o modelo treinado
    joblib.dump(pipeline, 'trained_model.pkl')
    logger.info("Modelo treinado salvo com sucesso.")

if __name__ == "__main__":
    train_model()
