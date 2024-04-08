from flask_sqlalchemy import SQLAlchemy
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

db = SQLAlchemy()

def init_db(app):
    """
    Inicializa a conexão ao banco de dados com as configurações do app Flask.
    """
    app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:1@localhost/atena'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.init_app(app)

    with app.app_context():
        try:
            # Cria todas as tabelas baseadas nos modelos, se não existirem.
            db.create_all()
        except Exception as e:
            logger.error(f"Erro ao criar tabelas: {e}")
