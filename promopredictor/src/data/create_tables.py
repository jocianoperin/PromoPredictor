from src.models import db  # Supondo que db seja a instância de SQLAlchemy configurada
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def create_tables_if_not_exists(app):
    with app.app_context():
        try:
            db.create_all()
            logger.info("Todas as tabelas necessárias verificadas/criadas com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao criar tabelas: {e}")

if __name__ == "__main__":
    from src.api import create_app  # Supondo que você tenha uma função para criar sua aplicação Flask
    app = create_app()
    create_tables_if_not_exists(app)
