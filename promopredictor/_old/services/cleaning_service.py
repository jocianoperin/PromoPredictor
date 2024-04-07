from repositories.vendas_export_repository import VendasExportRepository
from repositories.vendas_produtos_export_repository import VendasProdutosExportRepository
from utils.logging_config import get_logger

logger = get_logger(__name__)

class CleaningService:
    def __init__(self, vendas_export_repo: VendasExportRepository, vendas_produtos_export_repo: VendasProdutosExportRepository):
        self.vendas_export_repo = vendas_export_repo
        self.vendas_produtos_export_repo = vendas_produtos_export_repo

    def perform_cleaning(self):
        logger.info("Iniciando o processo de limpeza de dados.")
        self.vendas_export_repo.clean_data()
        self.vendas_produtos_export_repo.clean_data()
        logger.info("Processo de limpeza concluído com sucesso.")
