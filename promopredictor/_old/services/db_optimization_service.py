from repositories.vendas_export_repository import VendasExportRepository
from repositories.vendas_produtos_export_repository import VendasProdutosExportRepository
from utils.logging_config import get_logger

logger = get_logger(__name__)

class DbOptimizationService:
    def __init__(self, vendas_export_repo: VendasExportRepository, vendas_produtos_export_repo: VendasProdutosExportRepository):
        self.vendas_export_repo = vendas_export_repo
        self.vendas_produtos_export_repo = vendas_produtos_export_repo

    def perform_optimization(self):
        logger.info("Iniciando o processo de otimização do banco de dados.")

        # Chamar métodos específicos para a criação de índices nas tabelas vendasexport e vendasprodutosexport
        # Assumindo que tais métodos sejam implementados nos repositórios fornecidos
        self.vendas_export_repo.create_indexes()
        self.vendas_produtos_export_repo.create_indexes()

        logger.info("Processo de otimização do banco de dados concluído com sucesso.")