from services.cleaning_service import CleaningService
from services.db_optimization_service import DbOptimizationService
from repositories.vendas_export_repository import VendasExportRepository
from repositories.vendas_produtos_export_repository import VendasProdutosExportRepository
from utils.logging_config import get_logger

logger = get_logger(__name__)

def main():
    vendas_export_repo = VendasExportRepository()
    vendas_produtos_export_repo = VendasProdutosExportRepository()

    cleaning_service = CleaningService(vendas_export_repo, vendas_produtos_export_repo)
    optimization_service = DbOptimizationService(vendas_export_repo, vendas_produtos_export_repo)

    logger.info("Iniciando o processo de limpeza de dados.")
    cleaning_service.perform_cleaning()
    logger.info("Processo de limpeza de dados concluído.")

    logger.info("Iniciando o processo de otimização do banco de dados.")
    optimization_service.perform_optimization()
    logger.info("Processo de otimização do banco de dados concluído.")

if __name__ == "__main__":
    main()