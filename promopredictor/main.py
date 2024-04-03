from services.cleaning_service import CleaningService
from repositories.vendas_export_repository import VendasExportRepository
from repositories.vendas_produtos_export_repository import VendasProdutosExportRepository

def main():
    vendas_export_repo = VendasExportRepository()
    vendas_produtos_export_repo = VendasProdutosExportRepository()
    
    cleaning_service = CleaningService(vendas_export_repo, vendas_produtos_export_repo)
    cleaning_service.perform_cleaning()

if __name__ == "__main__":
    main()