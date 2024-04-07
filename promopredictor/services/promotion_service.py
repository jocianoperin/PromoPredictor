from repositories.promotions_repository import PromotionsRepository
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.logging_config import get_logger

logger = get_logger(__name__)

class PromotionService:
    def __init__(self, promotions_repository: PromotionsRepository):
        self.promotions_repository = promotions_repository

    def identify_and_insert_promotions(self):
        # Garanta que a tabela de promoções exista
        self.promotions_repository.create_promotions_table_if_not_exists()

        # Busque todos os produtos
        all_products = self.promotions_repository.fetch_all_products()

        # Agrupe os produtos por CodigoProduto e processe cada grupo em chunks
        grouped_products = self._group_products_by_code(all_products)

        # Identifique e insira promoções processando os dados em chunks
        self.promotions_repository.process_chunks(grouped_products)

    def _group_products_by_code(self, products):
        grouped = {}
        for product in products:
            key = product['CodigoProduto']
            if key not in grouped:
                grouped[key] = {'CodigoProduto': key, 'Entries': []}
            grouped[key]['Entries'].append(product)
        return list(grouped.values())
