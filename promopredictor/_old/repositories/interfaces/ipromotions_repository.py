from abc import ABC, abstractmethod
from typing import List, Dict, Any

class IPromotionsRepository(ABC):

    @abstractmethod
    def create_promotions_table_if_not_exists(self):
        pass

    @abstractmethod
    def insert_promotion(self, promo: Dict[str, Any]):
        pass

    @abstractmethod
    def fetch_all_products(self) -> List[Dict[str, Any]]:
        pass
