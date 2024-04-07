from abc import ABC, abstractmethod

class IVendasProdutosExportRepository(ABC):
    @abstractmethod
    def clean_data(self):
        """Método para limpar dados inconsistentes da tabela vendasprodutosexport."""
        pass
