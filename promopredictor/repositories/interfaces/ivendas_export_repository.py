from abc import ABC, abstractmethod

class IVendasExportRepository(ABC):
    @abstractmethod
    def clean_data(self):
        """Método para limpar dados inconsistentes da tabela vendasexport."""
        pass
