from abc import ABC, abstractmethod


class TableBase(ABC):

    @abstractmethod
    def from_csv(self, *args, **kwargs) -> "TableBase":
        pass

    @abstractmethod
    def from_parquet(self, *args, **kwargs) -> "TableBase":
        pass
