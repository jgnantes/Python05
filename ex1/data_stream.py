from typing import Any, List, Optional, Dict, Union
from abc import ABC, abstractmethod


class DataStream(ABC):
    """ """

    def init(self, stream_id: int) -> None:
        self.stream_id: int = stream_id
        self.stats: Dict[str, Union[str, int, float]] = {}

    def filter_data(
        self, 
        data_batch: List[Any], 
        criteria: Optional[str] = None
        ) -> List[Any]:
        """ """
        ...

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """ """
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """ """
        return self.stats


if __name__ == "__main__":
    