from typing import Any, List, Optional, Dict, Union, Tuple
from abc import ABC, abstractmethod


class DataStream(ABC):
    """ """

    def __init__(self, stream_id: str) -> None:
        """ """
        self.stream_id: str = stream_id
        self.stats: Dict[str, Union[str, int, float]] = {}

    def _parse_data(self, data: str) -> Tuple[Optional[str], Optional[Any]]:
        """ """
        length: int = 0
        for _ in data:
            length += 1
        i: int = 0
        for char in data:
            if char == ":" and i != length - 1 and i != 0:
                key: str = data[:i]
                value: Any = data[(i + 1):]
                return key, value
            i += 1
        return None, None

    def filter_data(
        self, 
        data_batch: List[Any], 
        criteria: Optional[str] = None
        ) -> List[Any]:
        """ """
        filtered_data: List[Any] = [
            item for item in data_batch if isinstance(item, str)
            ]
        return filtered_data

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """ """
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """ """
        return self.stats


class SensorStream(DataStream):
    """ """

    def __init__(self, stream_id: str) -> None:
        """ """
        super().__init__(stream_id)

    def filter_data(
        self, 
        data_batch: List[Any], 
        criteria: Optional[str] = None
        ) -> List[Any]:
        """ """
        filtered_data: List[Any] = super().filter_data(data_batch, criteria)
        if criteria == None:
            returning_data: List[Any] = [
                self._parse_data(item) for item in filtered_data
                if self._parse_data(item) != (None, None)
            ]
        return returning_data

    def process_batch(self, data_batch: List[Any]) -> str:
        """ """
        pass

if __name__ == "__main__":
    test = SensorStream("SENSOR_001")
    data: list = ["a:b", "b:a", 3124]
    print(test.filter_data(data))