from typing import Any, List, Optional, Dict, Union, Tuple
from abc import ABC, abstractmethod


class DataStream(ABC):
    """ """

    def __init__(self, stream_id: str) -> None:
        """ """
        self.stream_id: str = stream_id
        self.stats: Dict[str, Union[str, int, float]] = {}

    def _check_nbr(self, nbr: Any) -> bool:
        """ """
        try:
            nbr = int(nbr)
        except (ValueError, TypeError):
            return False
        return True

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
        while criteria is None:
            break
        filtered_data: List[Any] = [
            item for item in data_batch 
            if isinstance(item, str)
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
    type: str = "Environmental Data"

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
        returning_data: List[Any] = [
            (key, int(value)) for item in filtered_data
            for key, value in [self._parse_data(item)]
            if (key, value) != (None, None)
            and (key == "temp" or key == "humidity" or key == "pressure")
            and self._check_nbr(value)
        ]
        if criteria is not None:
            if criteria == "critical":
                criteria_data: List[Any] = [
                ]
                pass
        return returning_data

    def process_batch(self, data_batch: List[Any]) -> str:
        """ """
        processed_batch: List[Any] = []
        amount: int = 0
        amount_temp: int = 0
        avg: float = 0
        for item in data_batch:
            try:
                self.stats[item[0]] = item[1]
                amount += 1
                processed_batch += [f"{item[0]}:{item[1]}"]
                if item[0] == "temp":
                    try:
                        avg += item[1]
                        amount_temp += 1
                    except ValueError:
                        print(f"{item[1]} is not numeric")
                        continue
            except (ValueError, TypeError):
                print(f"{item} is not a [key:value] element")
                continue
        if avg != 0 and amount_temp > 0:
            avg = avg / amount_temp
            self.stats["temp"] = avg
        return f"Processing sensor batch: {processed_batch}\nSensor analysis: {amount} readings processed, avg temp: {avg}°C"

if __name__ == "__main__":
    s_inst = SensorStream("SENSOR_001")
    s_data = s_inst.filter_data(["temp:2", "humidity:4", "temp:82", "humidity:-42"])
    print(s_inst.process_batch(s_data))
    print(s_inst.stats)
    print(s_inst.type)