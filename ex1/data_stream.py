from typing import Any, List, Optional, Dict, Union, Tuple
from abc import ABC, abstractmethod


class DataStream(ABC):
    """ """

    def __init__(self, stream_id: str) -> None:
        """ """
        self.stream_id: str = stream_id
        self.stats: Dict[str, Union[str, int, float]] = {}

    def _check_nbr(self, data: Any) -> bool:
        """ """
        nbr: Any = 0
        try:
            nbr = float(data)
            nbr += 0
        except (ValueError, TypeError):
            return False
        return True

    def _parse_data(self, data: str) -> Tuple[Optional[str], Optional[str]]:
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
        if criteria is None or criteria is not None:
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
        if criteria is None:
            return filtered_data
        if criteria == "critical":
            critical_data: List[Any] = []
            for item in filtered_data:
                key, value = self._parse_data(item)
                if key is None or value is None or not self._check_nbr(value):
                    continue
                nbr: float = float(value)
                if key == "temp" and nbr >= 30:
                    critical_data += [item]
                elif key == "humidity" and nbr <= 30:
                    critical_data += [item]
                elif key == "pressure" and nbr <= 1000:
                    critical_data += [item]
            return critical_data
        return filtered_data

    def process_batch(self, data_batch: List[Any]) -> str:
        """ """
        filtered_batch: List[Tuple[str, float]] = [
            (key, float(value)) for item in data_batch
            for key, value in [self._parse_data(item)]
            if value
            and (key == "temp" or key == "humidity" or key == "pressure")
            and self._check_nbr(value)
        ]
        processed_batch: List[Any] = []
        amount: int = 0
        temp: float = None
        for item in filtered_batch:
            try:
                self.stats[item[0]] = item[1]
                amount += 1
                processed_batch += [f"{item[0]}:{item[1]}"]
                if item[0] == "temp":
                    temp = item[1]
            except (ValueError, TypeError):
                print(f"{item} is not a [key:value] element")
                continue

        string: str = f"Processing sensor batch: {processed_batch}\n"
        string += f"Sensor analysis: {amount} readings processed, "
        if temp is not None:
            string += f"avg temp: {temp}°C"
        else:
            string += f"no temperature data available"
        return string


class TransactionStream(DataStream):
    """ """
    type: str = "Financial Data"

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
        if criteria is None:
            return filtered_data
        if criteria == "critical":
            critical_data: List[Any] = []
            for item in filtered_data:
                key, value = self._parse_data(item)
                if key is None or value is None or not self._check_nbr(value):
                    continue
                nbr: float = float(value)
                if nbr >= 100:
                    critical_data += [item]
            return critical_data
        return filtered_data

    def process_batch(self, data_batch: List[Any]) -> str:
        """ """
        filtered_batch: List[Tuple[str, float]] = [
            (key, float(value)) for item in data_batch
            for key, value in [self._parse_data(item)]
            if value
            and (key == "buy" or key == "sell")
            and self._check_nbr(value)
        ]
        processed_batch: List[Any] = []
        amount: int = 0
        flow: float = 0
        for item in filtered_batch:
            try:
                self.stats[item[0]] = item[1]
                amount += 1
                processed_batch += [f"{item[0]}:{item[1]}"]
                if item[0] == "buy":
                    flow -= item[1]
                elif item[0] == "sell":
                    flow += item[1]
            except (ValueError, TypeError):
                print(f"{item} is not a [key:value] element")
                continue

        self.stats["total_operations"] = amount
        self.stats["net_flow"] = flow

        signal: str = ""
        if flow > 0:
            signal = "+"

        string: str = f"Processing transaction batch: {processed_batch}\n"
        string += f"Transaction analysis: {amount} operations processed, "
        string += f"net flow: {signal}{flow} units"
        return string


class EventStream(DataStream):
    """ """
    type: str = "System Events"

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
        valid_events: List[str] = ["login", "logout", "error"]
        returning_data: List[Any] = [
            item for item in filtered_data
            if item in valid_events
        ]
        if criteria == "critical":
            critical_data: List[Any] = [
                item for item in filtered_data
                if item == "error"
            ]
            return critical_data
        return returning_data

    def process_batch(self, data_batch: List[Any]) -> str:
        """ """
        filtered_batch: List[str] = [
            item for item in data_batch
            if item == "login" or item == "logout" or item == "error"
        ]
        processed_batch: List[Any] = []
        amount: int = 0
        errors: int = 0
        for item in filtered_batch:
            try:
                self.stats[item] = 1
                amount += 1
                processed_batch += [item]
                if item == "error":
                    errors += 1
            except (ValueError, TypeError):
                print(f"{item} is not a valid event")
                continue

        self.stats["total_events"] = amount
        self.stats["errors"] = errors

        string: str = f"Processing event batch: {processed_batch}\n"
        string += f"Event analysis: {amount} events processed, "
        string += f"{errors} error detected"
        return string


class StreamProcessor:
    """ """

    def __init__(self) -> None:
        """ """
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        """ """
        self.streams += [stream]

    def filter_streams(
        self, 
        data_batches: List[List[Any]], 
        criteria: Optional[str] = None
        ) -> List[List[Any]]:
        """ """
        results: List[List[Any]] = []
        i: int = 0
        for stream in self.streams:
            try:
                results += [stream.filter_data(data_batches[i], criteria)]
            except (ValueError, TypeError):
                print(f"Batch {i} could not be filtered")
            i += 1
        return results

    def process_streams(self, data_batches: List[List[Any]]) -> List[str]:
        """ """
        results: List[str] = []
        i: int = 0
        for stream in self.streams:
            try:
                results += [stream.process_batch(data_batches[i])]
            except (ValueError, TypeError):
                print(f"Batch {i} could not be processed")
            i += 1
        return results


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    sensor_stream = SensorStream("SENSOR_001")
    sensor_data = ["temp:22.5", "humidity:65", "pressure:1013"]
    print("Initializing Sensor Stream...")
    print(f"Stream ID: {sensor_stream.stream_id}, Type: {sensor_stream.type}")
    print(sensor_stream.process_batch(sensor_data), "\n")

    transaction_stream = TransactionStream("TRANS_001")
    transaction_data = ["buy:100", "sell:150", "buy:75"]
    print("Initializing Transaction Stream...")
    print(
        f"Stream ID: {transaction_stream.stream_id}, "
        f"Type: {transaction_stream.type}"
    )
    print(transaction_stream.process_batch(transaction_data), "\n")

    event_stream = EventStream("EVENT_001")
    event_data = ["login", "error", "logout"]
    print("Initializing Event Stream...")
    print(f"Stream ID: {event_stream.stream_id}, Type: {event_stream.type}")
    print(event_stream.process_batch(event_data), "\n")

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")

    processor = StreamProcessor()
    processor.add_stream(sensor_stream)
    processor.add_stream(transaction_stream)
    processor.add_stream(event_stream)

    batch_data = [
        ["temp:31", "humidity:20", "noise:44"],
        ["buy:40", "sell:120", "buy:10", "sell:5"],
        ["login", "error", "logout"]
    ]
    batch_results = processor.process_streams(batch_data)

    print("Batch 1 Results:")
    print(f"- Sensor data: {batch_results[0]}")
    print(f"- Transaction data: {batch_results[1]}")
    print(f"- Event data: {batch_results[2]}\n")

    filtered_results = processor.filter_streams(batch_data, "critical")
    print("Stream filtering active: High-priority data only")
    print(f"Filtered sensor data: {filtered_results[0]}")
    print(f"Filtered transaction data: {filtered_results[1]}")
    print(f"Filtered event data: {filtered_results[2]}")

    print("\nAll streams processed successfully. Nexus throughput optimal.")
