from typing import Any, List, Optional, Dict, Union, Tuple, Protocol
from abc import ABC, abstractmethod
import collections


class ProcessingStage(Protocol):
    """Base interface for pipeline stages"""

    def process(self, data: Any) -> Any:
        """ """
        pass


class InputStage:
    """Pipeline interface for inputing data"""

    def process(self, data: Any) -> Any:
        """Validates the existence of input data"""
        if data is None:
            raise ValueError("Input data cannot be 'None'")
        return data


class TransformStage:
    """Pipeline interface for transforming data"""

    def process(self, data: Any) -> Any:
        """Checks data type and returns it transformed"""
        if isinstance(data, str):
            return data.strip()
        elif isinstance(data, list):
            return [item for item in data]
        elif isinstance(data, dict):
            return {key: value for key, value in data.items()}
        else:
            raise TypeError("Input data must be string, list or dict")


class OutputStage:
    """Pipeline interface for returning data"""

    def process(self, data: Any) -> Any:
        """Returns final pipeline output"""
        return data


class ProcessingPipeline(ABC):
    """Abstract base for pipelines with multiple stages"""

    def __init__(self) -> None:
        self.stages: List[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage) -> None:
        """ """
        self.stages += [stage]

    def run_stages(self, data: Any) -> Any:
        """ """
        current_data: Any = data
        for stage in self.stages:
            current_data = stage.process(current_data)
        return current_data

    @abstractmethod
    def process(self, data: Any) -> Any:
        """ """
        pass


class JSONAdapter(ProcessingPipeline):
    """Concrete pipeline for JSON"""

    def __init__(self, pipeline_id: str) -> None:
        """Initilizes JSONAdapter"""
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        """If possible, processes dicts into a JSON format"""
        processed_data: Any = self.run_stages(data)
        if isinstance(processed_data, dict):
            items: List[str] = []
            for key, value in processed_data.items():
                items += [f'"{key}": "{value}"']
            return "{ " + ", ".join(items) + " }"
        return processed_data


class CSVAdapter(ProcessingPipeline):
    """Concrete pipeline for table data"""

    def __init__(self, pipeline_id: str) -> None:
        """Initializes CSVAdapter"""
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        """If possible, processes lists into table data"""
        processed_data: Any = self.run_stages(data)
        if isinstance(processed_data, list):
            return ",".join([str(item) for item in processed_data])
        return processed_data


class StreamAdapter(ProcessingPipeline):
    """Concrete pipeline for generic data flow"""

    def __init__(self, pipeline_id: str) -> None:
        """Initializes StreamAdapter"""
        super().__init__()
        self.pipeline_id = pipeline_id
        self.stats: Dict[str, Union[str, int]] = {}

    def process(self, data: Any) -> Union[str, Any]:
        """Counts how many of each data type an iterable has"""
        processed_data: Any = self.run_stages(data)
        counter: collections.Counter = collections.Counter()
        if isinstance(processed_data, list):
            for item in processed_data:
                counter[type(item).__name__] += 1
            self.stats = dict(counter)
        else:
            self.stats = {"items": 1}
        return processed_data


class NexusManager:
    """Orchestrator of multiple polymorphic pipelines i guess"""

    def __init__(self) -> None:
        """Initializes NexusManager"""
        self.pipelines: List[ProcessingPipeline] = []

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        """Adds a pipeline to the manager's list"""
        self.pipelines += [pipeline]

    def run_all(self, data: Any) -> List[Any]:
        """ """
        results: List[Any] = []
        for pipeline in self.pipelines:
            try:
                results += [pipeline.process(data)]
            except (ValueError, TypeError) as e:
                results += [f"Error with {type(pipeline).__name__}: {e}"]
        return results


if __name__ == "__main__":
    input_stage = InputStage()
    transform_stage = TransformStage()
    output_stage = OutputStage()

    json_pipeline = JSONAdapter("JSON_001")
    json_pipeline.add_stage(input_stage)
    json_pipeline.add_stage(transform_stage)
    json_pipeline.add_stage(output_stage)

    csv_pipeline = CSVAdapter("CSV_001")
    csv_pipeline.add_stage(input_stage)
    csv_pipeline.add_stage(transform_stage)
    csv_pipeline.add_stage(output_stage)

    stream_pipeline = StreamAdapter("STREAM_001")
    stream_pipeline.add_stage(input_stage)
    stream_pipeline.add_stage(transform_stage)
    stream_pipeline.add_stage(output_stage)

    manager = NexusManager()
    manager.add_pipeline(json_pipeline)
    manager.add_pipeline(csv_pipeline)
    manager.add_pipeline(stream_pipeline)

    print(json_pipeline.process({"name": "Neo", "role": "engineer"}))
    print(csv_pipeline.process(["alpha", "beta", "gamma"]))
    print(stream_pipeline.process([1, "x", 2, "y", 3, [2, 3]]))
    print(stream_pipeline.stats)
    print(manager.run_all(["one", "two", "three"]))
