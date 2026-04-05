from typing import Any, List
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    """ """

    @abstractmethod
    def process(self, data: Any) -> str:
        """ """
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """ """
        pass

    def format_output(self, result: str) -> str:
        """ """
        if result is not None:
            return result
        return f"Data couldn't be processed by {self.__class__.__name__}"


class NumericProcessor(DataProcessor):
    """ """

    def validate(self, data: Any) -> bool:
        """ """
        try:
            data = int(data)
            data += 0
        except (TypeError, ValueError):
            try:
                for item in data:
                    item += 0
            except (ValueError, TypeError):
                print(f"'{data}' is not a list of only numeric values")
                return False
        return True

    def process(self, data: Any) -> str:
        """ """
        summed: float = 0
        amount: int = 0
        avg: float = 0
        if self.validate(data):
            print("Validation: Numeric data verified")
            try:
                summed = float(data)
                amount = 1
                avg = summed
            except (TypeError, ValueError):
                try:
                    for item in data:
                        amount += 1
                        summed += float(item)
                    avg = summed / amount
                except ZeroDivisionError:
                    print("Numeric data is empty |", end=' ')
                    return ""
        else:
            return ""
        if summed == int(summed):
            summed = int(summed)
        result: str = f"Processed {amount} numeric values, "
        result += f"sum={summed}, avg={avg}"
        return super().format_output(result)


class TextProcessor(DataProcessor):
    """ """

    def validate(self, data: Any) -> bool:
        """ """
        try:
            data + ""
        except (ValueError, TypeError):
            return False
        return True

    def process(self, data: Any) -> str:
        """ """
        chars: int = 0
        words: int = 0
        previous_space: bool = True
        if self.validate(data):
            print("Validation: Text data verified")
            if data:
                for char in data:
                    chars += 1
                    if char != " " and previous_space:
                        words += 1
                    if char == " ":
                        previous_space = True
                    else:
                        previous_space = False
            else:
                print("Text data is empty |", end=' ')
                return ""
        else:
            print(f"'{data}' is not text")
            return ""
        result: str = f"Processed text: {chars} characters, "
        result += f"{words} words"
        return super().format_output(result)


class LogProcessor(DataProcessor):
    """ """

    def validate(self, data: Any) -> bool:
        """ """
        logs = {"ERROR", "INFO", "WARNING"}
        try:
            data += ""
            length: int = 0
            for _ in data:
                length += 1
            i: int = 0
            for char in data:
                i += 1
                if char == ":":
                    break
            level = data[:(i - 1)]
            if level not in logs or level == data or length == i:
                return False
        except (ValueError, TypeError):
            return False
        return True

    def process(self, data: Any) -> str:
        """ """
        i: int = 0
        if self.validate(data):
            print("Validation: Log entry verified")
            for char in data:
                if char == ":":
                    break
                i += 1
            level: str = data[:i]
            message: str = data[(i + 1):]
        else:
            print(f"'{data}' is not a valid log format")
            return ""
        if level == "ERROR" or level == "WARNING":
            result: str = f'[ALERT] {level} level detected:{message}'
        elif level == "INFO":
            result = f'[INFO] {level} level detected:{message}'
        return super().format_output(result)


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    np = NumericProcessor()
    numbers: List = [1, 2, 3, 4, 5]
    print("Initializing Numeric Processor...")
    print(f"Processing data: {numbers}")
    result_nbr: str = np.process(numbers)
    print(f"Output: {np.format_output(result_nbr)}\n")

    tp = TextProcessor()
    string: str = "Hello Nexus World"
    print("Initializing Text Processor...")
    print(f'Processing data: "{string}"')
    result_txt: str = tp.process(string)
    print(f"Output: {tp.format_output(result_txt)}\n")

    lp = LogProcessor()
    log: str = "ERROR: Connection timeout"
    print("Initializing Log Processor...")
    print(f'Processing data: "{log}"')
    result_log: str = lp.process(log)
    print(f"Output: {lp.format_output(result_log)}\n")

    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...\n")

    processors: List[DataProcessor] = [
        NumericProcessor(),
        TextProcessor(),
        LogProcessor()
    ]
    demo_data: List[Any] = [
        [1, 2, 3],
        "Hello Nexus",
        "INFO: System ready"
    ]

    result_1: str = processors[0].process(demo_data[0])
    result_2: str = processors[1].process(demo_data[1])
    result_3: str = processors[2].process(demo_data[2])

    print(f"\nResult 1: {result_1}")
    print(f"Result 2: {result_2}")
    print(f"Result 3: {result_3}\n")
    print("Foundation systems online. Nexus ready for advanced streams.")
