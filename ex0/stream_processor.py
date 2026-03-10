from typing import Any, List, Tuple
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
        return f"Data couldn't be processed by {self.__class__.__name__}\n"


class NumericProcessor(DataProcessor):
    """ """

    def validate(self, data: Any) -> bool:
        """ """
        try:
            data = int(data)
            data += 0
        except (TypeError, ValueError):
                for item in data:
                    try:
                        item += 0
                    except (ValueError, TypeError):
                        print(f"'{data}' is not a list of only numeric values")
                        return False
        return True

    def process(self, data: Any) -> str:
        """ """
        amount: int = 0
        summed: int = 0
        try:
            summed = int(data)
            amount = 1
        except TypeError:
            if self.validate(data):
                print("Validation: Numeric data verified")
                try:
                    for item in data:
                        amount += 1
                        summed += int(item)
                except ZeroDivisionError:
                    print("Numeric data is empty |", end = ' ')
                    return None
            else:
                return None
        avg = summed / amount
        result: str = f"Output: Processed {amount} numeric values, "
        result += f"sum={summed}, avg={avg}\n"
        return result


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
        if self.validate(data):
            print("Validation: Text data verified")
            if data:
                chars += 1
                if data[0] != " ":
                    words += 1
            else:
                print("Text data is empty |", end = ' ')
                return None
            for char in data[1:]:
                if char == " " and data[chars - 1] != " ":
                    words += 1
                chars += 1
        else:
            print(f"'{data}' is not text")
            return None
        result: str = f"Output: Processed text: {chars} characters, "
        result += f"{words} words\n"
        return result


class LogProcessor(DataProcessor):
    """ """

    def validate(self, data: Any) -> bool:
        """ """
        logs = {"ERROR", "INFO", "WARNING"}
        try:
            data += ""
            len: int = 0
            for _ in data:
                len += 1
            i: int = 0
            for char in data:
                i += 1
                if char == ":":
                    break
            level = data[:(i - 1)]
            if level not in logs or level == data or len == i:
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
            return None
        result: str = f'Output: [ALERT] {level} level detected:{message}\n'
        return result


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    np = NumericProcessor()
    numbers: List = [1, 2, 3, 4, 5]
    print("Initializing Numeric Processor...")
    print(f"Processing data: {numbers}")
    print(f"{np.format_output(np.process(numbers))}")

    tp = TextProcessor()
    string: str = "ton cheval est très joli"
    print("Initializing Text Processor...")
    print(f'Processing data: "{string}"')
    print(f"{tp.format_output(tp.process(string))}")

    lp = LogProcessor()
    log: str = "ERROR: Connection timeout"
    print("Initializing Log Processor...")
    print(f'Processing data: "{log}"')
    print(f"{lp.format_output(lp.process(log))}")

    print("=== Polymorphic Processing Demo ===")
