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
        if self.validate(result):
            return f"formatted: {result}"
        else:
            return None


class NumericProcessor(DataProcessor):
    """ """

    def validate(self, data: Any) -> bool:
        """ """
        try:
            test = int(data)
            test += 0
        except (TypeError, ValueError):
                for item in data:
                    try:
                        item += 0
                    except (ValueError, TypeError):
                        print(f"'{data}' is not a list of only numeric values")
                        return False
        print("Validation: Numeric data verified")
        return True

    def process(self, data: Any) -> str:
        """ """
        print("Initializing Numeric Processor...")
        print(f"Processing data: {data}")
        amount: int = 0
        summed: int = 0
        if self.validate(data):
            try:
                for item in data:
                    amount += 1
                    summed += int(item)
            except TypeError:
                amount = 1
                summed = data
            except ZeroDivisionError:
                print("Numeric data is empty |", end = ' ')
                return None
        else:
            return None
        avg = summed / amount
        return amount, summed, avg

    def format_output(self, result: str):
        """ """
        if result is not None:
            output: str = f"Output: Processed {result[0]} numeric values, "
            output += f"sum={result[1]}, avg={result[2]}\n"
            return output
        else:
            return "Data could not be processed\n"


class TextProcessor(DataProcessor):
    """ """

    def validate(self, data: Any) -> bool:
        """ """
        try:
            data + ""
        except (ValueError, TypeError):
            return False
        print("Validation: Text data verified")
        return True
    
    def process(self, data: Any) -> str:
        """ """
        print("Initializing Text Processor...")
        print(f'Processing data: "{data}"')
        chars: int = 0
        words: int = 0
        if self.validate(data):
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
        return chars, words
    
    def format_output(self, result: str) -> str:
        """ """
        if result is not None:
            output: str = f"Output: Processed text: {result[0]} characters, "
            output += f"{result[1]} words\n"
            return output
        else:
            return "Data could not be processed\n"


class LogProcessor(DataProcessor):
    """ """

    def validate(self, data: Any) -> bool:
        """ """
        logs = {"ERROR", "INFO", "WARNING"}
        try:
            data += ""
            i: int = 0
            for char in data:
                if char == ":":
                    break
                i += 1
            level = data[:i]
            if level not in logs or level == data:
                print(f"{data} is not a valid log format")
                return False
        except (ValueError, TypeError):
            print(f"{data} is not a valid log format")
            return False
        print("Validation: Log entry verified")
        return True


    def process(self, data: Any) -> str:
        """ """

    def format_output(self, result: str) -> str:
        """ """


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    pn = NumericProcessor()
    numbers: List = [1, 2, 3, 4, 5]
    result_n = pn.process(numbers)
    print(f"{pn.format_output(result_n)}")
    pt = TextProcessor()
    string: str = "ton cheval est très joli"
    result_t = pt.process(string)
    print(f"{pt.format_output(result_t)}")
