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

    def validate(self, data: List) -> bool:
        """ """
        for item in data:
            try:
                item += 0
            except (ValueError, TypeError):
                print(f"'{item}' from {data} is not numeric")
                return False
        print("Validation: Numeric data verified")
        return True
    
    def process(self, data: List) -> str:
        """ """
        print("Initializing Numeric Processor...")
        print(f"Processing data: {data}")
        if self.validate(data):
            amount = 0
            summed = 0
            for item in data:
                amount += 1
                summed += item
            avg = summed / amount
        else:
            return None
        return amount, summed, avg
    
    def format_output(self, result: Tuple):
        if result is not None:
            return f"Output: Processed {result[0]} numeric values, sum={result[1]}, avg={result[2]}\n"
        else:
            return "Data could not be processed\n"
    

class TextProcessor(DataProcessor):
    """ """

    def validate(self, data: str) -> bool:
        """ """
        try:
            data + ""
        except (ValueError, TypeError):
            return False
        print("Validation: Text data verified")
        return True
    
    def process(self, data: str) -> str:
        """ """
        print("Initializing Text Processor...")
        print(f"Processing data: {data}")
        chars: int = 0
        words: int = 0
        if self.validate(data):
            if data[0] != " " and data[0] != '\0':
                words = 1
            for char in data:
                if char == " " and data[chars + 1] != " " and data[chars + 1] != '\0':
                    words += 1
                chars += 1
        else:
            print(f"'{data}' is not text")
            return None
        return chars, words
    
    def format_output(self, result: str) -> str:
        if result is not None:
            return f"Output: Processed text: {result[0]} characters, {result[1]} words"
        else:
            return "Data could not be processed"


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    pn = NumericProcessor()
    numbers: list = [1, 2, 3, 4, 5]
    result_n = pn.process(numbers)
    print(f"{pn.format_output(result_n)}")
    pt = TextProcessor()
    string: str = "Hey Ho Let's Go"
    result_t = pt.process(string)
    print(f"{pt.format_output(result_t)}")
