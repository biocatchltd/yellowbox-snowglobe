from abc import ABC, abstractmethod
from typing import Container, Union, ClassVar

from dataclasses import dataclass
from enum import Enum, auto


class CaseMode(ABC):
    load_column_names: bool

    @abstractmethod
    def convert(self, s: str, column_names: Container[str]) -> str:
        pass

class IgnoreAll(CaseMode):
    """
    Will not change the column name from postgres results (column names will be lowercase)
    """
    load_column_names: ClassVar[bool] = False

    def convert(self, s: str, column_names: Container[str]) -> str:
        return s

@dataclass
class Upper(CaseMode):
    """
    Will convert all column names to uppercase
    """
    load_column_names: ClassVar[bool] = False

    ignore: Container[str] = ()
    """
    These column names will not be converted to uppercase
    """

    def convert(self, s: str, column_names: Container[str]) -> str:
        if s in self.ignore:
            return s
        return s.upper()


@dataclass
class AutoCase(CaseMode):
    """
    Will dynamically fetch all column names from the database and convert them to uppercase
    """
    force_ignore: Container[str] = ()
    """
    These column names will not be changed even if they appear in the database
    """
    force_upper: Container[str] = ()
    """
    These column names will be converted to uppercase even if they do not appear in the database
    """

    load_column_names: bool = True

    def convert(self, s: str, column_names: Container[str]) -> str:
        if s not in self.force_ignore and s in self.force_upper or (self.load_column_names and s in column_names):
            return s.upper()
        return s
