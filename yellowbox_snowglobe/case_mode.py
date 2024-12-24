from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Container


class CaseMode(ABC):
    @abstractmethod
    def convert(self, s: str, column_names: Container[str]) -> str:
        pass


class IgnoreAll(CaseMode):
    """
    Will not change the column name from postgres results (column names will be lowercase)
    """

    def convert(self, s: str, column_names: Container[str]) -> str:
        return s


@dataclass
class Upper(CaseMode):
    """
    Will convert all column names to uppercase
    """

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

    def convert(self, s: str, column_names: Container[str]) -> str:
        if (s not in self.force_ignore and s in self.force_upper) or s in column_names:
            return s.upper()
        return s
