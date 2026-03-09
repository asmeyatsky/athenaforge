from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class PatternCategory(Enum):
    MAP_CONSTRUCTOR = "MAP_CONSTRUCTOR"
    TRANSFORM = "TRANSFORM"
    FILTER = "FILTER"
    DATE_TRUNC = "DATE_TRUNC"
    DATE_ADD = "DATE_ADD"
    TRY_CAST = "TRY_CAST"
    REGEXP_LIKE = "REGEXP_LIKE"
    APPROX_DISTINCT = "APPROX_DISTINCT"
    ARBITRARY = "ARBITRARY"
    CONTAINS = "CONTAINS"
    MAP_KEYS = "MAP_KEYS"
    MAP_VALUES = "MAP_VALUES"
    UDF = "UDF"
    OTHER = "OTHER"


@dataclass(frozen=True)
class PatternExample:
    presto_sql: str
    googlesql: str


@dataclass(frozen=True)
class SqlTranslationPattern:
    name: str
    category: PatternCategory
    description: str
    presto_pattern: str
    googlesql_replacement: str
    examples: tuple[PatternExample, ...]
