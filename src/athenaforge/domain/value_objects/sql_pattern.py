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
    # Extended categories for 90+ pattern library
    STRING_FUNCTIONS = "STRING_FUNCTIONS"
    DATE_FUNCTIONS = "DATE_FUNCTIONS"
    ARRAY_FUNCTIONS = "ARRAY_FUNCTIONS"
    AGGREGATE_FUNCTIONS = "AGGREGATE_FUNCTIONS"
    APPROX_FUNCTIONS = "APPROX_FUNCTIONS"
    TYPE_CASTING = "TYPE_CASTING"
    CONDITIONAL_FUNCTIONS = "CONDITIONAL_FUNCTIONS"
    JSON_FUNCTIONS = "JSON_FUNCTIONS"
    WINDOW_FUNCTIONS = "WINDOW_FUNCTIONS"
    TABLE_FUNCTIONS = "TABLE_FUNCTIONS"
    BITWISE_FUNCTIONS = "BITWISE_FUNCTIONS"
    MATH_FUNCTIONS = "MATH_FUNCTIONS"
    REGEXP_FUNCTIONS = "REGEXP_FUNCTIONS"
    MAP_FUNCTIONS = "MAP_FUNCTIONS"
    MISC_FUNCTIONS = "MISC_FUNCTIONS"
    PRESTO_SYNTAX = "PRESTO_SYNTAX"


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
