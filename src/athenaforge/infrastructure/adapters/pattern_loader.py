from __future__ import annotations

import yaml

from athenaforge.domain.value_objects.sql_pattern import (
    PatternCategory,
    PatternExample,
    SqlTranslationPattern,
)


class PatternLoader:
    """Loads SQL translation patterns from a YAML file into domain value objects.

    Expected YAML structure::

        - name: map_constructor
          category: MAP_CONSTRUCTOR
          description: Convert MAP() constructor syntax
          presto_pattern: "MAP(ARRAY[...], ARRAY[...])"
          googlesql_replacement: "STRUCT(...)"
          examples:
            - presto_sql: "SELECT MAP(ARRAY[1], ARRAY['a'])"
              googlesql: "SELECT STRUCT(1 AS key, 'a' AS value)"
    """

    def load(self, path: str) -> list[SqlTranslationPattern]:
        with open(path, "r") as f:
            raw: list[dict] = yaml.safe_load(f) or []

        patterns: list[SqlTranslationPattern] = []
        for entry in raw:
            examples = tuple(
                PatternExample(
                    presto_sql=ex["presto_sql"],
                    googlesql=ex["googlesql"],
                )
                for ex in entry.get("examples", [])
            )
            pattern = SqlTranslationPattern(
                name=entry["name"],
                category=PatternCategory(entry["category"]),
                description=entry.get("description", ""),
                presto_pattern=entry["presto_pattern"],
                googlesql_replacement=entry["googlesql_replacement"],
                examples=examples,
            )
            patterns.append(pattern)
        return patterns
