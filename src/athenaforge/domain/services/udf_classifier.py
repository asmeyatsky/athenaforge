from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum


class UDFCategory(Enum):
    SQL_UDF = "SQL_UDF"
    JS_UDF = "JS_UDF"
    CLOUD_RUN_REMOTE = "CLOUD_RUN_REMOTE"


@dataclass(frozen=True)
class UDFClassificationResult:
    udf_name: str
    category: UDFCategory
    reason: str


# JavaScript indicators
_JS_PATTERNS = re.compile(
    r"\b(?:var|let|const|function)\b"
    r"|=>"
    r"|JSON\.parse"
    r"|JSON\.stringify"
    r"|console\.log",
)

# Cloud Run / Remote indicators
_REMOTE_PATTERNS = re.compile(
    r"\b(?:import|class|public|private|protected)\b"
    r"|https?://"
    r"|require\s*\("
    r"|from\s+\S+\s+import",
)

# SQL-only keywords (used to positively identify pure SQL)
_SQL_KEYWORDS = re.compile(
    r"\b(?:SELECT|FROM|WHERE|CASE|WHEN|THEN|ELSE|END|"
    r"GROUP\s+BY|ORDER\s+BY|JOIN|LEFT|RIGHT|INNER|OUTER|"
    r"UNION|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|"
    r"AND|OR|NOT|IN|EXISTS|BETWEEN|LIKE|IS|NULL|"
    r"HAVING|LIMIT|OFFSET|AS|ON|SET|VALUES|INTO|"
    r"CAST|COALESCE|IFNULL|NULLIF|COUNT|SUM|AVG|MIN|MAX|"
    r"DISTINCT|ALL|ANY|SOME|CROSS|FULL|NATURAL|USING|"
    r"WITH|RECURSIVE|OVER|PARTITION|ROW_NUMBER|RANK|"
    r"DENSE_RANK|LEAD|LAG|FIRST_VALUE|LAST_VALUE|"
    r"RETURN|RETURNS|BEGIN|DECLARE|IF|WHILE|LOOP|"
    r"INT|INTEGER|STRING|FLOAT|DOUBLE|BOOLEAN|DATE|TIMESTAMP|"
    r"ARRAY|STRUCT|MAP|NUMERIC|DECIMAL|BIGINT|SMALLINT|"
    r"TRUE|FALSE)\b",
    re.IGNORECASE,
)


class UDFClassifier:
    """Pure domain service that classifies UDF bodies by implementation type."""

    def classify(self, udf_name: str, udf_body: str) -> UDFClassificationResult:
        """Classify a UDF based on its body content."""
        # Check for Cloud Run / Remote patterns first (most specific)
        if _REMOTE_PATTERNS.search(udf_body):
            return UDFClassificationResult(
                udf_name=udf_name,
                category=UDFCategory.CLOUD_RUN_REMOTE,
                reason="Body contains HTTP calls, external imports, or complex Java patterns",
            )

        # Check for JavaScript patterns
        if _JS_PATTERNS.search(udf_body):
            return UDFClassificationResult(
                udf_name=udf_name,
                category=UDFCategory.JS_UDF,
                reason="Body contains JavaScript patterns",
            )

        # Default to SQL UDF
        return UDFClassificationResult(
            udf_name=udf_name,
            category=UDFCategory.SQL_UDF,
            reason="Body contains only SQL constructs",
        )
