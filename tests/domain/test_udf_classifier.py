"""Tests for UDFClassifier — pure domain logic, no mocks."""
from __future__ import annotations

import pytest

from athenaforge.domain.services.udf_classifier import (
    UDFCategory,
    UDFClassificationResult,
    UDFClassifier,
)


class TestSQLUDFClassification:
    def test_classify_sql_with_select_case_from(self):
        classifier = UDFClassifier()
        body = "SELECT CASE WHEN amount > 100 THEN 'high' ELSE 'low' END FROM transactions"
        result = classifier.classify("compute_tier", body)

        assert result.category == UDFCategory.SQL_UDF
        assert result.udf_name == "compute_tier"

    def test_classify_sql_with_where_clause(self):
        classifier = UDFClassifier()
        body = "SELECT COUNT(*) FROM users WHERE active = TRUE"
        result = classifier.classify("count_active", body)

        assert result.category == UDFCategory.SQL_UDF

    def test_classify_simple_expression_defaults_to_sql(self):
        classifier = UDFClassifier()
        body = "COALESCE(a, b, 0)"
        result = classifier.classify("safe_value", body)

        assert result.category == UDFCategory.SQL_UDF
        assert "SQL" in result.reason


class TestJSUDFClassification:
    def test_classify_js_with_var_keyword(self):
        classifier = UDFClassifier()
        body = "var result = input * 2; return result;"
        result = classifier.classify("double_it", body)

        assert result.category == UDFCategory.JS_UDF

    def test_classify_js_with_function_keyword(self):
        classifier = UDFClassifier()
        body = "function transform(x) { return x.toUpperCase(); }"
        result = classifier.classify("upper", body)

        assert result.category == UDFCategory.JS_UDF

    def test_classify_js_with_json_parse(self):
        classifier = UDFClassifier()
        body = "var obj = JSON.parse(input); return obj.name;"
        result = classifier.classify("extract_name", body)

        assert result.category == UDFCategory.JS_UDF
        assert "JavaScript" in result.reason

    def test_classify_js_with_arrow_function(self):
        classifier = UDFClassifier()
        body = "const fn = (x) => x * 2; return fn(input);"
        result = classifier.classify("arrow_fn", body)

        assert result.category == UDFCategory.JS_UDF

    def test_classify_js_with_let_keyword(self):
        classifier = UDFClassifier()
        body = "let total = 0; total += input; return total;"
        result = classifier.classify("accumulate", body)

        assert result.category == UDFCategory.JS_UDF


class TestCloudRunRemoteClassification:
    def test_classify_with_import_statement(self):
        classifier = UDFClassifier()
        body = "import requests\nresponse = requests.get('https://api.example.com')"
        result = classifier.classify("api_call", body)

        assert result.category == UDFCategory.CLOUD_RUN_REMOTE

    def test_classify_with_class_keyword(self):
        classifier = UDFClassifier()
        body = "class Transformer:\n    def transform(self, data):\n        return data"
        result = classifier.classify("transformer", body)

        assert result.category == UDFCategory.CLOUD_RUN_REMOTE

    def test_classify_with_http_url(self):
        classifier = UDFClassifier()
        body = "endpoint = 'https://service.example.com/api/v1/transform'"
        result = classifier.classify("remote_transform", body)

        assert result.category == UDFCategory.CLOUD_RUN_REMOTE

    def test_classify_with_from_import(self):
        classifier = UDFClassifier()
        body = "from mymodule import helper\nresult = helper.process(data)"
        result = classifier.classify("module_udf", body)

        assert result.category == UDFCategory.CLOUD_RUN_REMOTE
        assert "HTTP" in result.reason or "import" in result.reason.lower() or "Remote" in result.reason

    def test_classify_with_require(self):
        classifier = UDFClassifier()
        body = "const axios = require('axios');\naxios.get('https://api.example.com');"
        result = classifier.classify("node_call", body)

        assert result.category == UDFCategory.CLOUD_RUN_REMOTE


class TestClassificationPrecedence:
    def test_remote_takes_precedence_over_js(self):
        """Cloud Run patterns are checked first, so import + JS patterns -> CLOUD_RUN_REMOTE."""
        classifier = UDFClassifier()
        body = "import json\nvar x = JSON.parse(data);"
        result = classifier.classify("mixed", body)

        assert result.category == UDFCategory.CLOUD_RUN_REMOTE

    def test_remote_takes_precedence_over_sql(self):
        classifier = UDFClassifier()
        body = "import pandas\nSELECT * FROM table"
        result = classifier.classify("mixed_sql", body)

        assert result.category == UDFCategory.CLOUD_RUN_REMOTE


class TestResultStructure:
    def test_result_has_correct_udf_name(self):
        classifier = UDFClassifier()
        result = classifier.classify("my_udf", "SELECT 1")

        assert result.udf_name == "my_udf"

    def test_result_is_frozen_dataclass(self):
        classifier = UDFClassifier()
        result = classifier.classify("my_udf", "SELECT 1")

        with pytest.raises(AttributeError):
            result.category = UDFCategory.JS_UDF  # type: ignore[misc]
