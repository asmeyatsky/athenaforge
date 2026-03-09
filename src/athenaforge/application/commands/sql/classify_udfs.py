from __future__ import annotations

from athenaforge.application.dtos.sql_dtos import UDFClassificationReport
from athenaforge.domain.ports.event_bus import EventBusPort
from athenaforge.domain.services.udf_classifier import UDFCategory, UDFClassifier


class ClassifyUDFsUseCase:
    """Classifies a set of UDFs into SQL, JavaScript, or Cloud Run categories."""

    def __init__(
        self,
        classifier: UDFClassifier,
        event_bus: EventBusPort,
    ) -> None:
        self._classifier = classifier
        self._event_bus = event_bus

    async def execute(
        self, udfs: dict[str, str]
    ) -> UDFClassificationReport:
        """Classify each UDF from a name-to-body mapping.

        Returns a ``UDFClassificationReport`` with counts per category and
        a mapping of UDF name to its classification string.
        """
        classifications: dict[str, str] = {}
        sql_count = 0
        js_count = 0
        cloud_run_count = 0

        for udf_name, udf_body in udfs.items():
            result = self._classifier.classify(udf_name, udf_body)
            classifications[udf_name] = result.category.value

            if result.category is UDFCategory.SQL_UDF:
                sql_count += 1
            elif result.category is UDFCategory.JS_UDF:
                js_count += 1
            elif result.category is UDFCategory.CLOUD_RUN_REMOTE:
                cloud_run_count += 1

        return UDFClassificationReport(
            total_udfs=len(udfs),
            sql_udfs=sql_count,
            js_udfs=js_count,
            cloud_run_udfs=cloud_run_count,
            classifications=classifications,
        )
