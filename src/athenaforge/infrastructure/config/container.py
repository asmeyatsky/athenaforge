from __future__ import annotations

import os

from athenaforge.infrastructure.config.app_config import AppConfig

# ── adapters ─────────────────────────────────────────────────────
from athenaforge.infrastructure.adapters.yaml_config_adapter import YamlConfigAdapter
from athenaforge.infrastructure.adapters.jinja_terraform_adapter import JinjaTerraformAdapter
from athenaforge.infrastructure.adapters.bqms_translation_adapter import BqmsTranslationAdapter
from athenaforge.infrastructure.adapters.bigquery_adapter import BigQueryAdapter
from athenaforge.infrastructure.adapters.gcs_storage_adapter import GcsStorageAdapter
from athenaforge.infrastructure.adapters.s3_storage_adapter import S3StorageAdapter
from athenaforge.infrastructure.adapters.in_memory_event_bus import InMemoryEventBus
from athenaforge.infrastructure.adapters.sts_transfer_adapter import StsTransferAdapter
from athenaforge.infrastructure.adapters.dvt_adapter import DvtAdapter
from athenaforge.infrastructure.adapters.pattern_loader import PatternLoader

# ── repositories ─────────────────────────────────────────────────
from athenaforge.infrastructure.repositories import (
    TableInventoryRepository,
    TranslationBatchRepository,
    WaveRepository,
    TransferJobRepository,
    StreamingJobRepository,
)

# ── domain services ─────────────────────────────────────────────
from athenaforge.domain.services.tier_classification_service import TierClassificationService
from athenaforge.domain.services.sql_pattern_matcher import SqlPatternMatcher
from athenaforge.domain.services.wave_planner_service import WavePlannerService
from athenaforge.domain.services.cost_calculator import EgressCostCalculator, SlotPricingCalculator
from athenaforge.domain.services.rollback_evaluator import RollbackEvaluator
from athenaforge.domain.services.dependency_scanner import DependencyScanner
from athenaforge.domain.services.dag_rewriter_service import DAGRewriterService
from athenaforge.domain.services.parallel_running_state_machine import ParallelRunningStateMachine
from athenaforge.domain.services.udf_classifier import UDFClassifier
from athenaforge.domain.services.delta_log_health_service import DeltaLogHealthService
from athenaforge.domain.services.map_cascade_analyser import MapCascadeAnalyser

# ── value objects ────────────────────────────────────────────────
from athenaforge.domain.value_objects.sql_pattern import SqlTranslationPattern

# ── use cases ────────────────────────────────────────────────────
from athenaforge.application.commands.foundation.generate_scaffold import GenerateScaffoldUseCase
from athenaforge.application.commands.foundation.classify_tiers import ClassifyTiersUseCase
from athenaforge.application.commands.foundation.configure_pricing import ConfigurePricingUseCase
from athenaforge.application.commands.foundation.bootstrap_dataplex import BootstrapDataplexUseCase
from athenaforge.application.commands.foundation.check_delta_health import CheckDeltaHealthUseCase
from athenaforge.application.commands.sql.translate_batch import TranslateBatchUseCase
from athenaforge.application.commands.sql.analyse_map_cascade import AnalyseMapCascadeUseCase
from athenaforge.application.commands.sql.normalise_case_sensitivity import NormaliseCaseSensitivityUseCase
from athenaforge.application.commands.sql.classify_udfs import ClassifyUDFsUseCase
from athenaforge.application.commands.sql.validate_queries import ValidateQueriesUseCase
from athenaforge.application.commands.transfer.plan_delta_compaction import PlanDeltaCompactionUseCase
from athenaforge.application.commands.transfer.model_egress_cost import ModelEgressCostUseCase
from athenaforge.application.commands.transfer.create_sts_jobs import CreateSTSJobsUseCase
from athenaforge.application.commands.transfer.run_dvt_validation import RunDVTValidationUseCase
from athenaforge.application.commands.transfer.control_streaming_cutover import ControlStreamingCutoverUseCase
from athenaforge.application.commands.wave.plan_waves import PlanWavesUseCase
from athenaforge.application.commands.wave.evaluate_rollback import EvaluateRollbackUseCase
from athenaforge.application.commands.wave.enforce_wave_gate import EnforceWaveGateUseCase
from athenaforge.application.commands.wave.migrate_dashboards import MigrateDashboardsUseCase
from athenaforge.application.commands.wave.reconcile_kpis import ReconcileKPIsUseCase
from athenaforge.application.commands.wave.control_parallel_run import ControlParallelRunUseCase
from athenaforge.application.commands.dependency.scan_spark_flink_jobs import ScanSparkFlinkJobsUseCase
from athenaforge.application.commands.dependency.rewrite_dags import RewriteDAGsUseCase
from athenaforge.application.commands.dependency.migrate_kafka_topics import MigrateKafkaTopicsUseCase
from athenaforge.application.commands.dependency.rewrite_lambdas import RewriteLambdasUseCase
from athenaforge.application.commands.dependency.map_iam_permissions import MapIAMPermissionsUseCase
from athenaforge.application.queries.foundation.get_tier_summary import GetTierSummaryQuery

class DependencyContainer:
    """Composition root that lazily wires all adapters, services, and use cases."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config

        # Lazy singletons
        self._event_bus: InMemoryEventBus | None = None
        self._yaml_config_adapter: YamlConfigAdapter | None = None
        self._terraform_adapter: JinjaTerraformAdapter | None = None
        self._bqms_adapter: BqmsTranslationAdapter | None = None
        self._bigquery_adapter: BigQueryAdapter | None = None
        self._gcs_adapter: GcsStorageAdapter | None = None
        self._s3_adapter: S3StorageAdapter | None = None
        self._sts_adapter: StsTransferAdapter | None = None
        self._dvt_adapter: DvtAdapter | None = None
        self._table_inventory_repo: TableInventoryRepository | None = None
        self._translation_batch_repo: TranslationBatchRepository | None = None
        self._wave_repo: WaveRepository | None = None
        self._transfer_job_repo: TransferJobRepository | None = None
        self._streaming_job_repo: StreamingJobRepository | None = None
        self._tier_service: TierClassificationService | None = None
        self._pattern_matcher: SqlPatternMatcher | None = None
        self._wave_planner: WavePlannerService | None = None
        self._egress_calculator: EgressCostCalculator | None = None
        self._slot_calculator: SlotPricingCalculator | None = None
        self._rollback_evaluator: RollbackEvaluator | None = None
        self._dependency_scanner: DependencyScanner | None = None
        self._dag_rewriter: DAGRewriterService | None = None
        self._parallel_run_sm: ParallelRunningStateMachine | None = None
        self._udf_classifier: UDFClassifier | None = None
        self._delta_health_service: DeltaLogHealthService | None = None
        self._map_cascade_analyser: MapCascadeAnalyser | None = None

    # ── config ───────────────────────────────────────────────────

    @property
    def config(self) -> AppConfig:
        return self._config

    # ── event bus ────────────────────────────────────────────────

    @property
    def event_bus(self) -> InMemoryEventBus:
        if self._event_bus is None:
            self._event_bus = InMemoryEventBus()
        return self._event_bus

    # ── adapters ─────────────────────────────────────────────────

    @property
    def yaml_config_adapter(self) -> YamlConfigAdapter:
        if self._yaml_config_adapter is None:
            self._yaml_config_adapter = YamlConfigAdapter()
        return self._yaml_config_adapter

    @property
    def terraform_adapter(self) -> JinjaTerraformAdapter:
        if self._terraform_adapter is None:
            template_dir = self._config.template_dir or str(
                os.path.join(os.path.dirname(__file__), os.pardir, "templates")
            )
            self._terraform_adapter = JinjaTerraformAdapter(template_dir)
        return self._terraform_adapter

    @property
    def bqms_adapter(self) -> BqmsTranslationAdapter:
        if self._bqms_adapter is None:
            self._bqms_adapter = BqmsTranslationAdapter(
                project_id=self._config.gcp_project_id,
                location=self._config.gcp_location,
            )
        return self._bqms_adapter

    @property
    def bigquery_adapter(self) -> BigQueryAdapter:
        if self._bigquery_adapter is None:
            self._bigquery_adapter = BigQueryAdapter(
                project_id=self._config.gcp_project_id,
            )
        return self._bigquery_adapter

    @property
    def gcs_adapter(self) -> GcsStorageAdapter:
        if self._gcs_adapter is None:
            self._gcs_adapter = GcsStorageAdapter(
                project_id=self._config.gcp_project_id,
            )
        return self._gcs_adapter

    @property
    def s3_adapter(self) -> S3StorageAdapter:
        if self._s3_adapter is None:
            self._s3_adapter = S3StorageAdapter(
                region=self._config.aws_region,
            )
        return self._s3_adapter

    @property
    def sts_adapter(self) -> StsTransferAdapter:
        if self._sts_adapter is None:
            self._sts_adapter = StsTransferAdapter(
                project_id=self._config.gcp_project_id,
            )
        return self._sts_adapter

    @property
    def dvt_adapter(self) -> DvtAdapter:
        if self._dvt_adapter is None:
            self._dvt_adapter = DvtAdapter()
        return self._dvt_adapter

    # ── repositories ─────────────────────────────────────────────

    @property
    def table_inventory_repo(self) -> TableInventoryRepository:
        if self._table_inventory_repo is None:
            self._table_inventory_repo = TableInventoryRepository(
                data_dir=self._config.data_dir,
            )
        return self._table_inventory_repo

    @property
    def translation_batch_repo(self) -> TranslationBatchRepository:
        if self._translation_batch_repo is None:
            self._translation_batch_repo = TranslationBatchRepository(
                data_dir=self._config.data_dir,
            )
        return self._translation_batch_repo

    @property
    def wave_repo(self) -> WaveRepository:
        if self._wave_repo is None:
            self._wave_repo = WaveRepository(
                data_dir=self._config.data_dir,
            )
        return self._wave_repo

    @property
    def transfer_job_repo(self) -> TransferJobRepository:
        if self._transfer_job_repo is None:
            self._transfer_job_repo = TransferJobRepository(
                data_dir=self._config.data_dir,
            )
        return self._transfer_job_repo

    @property
    def streaming_job_repo(self) -> StreamingJobRepository:
        if self._streaming_job_repo is None:
            self._streaming_job_repo = StreamingJobRepository(
                data_dir=self._config.data_dir,
            )
        return self._streaming_job_repo

    # ── domain services ──────────────────────────────────────────

    @property
    def tier_classification_service(self) -> TierClassificationService:
        if self._tier_service is None:
            self._tier_service = TierClassificationService()
        return self._tier_service

    @property
    def sql_pattern_matcher(self) -> SqlPatternMatcher:
        if self._pattern_matcher is None:
            patterns = self._load_patterns()
            self._pattern_matcher = SqlPatternMatcher(patterns)
        return self._pattern_matcher

    @property
    def wave_planner_service(self) -> WavePlannerService:
        if self._wave_planner is None:
            self._wave_planner = WavePlannerService()
        return self._wave_planner

    @property
    def egress_cost_calculator(self) -> EgressCostCalculator:
        if self._egress_calculator is None:
            self._egress_calculator = EgressCostCalculator()
        return self._egress_calculator

    @property
    def slot_pricing_calculator(self) -> SlotPricingCalculator:
        if self._slot_calculator is None:
            self._slot_calculator = SlotPricingCalculator()
        return self._slot_calculator

    @property
    def rollback_evaluator(self) -> RollbackEvaluator:
        if self._rollback_evaluator is None:
            self._rollback_evaluator = RollbackEvaluator()
        return self._rollback_evaluator

    @property
    def dependency_scanner(self) -> DependencyScanner:
        if self._dependency_scanner is None:
            self._dependency_scanner = DependencyScanner()
        return self._dependency_scanner

    @property
    def dag_rewriter_service(self) -> DAGRewriterService:
        if self._dag_rewriter is None:
            self._dag_rewriter = DAGRewriterService()
        return self._dag_rewriter

    @property
    def parallel_running_state_machine(self) -> ParallelRunningStateMachine:
        if self._parallel_run_sm is None:
            self._parallel_run_sm = ParallelRunningStateMachine()
        return self._parallel_run_sm

    @property
    def udf_classifier(self) -> UDFClassifier:
        if self._udf_classifier is None:
            self._udf_classifier = UDFClassifier()
        return self._udf_classifier

    @property
    def delta_log_health_service(self) -> DeltaLogHealthService:
        if self._delta_health_service is None:
            self._delta_health_service = DeltaLogHealthService()
        return self._delta_health_service

    @property
    def map_cascade_analyser(self) -> MapCascadeAnalyser:
        if self._map_cascade_analyser is None:
            self._map_cascade_analyser = MapCascadeAnalyser()
        return self._map_cascade_analyser

    # ── use cases: foundation ────────────────────────────────────

    @property
    def generate_scaffold_use_case(self) -> GenerateScaffoldUseCase:
        return GenerateScaffoldUseCase(
            config_port=self.yaml_config_adapter,
            terraform_generator=self.terraform_adapter,
            event_bus=self.event_bus,
        )

    @property
    def classify_tiers_use_case(self) -> ClassifyTiersUseCase:
        return ClassifyTiersUseCase(
            tier_service=self.tier_classification_service,
            table_repo=self.table_inventory_repo,
            event_bus=self.event_bus,
            table_write_repo=self.table_inventory_repo,
        )

    @property
    def configure_pricing_use_case(self) -> ConfigurePricingUseCase:
        return ConfigurePricingUseCase(
            pricing_calculator=self.slot_pricing_calculator,
            terraform_generator=self.terraform_adapter,
        )

    @property
    def bootstrap_dataplex_use_case(self) -> BootstrapDataplexUseCase:
        return BootstrapDataplexUseCase(
            event_bus=self.event_bus,
        )

    @property
    def check_delta_health_use_case(self) -> CheckDeltaHealthUseCase:
        return CheckDeltaHealthUseCase(
            health_service=self.delta_log_health_service,
            storage_port=self.s3_adapter,
            event_bus=self.event_bus,
        )

    # ── use cases: SQL ───────────────────────────────────────────

    @property
    def translate_batch_use_case(self) -> TranslateBatchUseCase:
        return TranslateBatchUseCase(
            pattern_matcher=self.sql_pattern_matcher,
            translation_port=self.bqms_adapter,
            batch_repo=self.translation_batch_repo,
            event_bus=self.event_bus,
        )

    @property
    def analyse_map_cascade_use_case(self) -> AnalyseMapCascadeUseCase:
        return AnalyseMapCascadeUseCase(
            analyser=self.map_cascade_analyser,
            event_bus=self.event_bus,
        )

    @property
    def normalise_case_sensitivity_use_case(self) -> NormaliseCaseSensitivityUseCase:
        return NormaliseCaseSensitivityUseCase()

    @property
    def classify_udfs_use_case(self) -> ClassifyUDFsUseCase:
        return ClassifyUDFsUseCase(
            classifier=self.udf_classifier,
        )

    @property
    def validate_queries_use_case(self) -> ValidateQueriesUseCase:
        return ValidateQueriesUseCase(
            bigquery_port=self.bigquery_adapter,
            event_bus=self.event_bus,
        )

    # ── use cases: transfer ──────────────────────────────────────

    @property
    def plan_delta_compaction_use_case(self) -> PlanDeltaCompactionUseCase:
        return PlanDeltaCompactionUseCase(
            health_service=self.delta_log_health_service,
            storage_port=self.s3_adapter,
            event_bus=self.event_bus,
        )

    @property
    def model_egress_cost_use_case(self) -> ModelEgressCostUseCase:
        return ModelEgressCostUseCase(
            cost_calculator=self.egress_cost_calculator,
            event_bus=self.event_bus,
        )

    @property
    def create_sts_jobs_use_case(self) -> CreateSTSJobsUseCase:
        return CreateSTSJobsUseCase(
            transfer_port=self.sts_adapter,
            event_bus=self.event_bus,
        )

    @property
    def run_dvt_validation_use_case(self) -> RunDVTValidationUseCase:
        return RunDVTValidationUseCase(
            dvt_port=self.dvt_adapter,
            event_bus=self.event_bus,
        )

    @property
    def control_streaming_cutover_use_case(self) -> ControlStreamingCutoverUseCase:
        return ControlStreamingCutoverUseCase(
            event_bus=self.event_bus,
        )

    # ── use cases: wave ──────────────────────────────────────────

    @property
    def plan_waves_use_case(self) -> PlanWavesUseCase:
        return PlanWavesUseCase(
            planner=self.wave_planner_service,
            table_repo=self.table_inventory_repo,
            event_bus=self.event_bus,
        )

    @property
    def evaluate_rollback_use_case(self) -> EvaluateRollbackUseCase:
        return EvaluateRollbackUseCase(
            evaluator=self.rollback_evaluator,
            event_bus=self.event_bus,
        )

    @property
    def enforce_wave_gate_use_case(self) -> EnforceWaveGateUseCase:
        return EnforceWaveGateUseCase(
            event_bus=self.event_bus,
        )

    @property
    def migrate_dashboards_use_case(self) -> MigrateDashboardsUseCase:
        return MigrateDashboardsUseCase()

    @property
    def reconcile_kpis_use_case(self) -> ReconcileKPIsUseCase:
        return ReconcileKPIsUseCase()

    @property
    def control_parallel_run_use_case(self) -> ControlParallelRunUseCase:
        return ControlParallelRunUseCase(
            state_machine=self.parallel_running_state_machine,
            wave_repo=self.wave_repo,
            event_bus=self.event_bus,
        )

    # ── use cases: dependency ────────────────────────────────────

    @property
    def scan_spark_flink_jobs_use_case(self) -> ScanSparkFlinkJobsUseCase:
        return ScanSparkFlinkJobsUseCase(
            scanner=self.dependency_scanner,
            storage_port=self.s3_adapter,
            event_bus=self.event_bus,
        )

    @property
    def rewrite_dags_use_case(self) -> RewriteDAGsUseCase:
        return RewriteDAGsUseCase(
            rewriter=self.dag_rewriter_service,
            event_bus=self.event_bus,
        )

    @property
    def migrate_kafka_topics_use_case(self) -> MigrateKafkaTopicsUseCase:
        return MigrateKafkaTopicsUseCase(
            event_bus=self.event_bus,
        )

    @property
    def rewrite_lambdas_use_case(self) -> RewriteLambdasUseCase:
        return RewriteLambdasUseCase(
            scanner=self.dependency_scanner,
            event_bus=self.event_bus,
        )

    @property
    def map_iam_permissions_use_case(self) -> MapIAMPermissionsUseCase:
        return MapIAMPermissionsUseCase(
            event_bus=self.event_bus,
        )

    # ── queries ──────────────────────────────────────────────────

    @property
    def get_tier_summary_query(self) -> GetTierSummaryQuery:
        return GetTierSummaryQuery(
            table_repo=self.table_inventory_repo,
        )

    # ── private helpers ──────────────────────────────────────────

    def _load_patterns(self) -> list[SqlTranslationPattern]:
        """Load SQL translation patterns from the patterns directory."""
        pattern_dir = self._config.pattern_dir or os.path.join(
            os.path.dirname(__file__), os.pardir, "patterns"
        )
        patterns_file = os.path.join(pattern_dir, "presto_patterns.yaml")

        if not os.path.exists(patterns_file):
            return []

        loader = PatternLoader()
        return loader.load(patterns_file)
