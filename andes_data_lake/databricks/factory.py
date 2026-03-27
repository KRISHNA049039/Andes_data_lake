"""Composition root for the Databricks Medallion package.

Wires all dependencies together and returns a fully configured
:class:`MedallionOrchestrator` ready for use.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from andes_data_lake.databricks.bronze_processor import BronzeLayerProcessor
from andes_data_lake.databricks.delta_table_manager import DeltaTableManager
from andes_data_lake.databricks.gold_processor import GoldLayerProcessor
from andes_data_lake.databricks.job_manager import DatabricksJobManager
from andes_data_lake.databricks.medallion_orchestrator import MedallionOrchestrator
from andes_data_lake.databricks.silver_processor import SilverLayerProcessor
from andes_data_lake.databricks.workspace_client import DatabricksWorkspaceClient


@dataclass
class MedallionConfig:
    """Configuration needed to bootstrap the Medallion stack."""

    databricks_host: str
    databricks_token: str


def create_medallion_orchestrator(
    config: MedallionConfig,
    validation_pipeline: Any,
    catalog_service: Any,
    rbac_module: Any,
    log_store: Any,
    dead_letter_queue: Any,
    idempotency_store: Any,
) -> MedallionOrchestrator:
    """Wire all Medallion components and return a ready orchestrator.

    Existing Andes components are injected from the caller — this factory
    only creates the Databricks-specific objects.
    """
    workspace_client = DatabricksWorkspaceClient(
        host=config.databricks_host,
        token=config.databricks_token,
    )
    delta_mgr = DeltaTableManager(
        workspace_client=workspace_client,
        log_store=log_store,
    )
    bronze = BronzeLayerProcessor(
        delta_table_manager=delta_mgr,
        log_store=log_store,
    )
    silver = SilverLayerProcessor(
        delta_table_manager=delta_mgr,
        validation_pipeline=validation_pipeline,
        log_store=log_store,
        dead_letter_queue=dead_letter_queue,
    )
    gold = GoldLayerProcessor(
        delta_table_manager=delta_mgr,
        log_store=log_store,
    )
    return MedallionOrchestrator(
        bronze_processor=bronze,
        silver_processor=silver,
        gold_processor=gold,
        catalog_service=catalog_service,
        rbac=rbac_module,
        log_store=log_store,
        idempotency_store=idempotency_store,
    )
