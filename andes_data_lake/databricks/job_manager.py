"""Databricks job management for scheduled medallion pipeline runs."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from andes_data_lake.databricks.models import (
    ClusterConfig,
    DatabricksJobConfig,
    JobRunResult,
    JobRunStatus,
    MedallionJobConfig,
)
from andes_data_lake.databricks.workspace_client import DatabricksWorkspaceClient


class DatabricksJobManager:
    """Defines and manages Databricks workflow jobs for medallion pipelines.

    Supports cron-based scheduling, on-demand triggered runs, job listing
    with optional dataset filtering, and deletion.  All mutations are
    logged to the injected *log_store*.
    """

    def __init__(self, workspace_client: DatabricksWorkspaceClient, log_store: object) -> None:
        self._workspace_client = workspace_client
        self._log_store = log_store
        self._jobs: dict[str, DatabricksJobConfig] = {}

    # ── Job lifecycle ──────────────────────────────────────────────────────

    def create_medallion_job(
        self,
        dataset_id: str,
        schedule: str,
        config: MedallionJobConfig,
    ) -> DatabricksJobConfig:
        """Create a new medallion pipeline job definition.

        Returns the persisted :class:`DatabricksJobConfig`.
        """
        now = datetime.now(timezone.utc)
        job_config = DatabricksJobConfig(
            job_id=str(uuid.uuid4()),
            job_name=f"medallion_{dataset_id}",
            dataset_id=dataset_id,
            schedule_cron=schedule,
            cluster_config=ClusterConfig(),
            medallion_config=config,
            created_at=now,
            updated_at=now,
        )
        self._jobs[job_config.job_id] = job_config
        self._log_store.write(
            {
                "source_component": "DatabricksJobManager",
                "message": f"Created medallion job {job_config.job_id} for dataset {dataset_id}",
                "severity": "INFO",
            }
        )
        return job_config

    def trigger_job(self, job_id: str) -> JobRunResult:
        """Submit an on-demand run for an existing job.

        Logs execution outcome (success or failure) to the log store.
        """
        job_config = self._jobs[job_id]
        try:
            result = self._workspace_client.submit_job(job_config)
            self._log_store.write(
                {
                    "source_component": "DatabricksJobManager",
                    "message": f"Triggered job {job_id}, run_id={result.run_id}",
                    "severity": "INFO",
                }
            )
            return result
        except Exception as exc:
            self._log_store.write(
                {
                    "source_component": "DatabricksJobManager",
                    "message": f"Job {job_id} failed: {exc}",
                    "severity": "ERROR",
                }
            )
            raise

    def get_job_run_status(self, run_id: str) -> JobRunStatus:
        """Return the current status of a job run."""
        return self._workspace_client.get_job_status(run_id)

    def list_jobs(self, dataset_id: str | None = None) -> list[DatabricksJobConfig]:
        """Return all registered jobs, optionally filtered by *dataset_id*."""
        jobs = list(self._jobs.values())
        if dataset_id is not None:
            jobs = [j for j in jobs if j.dataset_id == dataset_id]
        return jobs

    def delete_job(self, job_id: str) -> None:
        """Remove a job from the registry and log the deletion."""
        del self._jobs[job_id]
        self._log_store.write(
            {
                "source_component": "DatabricksJobManager",
                "message": f"Deleted job {job_id}",
                "severity": "INFO",
            }
        )
