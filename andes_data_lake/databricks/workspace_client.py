"""Thin wrapper around the Databricks SDK for workspace operations."""

from __future__ import annotations

from andes_data_lake.databricks.errors import DatabricksConnectionError
from andes_data_lake.databricks.models import (
    ClusterConfig,
    DatabricksJobConfig,
    JobRunResult,
    JobRunStatus,
)

try:
    from databricks.sdk import WorkspaceClient as _SDKWorkspaceClient

    _HAS_SDK = True
except ImportError:  # pragma: no cover
    _HAS_SDK = False


class DatabricksWorkspaceClient:
    """Client for interacting with a Databricks workspace.

    Uses the official Databricks SDK when available; otherwise falls back to
    a lightweight ``requests``-based implementation.  All connection-level
    errors are wrapped in :class:`DatabricksConnectionError`.
    """

    def __init__(self, host: str, token: str) -> None:
        self.host = host.rstrip("/")
        self.token = token

        if _HAS_SDK:
            try:
                self._client = _SDKWorkspaceClient(host=self.host, token=self.token)
            except Exception as exc:
                raise DatabricksConnectionError(
                    f"Failed to initialise Databricks SDK client: {exc}",
                    details={"host": self.host},
                ) from exc
        else:
            self._client = None

    # ── SQL execution ──────────────────────────────────────────────────────

    def execute_sql(self, sql: str, warehouse_id: str) -> list[dict]:
        """Submit a SQL statement to a Databricks SQL Warehouse.

        Returns the result set as a list of dicts (one dict per row).
        """
        try:
            if self._client is not None:
                return self._execute_sql_sdk(sql, warehouse_id)
            return self._execute_sql_rest(sql, warehouse_id)
        except DatabricksConnectionError:
            raise
        except Exception as exc:
            raise DatabricksConnectionError(
                f"SQL execution failed: {exc}",
                details={"warehouse_id": warehouse_id},
            ) from exc

    # ── Job management ─────────────────────────────────────────────────────

    def submit_job(self, job_config: DatabricksJobConfig) -> JobRunResult:
        """Submit a one-time job run and return a :class:`JobRunResult`."""
        try:
            if self._client is not None:
                return self._submit_job_sdk(job_config)
            return self._submit_job_rest(job_config)
        except DatabricksConnectionError:
            raise
        except Exception as exc:
            raise DatabricksConnectionError(
                f"Job submission failed: {exc}",
                details={"job_id": job_config.job_id},
            ) from exc

    def get_job_status(self, run_id: str) -> JobRunStatus:
        """Return the current status of a job run."""
        try:
            if self._client is not None:
                return self._get_job_status_sdk(run_id)
            return self._get_job_status_rest(run_id)
        except DatabricksConnectionError:
            raise
        except Exception as exc:
            raise DatabricksConnectionError(
                f"Failed to get job status: {exc}",
                details={"run_id": run_id},
            ) from exc

    # ── Cluster management ─────────────────────────────────────────────────

    def create_cluster(self, config: ClusterConfig) -> str:
        """Create a Databricks cluster and return its ``cluster_id``."""
        try:
            if self._client is not None:
                return self._create_cluster_sdk(config)
            return self._create_cluster_rest(config)
        except DatabricksConnectionError:
            raise
        except Exception as exc:
            raise DatabricksConnectionError(
                f"Cluster creation failed: {exc}",
            ) from exc

    def terminate_cluster(self, cluster_id: str) -> None:
        """Terminate a running Databricks cluster."""
        try:
            if self._client is not None:
                return self._terminate_cluster_sdk(cluster_id)
            return self._terminate_cluster_rest(cluster_id)
        except DatabricksConnectionError:
            raise
        except Exception as exc:
            raise DatabricksConnectionError(
                f"Cluster termination failed: {exc}",
                details={"cluster_id": cluster_id},
            ) from exc

    # ── SDK-backed implementations ─────────────────────────────────────────

    def _execute_sql_sdk(self, sql: str, warehouse_id: str) -> list[dict]:
        """Execute SQL via the Databricks SDK ``statement_execution`` API."""
        response = self._client.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=warehouse_id,
        )
        columns = [col.name for col in (response.manifest.schema.columns or [])]
        rows: list[dict] = []
        if response.result and response.result.data_array:
            for row_values in response.result.data_array:
                rows.append(dict(zip(columns, row_values)))
        return rows

    def _submit_job_sdk(self, job_config: DatabricksJobConfig) -> JobRunResult:
        """Submit a job run via the Databricks SDK."""
        run = self._client.jobs.run_now(job_id=int(job_config.job_id))
        return JobRunResult(
            run_id=str(run.run_id),
            status=JobRunStatus.PENDING,
        )

    def _get_job_status_sdk(self, run_id: str) -> JobRunStatus:
        """Get job run status via the Databricks SDK."""
        run = self._client.jobs.get_run(run_id=int(run_id))
        state = run.state
        if state and state.life_cycle_state:
            lcs = state.life_cycle_state.value
            if lcs in ("PENDING", "BLOCKED", "QUEUED"):
                return JobRunStatus.PENDING
            if lcs == "RUNNING":
                return JobRunStatus.RUNNING
            if lcs == "TERMINATED":
                result_state = (state.result_state.value if state.result_state else "")
                if result_state == "SUCCESS":
                    return JobRunStatus.SUCCEEDED
                if result_state == "CANCELED":
                    return JobRunStatus.CANCELLED
                return JobRunStatus.FAILED
            if lcs in ("TERMINATING",):
                return JobRunStatus.RUNNING
        return JobRunStatus.PENDING

    def _create_cluster_sdk(self, config: ClusterConfig) -> str:
        """Create a cluster via the Databricks SDK."""
        kwargs: dict = {
            "cluster_name": f"medallion-{config.spark_version}",
            "spark_version": config.spark_version,
            "node_type_id": config.node_type_id,
            "num_workers": config.num_workers,
        }
        if config.autoscale_min is not None and config.autoscale_max is not None:
            kwargs.pop("num_workers", None)
            kwargs["autoscale"] = {
                "min_workers": config.autoscale_min,
                "max_workers": config.autoscale_max,
            }
        response = self._client.clusters.create(**kwargs)
        return response.cluster_id

    def _terminate_cluster_sdk(self, cluster_id: str) -> None:
        """Terminate a cluster via the Databricks SDK."""
        self._client.clusters.delete(cluster_id=cluster_id)

    # ── REST fallback implementations ──────────────────────────────────────

    def _api_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _execute_sql_rest(self, sql: str, warehouse_id: str) -> list[dict]:
        """Execute SQL via the REST API (fallback when SDK is unavailable)."""
        import requests

        url = f"{self.host}/api/2.0/sql/statements"
        payload = {
            "statement": sql,
            "warehouse_id": warehouse_id,
            "wait_timeout": "30s",
        }
        resp = requests.post(url, json=payload, headers=self._api_headers(), timeout=60)
        resp.raise_for_status()
        data = resp.json()

        columns = [
            col["name"]
            for col in data.get("manifest", {}).get("schema", {}).get("columns", [])
        ]
        rows: list[dict] = []
        for row_values in data.get("result", {}).get("data_array", []):
            rows.append(dict(zip(columns, row_values)))
        return rows

    def _submit_job_rest(self, job_config: DatabricksJobConfig) -> JobRunResult:
        """Submit a job run via the REST API."""
        import requests

        url = f"{self.host}/api/2.1/jobs/run-now"
        payload = {"job_id": int(job_config.job_id)}
        resp = requests.post(url, json=payload, headers=self._api_headers(), timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return JobRunResult(
            run_id=str(data["run_id"]),
            status=JobRunStatus.PENDING,
        )

    def _get_job_status_rest(self, run_id: str) -> JobRunStatus:
        """Get job run status via the REST API."""
        import requests

        url = f"{self.host}/api/2.1/jobs/runs/get"
        resp = requests.get(
            url,
            params={"run_id": run_id},
            headers=self._api_headers(),
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        state = data.get("state", {})
        lcs = state.get("life_cycle_state", "")
        if lcs in ("PENDING", "BLOCKED", "QUEUED"):
            return JobRunStatus.PENDING
        if lcs == "RUNNING":
            return JobRunStatus.RUNNING
        if lcs == "TERMINATED":
            result_state = state.get("result_state", "")
            if result_state == "SUCCESS":
                return JobRunStatus.SUCCEEDED
            if result_state == "CANCELED":
                return JobRunStatus.CANCELLED
            return JobRunStatus.FAILED
        return JobRunStatus.PENDING

    def _create_cluster_rest(self, config: ClusterConfig) -> str:
        """Create a cluster via the REST API."""
        import requests

        url = f"{self.host}/api/2.0/clusters/create"
        payload: dict = {
            "cluster_name": f"medallion-{config.spark_version}",
            "spark_version": config.spark_version,
            "node_type_id": config.node_type_id,
            "num_workers": config.num_workers,
        }
        if config.autoscale_min is not None and config.autoscale_max is not None:
            payload.pop("num_workers", None)
            payload["autoscale"] = {
                "min_workers": config.autoscale_min,
                "max_workers": config.autoscale_max,
            }
        resp = requests.post(url, json=payload, headers=self._api_headers(), timeout=60)
        resp.raise_for_status()
        return resp.json()["cluster_id"]

    def _terminate_cluster_rest(self, cluster_id: str) -> None:
        """Terminate a cluster via the REST API."""
        import requests

        url = f"{self.host}/api/2.0/clusters/delete"
        payload = {"cluster_id": cluster_id}
        resp = requests.post(url, json=payload, headers=self._api_headers(), timeout=60)
        resp.raise_for_status()
