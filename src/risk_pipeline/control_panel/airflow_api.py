"""Small server-side client for the Airflow 3 stable REST API."""

import json
import os
from urllib.parse import quote, urlsplit

import requests


WORKFLOWS = {
    "xva_generate_inputs": "Generate batch",
    "xva_inputs": "Inputs (daily schedule)",
    "xva_calculations": "Calculations",
    "xva_counterparty_rerun": "Counterparty correction",
    "xva_reports": "Reports",
}


class PanelError(Exception):
    """An operational error that can be displayed without exposing connection credentials."""

    def __init__(self, message, status=None):
        """Keep the HTTP status for missing-run detection while excluding raw server responses."""
        super().__init__(message)
        self.status = status


class AirflowAPI:
    """Authenticate on the server and perform bounded, explicitly scoped workflow requests."""

    def __init__(self, url, token=None, username=None, password=None, session=None):
        """Accept server configuration, with a reusable session for one browser session."""
        parsed = urlsplit(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise PanelError("XVA_AIRFLOW_URL must be an HTTP(S) server URL without credentials, query, or fragment.")
        self.url = url.rstrip("/")
        self.token, self.username, self.password = token, username, password
        self.session = session or requests.Session()

    @classmethod
    def from_environment(cls):
        """Read API credentials from the Streamlit server environment, never browser controls."""
        return cls(os.getenv("XVA_AIRFLOW_URL", "http://localhost:8080"), os.getenv("XVA_AIRFLOW_TOKEN"),
                   os.getenv("XVA_AIRFLOW_USERNAME"), os.getenv("XVA_AIRFLOW_PASSWORD"))

    def _send(self, method, path, **kwargs):
        """Make one timed request without following redirects that could disclose a bearer token."""
        try:
            return self.session.request(method, self.url + path, timeout=(5, 30), allow_redirects=False, **kwargs)
        except requests.RequestException:
            raise PanelError("Airflow could not be reached. Check the API URL and server. If a launch timed out, submit the same form again to recover its run.") from None

    def _login(self):
        """Obtain a short-lived JWT using Airflow's configured auth manager."""
        if not self.username or not self.password:
            raise PanelError("Configure XVA_AIRFLOW_TOKEN or XVA_AIRFLOW_USERNAME and XVA_AIRFLOW_PASSWORD on the Streamlit server.")
        response = self._send("POST", "/auth/token", json={"username": self.username, "password": self.password})
        if not 200 <= response.status_code < 300:
            raise PanelError("Airflow authentication failed. Check the server-side credentials and auth manager.", response.status_code)
        try:
            self.token = response.json()["access_token"]
            if not isinstance(self.token, str) or not self.token:
                raise ValueError
        except (ValueError, KeyError, TypeError):
            raise PanelError("Airflow returned an invalid authentication response.") from None

    def request(self, method, path, **kwargs):
        """Return JSON or a sanitized error; renew password-based sessions once after expiry."""
        if not self.token:
            self._login()
        headers = {"Authorization": f"Bearer {self.token}", "Accept": "application/json"}
        response = self._send(method, "/api/v2" + path, headers=headers, **kwargs)
        if response.status_code == 401 and self.username and self.password:
            self._login()
            headers["Authorization"] = f"Bearer {self.token}"
            response = self._send(method, "/api/v2" + path, headers=headers, **kwargs)
        if not 200 <= response.status_code < 300:
            messages = {401: "Airflow token expired or is invalid. Refresh the server-side credentials.",
                        403: "The Airflow account does not have permission for this action.",
                        404: "Airflow could not find this workflow, run, task, or handoff.",
                        409: "This Airflow run already exists or conflicts with another request.",
                        422: "Airflow rejected the request. Check the configuration and Airflow version."}
            raise PanelError(messages.get(response.status_code, f"Airflow request failed (HTTP {response.status_code})."), response.status_code)
        try:
            return response.json()
        except ValueError:
            raise PanelError("Airflow returned an invalid JSON response.") from None

    def _dag_path(self, dag_id):
        """Limit write-capable controls to this project's known XVA workflows."""
        if dag_id not in WORKFLOWS:
            raise PanelError("Unknown XVA workflow.")
        return "/dags/" + quote(dag_id, safe="")

    def run_path(self, dag_id, run_id):
        """Escape run identifiers, including manual run timestamps, as path components."""
        return self._dag_path(dag_id) + "/dagRuns/" + quote(run_id, safe="")

    def dag(self, dag_id):
        """Read workflow availability and pause status."""
        return self.request("GET", self._dag_path(dag_id))

    def unpause(self, dag_id):
        """Enable one explicitly selected workflow; enabling inputs also enables its daily schedule."""
        return self.request("PATCH", self._dag_path(dag_id), json={"is_paused": False})

    def runs(self, dag_id, limit=30):
        """Return the most recent runs for workflow selection; exact child lookups bypass this window."""
        return self.request("GET", self._dag_path(dag_id) + "/dagRuns",
                            params={"limit": limit, "order_by": "-run_after"})["dag_runs"]

    def run(self, dag_id, run_id):
        """Read a particular run without inferring its identity from a date or timestamp."""
        return self.request("GET", self.run_path(dag_id, run_id))

    def trigger(self, dag_id, configuration, run_id):
        """Start a workflow with a stable request ID and recover an already accepted identical request."""
        if self.dag(dag_id).get("is_paused"):
            raise PanelError(f"{WORKFLOWS[dag_id]} is paused. Enable it in Workflow controls first.")
        try:
            return self.request("POST", self._dag_path(dag_id) + "/dagRuns",
                                json={"dag_run_id": run_id, "logical_date": None, "conf": configuration})
        except PanelError as error:
            if error.status != 409:
                raise
            existing = self.run(dag_id, run_id)
            if existing.get("conf") != configuration:
                raise PanelError("The request ID is already associated with a different configuration.") from None
            return existing

    def tasks(self, dag_id, run_id):
        """Read every task instance, including pagination for a mapped workflow."""
        path, rows, offset = self.run_path(dag_id, run_id) + "/taskInstances", [], 0
        while True:
            page = self.request("GET", path, params={"limit": 100, "offset": offset})
            items = page["task_instances"]
            rows.extend(items)
            offset += len(items)
            if not items or offset >= page["total_entries"]:
                return rows

    def xcom(self, dag_id, run_id, task_id):
        """Read a plain TaskFlow handoff, decoding the API's native JSON representation."""
        path = self.run_path(dag_id, run_id) + "/taskInstances/" + quote(task_id, safe="") + "/xcomEntries/return_value"
        value = self.request("GET", path)["value"]
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except ValueError:
                raise PanelError("The workflow handoff is not a JSON object.") from None
        if not isinstance(value, dict):
            raise PanelError("The workflow handoff is not a JSON object.")
        return value

    def logs(self, dag_id, run_id, task_id, attempt, map_index=-1):
        """Read the available log pages for one task attempt using Airflow 3's structured log format."""
        path = self.run_path(dag_id, run_id) + "/taskInstances/" + quote(task_id, safe="") + f"/logs/{attempt}"
        params, lines, seen = {"map_index": map_index}, [], set()
        for _ in range(20):
            page = self.request("GET", path, params=params)
            for entry in page.get("content", []):
                lines.append(entry if isinstance(entry, str) else json.dumps(entry, ensure_ascii=False))
            token = page.get("continuation_token")
            if not token or token in seen:
                break
            seen.add(token)
            params = {**params, "token": token}
        else:
            lines.append("Log display limited to 20 pages. Open Airflow for the complete log.")
        return "\n".join(lines)

    def retry_task(self, dag_id, run_id, task_id, map_index=-1):
        """Clear a failed task and its downstream tasks in the original run, preserving its configuration."""
        task = next((row for row in self.tasks(dag_id, run_id)
                     if row["task_id"] == task_id and row.get("map_index", -1) == map_index), None)
        if not task or task.get("state") != "failed":
            raise PanelError("Only a currently failed task can be retried here. Refresh the execution first.")
        task_key = [task_id, map_index] if map_index >= 0 else task_id
        return self.request("POST", self._dag_path(dag_id) + "/clearTaskInstances",
                            json={"dag_run_id": run_id, "task_ids": [task_key], "dry_run": False,
                                  "only_failed": False, "include_downstream": True,
                                  "include_upstream": False, "include_past": False, "include_future": False,
                                  "reset_dag_runs": True, "run_on_latest_version": False,
                                  "prevent_running_task": True})
