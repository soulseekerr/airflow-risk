import { useCallback, useEffect, useRef, useState } from "react";
import { Play, RefreshCw, X } from "lucide-react";

export async function request(path, options = {}) {
  const response = await fetch(path, options);
  const body = await response.json();
  if (!response.ok) {
    const error = new Error(
      body.detail && typeof body.detail === "string"
        ? body.detail
        : "The execution request failed. Check the configuration.",
    );
    error.status = response.status;
    throw error;
  }
  return body;
}
const post = (path, body) =>
  request(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
const params = (values) => new URLSearchParams(values).toString();
export const stateLabel = (state) => (state || "pending").replaceAll("_", " ");
export function Status({ state }) {
  return (
    <span className={`run-status state-${state || "pending"}`}>
      <i />
      {stateLabel(state)}
    </span>
  );
}

export function useExecution() {
  const [workflow, setWorkflow] = useState("");
  const [runId, setRunId] = useState("");
  const [runs, setRuns] = useState([]);
  const [snapshot, setSnapshot] = useState(null);
  const [error, setError] = useState("");
  const [updated, setUpdated] = useState("");
  const [revision, setRevision] = useState(0);
  const refresh = useCallback(() => setRevision((value) => value + 1), []);
  const chooseWorkflow = (id) => {
    setWorkflow(id);
    setRunId("");
    setRuns([]);
    setSnapshot(null);
    setError("");
  };
  useEffect(() => {
    if (!workflow) return;
    const controller = new AbortController();
    request(`/api/runs?${params({ dag_id: workflow })}`, {
      signal: controller.signal,
    })
      .then((body) => setRuns(body.runs))
      .catch((problem) => {
        if (problem.name !== "AbortError") setError(problem.message);
      });
    return () => controller.abort();
  }, [workflow, revision]);
  useEffect(() => {
    setSnapshot(null);
    setUpdated("");
    setError("");
    if (!workflow || !runId) return;
    const controller = new AbortController();
    let timer;
    const poll = async () => {
      try {
        const body = await request(
          `/api/execution?${params({ dag_id: workflow, run_id: runId })}`,
          { signal: controller.signal },
        );
        if (controller.signal.aborted) return;
        setSnapshot({ ...body, workflow, runId });
        setUpdated(new Date().toLocaleTimeString());
        setError("");
      } catch (problem) {
        if (controller.signal.aborted) return;
        setSnapshot(null);
        setError(problem.message);
      }
      if (!controller.signal.aborted) timer = setTimeout(poll, 10000);
    };
    poll();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [workflow, runId, revision]);
  const stages =
    snapshot?.workflow === workflow && snapshot?.runId === runId
      ? snapshot.stages
      : null;
  const selectRun = (id) => {
    setSnapshot(null);
    setRunId(id);
    setError("");
  };
  const launched = (run) => {
    setSnapshot(null);
    setWorkflow(run.dag_id);
    setRunId(run.dag_run_id);
    setRuns([run]);
    refresh();
  };
  return {
    workflow,
    runId,
    runs,
    stages,
    error,
    updated,
    chooseWorkflow,
    selectRun,
    launched,
    refresh,
  };
}

export function decorateNode(node, stages, monitoring) {
  if (!monitoring) return node;
  const dagId = node.type === "workflow" ? node.data.id : node.data.dag_id;
  const stage = stages?.find((item) => item.dag_id === dagId);
  const instances =
    stage?.tasks.filter((task) => task.task_id === node.data.id) || [];
  let state = !stages
    ? "unavailable"
    : !stage
      ? "outside_execution"
      : stage.state || "pending";
  if (node.type === "task" && stage) {
    const order = [
      "failed",
      "upstream_failed",
      "running",
      "up_for_retry",
      "queued",
      "scheduled",
      null,
      "success",
      "skipped",
      "removed",
    ];
    state = instances.length
      ? [...instances].sort(
          (a, b) => order.indexOf(a.state) - order.indexOf(b.state),
        )[0].state || "pending"
      : stage.dag_run_id
        ? "not_in_run"
        : "waiting";
  }
  return {
    ...node,
    data: { ...node.data, runtime: { state, stage, instances } },
  };
}

export function ExecutionBar({ catalog, execution, launch }) {
  const e = execution;
  const overall = e.stages?.some((stage) => stage.state === "failed")
    ? "failed"
    : e.stages?.every((stage) => stage.state === "success")
      ? "success"
      : "running";
  return (
    <div className="execution-panel">
      <div className="execution-controls">
        <span className="eyebrow">EXECUTION</span>
        <select
          aria-label="Execution workflow"
          value={e.workflow}
          onChange={(event) => e.chooseWorkflow(event.target.value)}
        >
          <option value="">Definitions only</option>
          {catalog.dags.map((dag) => (
            <option key={dag.id} value={dag.id}>
              {dag.label}
            </option>
          ))}
        </select>
        {e.workflow && (
          <select
            aria-label="Execution run"
            value={e.runId}
            onChange={(event) => e.selectRun(event.target.value)}
          >
            <option value="">Select a run ({e.runs.length})</option>
            {e.runs.map((run) => (
              <option key={run.dag_run_id} value={run.dag_run_id}>
                {run.dag_run_id} · {run.state}
              </option>
            ))}
          </select>
        )}
        {e.workflow && (
          <button
            className="icon-button"
            onClick={e.refresh}
            aria-label="Refresh execution"
          >
            <RefreshCw size={14} />
          </button>
        )}
        {e.stages && <Status state={overall} />}
        <button className="execute-button" onClick={launch}>
          <Play size={13} />
          Run workflow
        </button>
      </div>
      {e.error && (
        <p className="execution-error" role="alert">
          {e.error}
        </p>
      )}
      {e.runId && !e.error && (
        <p className="execution-note">
          {e.updated
            ? `Updated ${e.updated} · refreshes every 10s`
            : "Loading execution…"}{" "}
          · Current definitions with selected-run status
        </p>
      )}
    </div>
  );
}

function defaults(dagId) {
  const valuation_date = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/London",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date());
  switch (dagId) {
    case "xva_generate_inputs":
      return {
        valuation_date,
        generation: {
          counterparty_count: 2000,
          trade_count: 10000,
          simulation_paths: 20,
          seed: 42,
        },
      };
    case "xva_calculations":
      return { batch_id: "", snapshot_path: "" };
    case "xva_reports":
      return { batch_id: "", publication_path: "" };
    case "xva_counterparty_rerun":
      return { valuation_date, counterparty_code: "", feeds: {} };
    case "xva_feed_ingestion":
      return { valuation_date, feed_type: "trades", input_path: "" };
    case "xva_inputs":
      return { valuation_date };
    default:
      return {};
  }
}

export function LaunchDialog({ catalog, initialDag, close, launched }) {
  const [dagId, setDagId] = useState(initialDag || "xva_generate_inputs");
  const [configuration, setConfiguration] = useState(() =>
    JSON.stringify(defaults(dagId), null, 2),
  );
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  // Preserve the same ID and payload after an uncertain response, including while retrying.
  const pending = useRef(null);
  const [uncertain, setUncertain] = useState(false);
  const submit = async (event) => {
    event.preventDefault();
    setError("");
    try {
      const conf = JSON.parse(configuration);
      if (!conf || typeof conf !== "object" || Array.isArray(conf))
        throw new Error("Configuration must be a JSON object.");
      pending.current ||= {
        dag_id: dagId,
        configuration: conf,
        run_id: `riskflow__${crypto.randomUUID()}`,
      };
      setBusy(true);
      setUncertain(true);
      const run = await post("/api/runs", pending.current);
      launched(run);
      close();
    } catch (problem) {
      setError(problem.message);
      if ([400, 403, 404, 415, 422].includes(problem.status)) {
        pending.current = null;
        setUncertain(false);
      }
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="modal-backdrop">
      <form
        className="launch-dialog"
        onSubmit={submit}
        aria-label="Launch workflow"
      >
        <div className="dialog-heading">
          <h2>Run workflow</h2>
          <button
            type="button"
            className="icon-button"
            onClick={close}
            disabled={busy}
            aria-label="Close launch"
          >
            <X size={18} />
          </button>
        </div>
        <p>
          Start an existing code-defined DAG. Its configured downstream
          workflows are triggered by Airflow.
        </p>
        <label>
          Workflow
          <select
            aria-label="Launch workflow selection"
            value={dagId}
            disabled={busy || uncertain}
            onChange={(event) => {
              setDagId(event.target.value);
              setConfiguration(
                JSON.stringify(defaults(event.target.value), null, 2),
              );
            }}
          >
            {catalog.dags.map((dag) => (
              <option key={dag.id} value={dag.id}>
                {dag.label}
              </option>
            ))}
          </select>
        </label>
        <label>
          Run configuration (JSON)
          <textarea
            aria-label="Run configuration"
            value={configuration}
            disabled={busy || uncertain}
            onChange={(event) => setConfiguration(event.target.value)}
            spellCheck={false}
          />
        </label>
        <p className="small muted">
          Paths are on the Airflow worker. Direct calculation/report runs
          require a previous handoff. Paused DAGs must be enabled in Airflow
          first.
        </p>
        {error && (
          <p className="execution-error" role="alert">
            {error}
          </p>
        )}
        {uncertain && error && (
          <p className="small muted">
            Retry uses the same request ID and configuration to recover an
            accepted run. Close and reopen to prepare a different request; check
            recent runs first if the outcome is uncertain.
          </p>
        )}
        <button className="execute-button" disabled={busy} type="submit">
          <Play size={14} />
          {busy
            ? "Submitting…"
            : uncertain
              ? "Retry same request"
              : "Launch run"}
        </button>
      </form>
    </div>
  );
}

export function TaskExecution({ runtime, taskId, refresh }) {
  const [instanceIndex, setInstanceIndex] = useState(0);
  const instances = runtime.instances;
  const instance = instances[instanceIndex] || instances[0];
  const stage = runtime.stage;
  const [attempt, setAttempt] = useState(instance?.try_number || 1);
  const [logs, setLogs] = useState(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [confirm, setConfirm] = useState(false);
  const active = useRef(true);
  useEffect(() => {
    active.current = true;
    return () => {
      active.current = false;
    };
  }, []);
  const context =
    stage && instance
      ? {
          dag_id: stage.dag_id,
          run_id: stage.dag_run_id,
          task_id: taskId,
          map_index: instance.map_index ?? -1,
        }
      : null;
  const loadLogs = async () => {
    setBusy(true);
    setError("");
    setLogs(null);
    try {
      const body = await request(
        `/api/logs?${params({ ...context, attempt })}`,
      );
      if (active.current)
        setLogs(body.content || "No logs available for this attempt.");
    } catch (problem) {
      if (active.current) setError(problem.message);
    } finally {
      if (active.current) setBusy(false);
    }
  };
  const retry = async () => {
    setBusy(true);
    setError("");
    try {
      await post("/api/retry", context);
      if (active.current) {
        setConfirm(false);
        refresh();
      }
    } catch (problem) {
      if (active.current) setError(problem.message);
    } finally {
      if (active.current) setBusy(false);
    }
  };
  return (
    <div className="inspector-section runtime-details">
      <h4>EXECUTION</h4>
      <Status state={instance ? instance.state || "pending" : runtime.state} />
      {stage?.dag_run_id && (
        <code className="runtime-run">{stage.dag_run_id}</code>
      )}
      {instance && (
        <>
          {instances.length > 1 && (
            <label>
              Mapped instance
              <select
                aria-label="Mapped task instance"
                disabled={busy}
                value={instanceIndex}
                onChange={(event) => {
                  setInstanceIndex(Number(event.target.value));
                  setLogs(null);
                  setConfirm(false);
                  setAttempt(
                    instances[Number(event.target.value)].try_number || 1,
                  );
                }}
              >
                {instances.map((item, index) => (
                  <option key={item.map_index} value={index}>
                    {item.map_index} · {item.state || "pending"}
                  </option>
                ))}
              </select>
            </label>
          )}
          <p className="small muted">
            Attempt {instance.try_number || 0} ·{" "}
            {instance.duration != null
              ? `${Number(instance.duration).toFixed(1)}s`
              : "Duration pending"}
          </p>
          {instance.try_number > 0 && (
            <div className="log-actions">
              <select
                aria-label="Log attempt"
                disabled={busy}
                value={attempt}
                onChange={(event) => {
                  setAttempt(Number(event.target.value));
                  setLogs(null);
                }}
              >
                {Array.from({ length: instance.try_number }, (_, index) => (
                  <option key={index + 1} value={index + 1}>
                    Attempt {index + 1}
                  </option>
                ))}
              </select>
              <button
                className="execute-button"
                disabled={busy}
                onClick={loadLogs}
              >
                Load logs
              </button>
            </div>
          )}
          {instance.state === "failed" && !confirm && (
            <button
              className="retry-button"
              disabled={busy}
              onClick={() => setConfirm(true)}
            >
              Retry failed task
            </button>
          )}
          {confirm && (
            <div className="retry-confirm">
              <p>
                Clear this failed task and its downstream tasks in the same DAG
                run? The original configuration is preserved. Linked DAG runs
                are not cleared.
              </p>
              <button className="retry-button" disabled={busy} onClick={retry}>
                Confirm retry
              </button>
              <button
                className="icon-button"
                disabled={busy}
                onClick={() => setConfirm(false)}
              >
                Cancel
              </button>
            </div>
          )}
        </>
      )}
      {error && (
        <p className="execution-error" role="alert">
          {error}
        </p>
      )}
      {logs !== null && (
        <pre className="task-logs" aria-label="Task logs">
          {logs}
        </pre>
      )}
    </div>
  );
}
