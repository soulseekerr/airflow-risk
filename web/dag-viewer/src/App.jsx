import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Background,
  BackgroundVariant,
  Controls,
  Handle,
  MiniMap,
  Position,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
} from "@xyflow/react";
import {
  ArrowDown,
  ArrowRight,
  ArrowUpRight,
  Braces,
  CalendarDays,
  ChevronRight,
  CircleDot,
  Code2,
  Eye,
  FileCode2,
  Focus,
  GitBranch,
  Layers3,
  LockKeyhole,
  Network,
  PanelRightClose,
  PanelRightOpen,
  RefreshCw,
  Search,
  Waypoints,
  Workflow,
  X,
} from "lucide-react";
import { overviewGraph, taskGraph, taskLabel } from "./graph";
import {
  useExecution,
  ExecutionBar,
  LaunchDialog,
  TaskExecution,
  Status,
  decorateNode,
} from "./execution";

const accentFor = (id) =>
  id.includes("report")
    ? "purple"
    : id.includes("input") || id.includes("feed")
      ? "teal"
      : id.includes("rerun")
        ? "amber"
        : id.includes("demo")
          ? "gray"
          : "blue";

function Pins({ orientation }) {
  return (
    <>
      <Handle
        type="target"
        id="in"
        position={orientation === "TB" ? Position.Top : Position.Left}
        isConnectable={false}
      />
      <Handle
        type="source"
        id="out"
        position={orientation === "TB" ? Position.Bottom : Position.Right}
        isConnectable={false}
      />
    </>
  );
}

function WorkflowNode({ data, selected }) {
  return (
    <div
      className={`graph-card workflow-card ${data.runtime ? `runtime-${data.runtime.state}` : ""} accent-${accentFor(data.id)} ${selected ? "is-selected" : ""}`}
    >
      <Pins orientation="LR" />
      <div className="node-heading">
        <Workflow size={15} />
        <span>AIRFLOW DAG</span>
        {data.runtime ? (
          <Status state={data.runtime.state} />
        ) : (
          <span className="node-count">{data.tasks.length} tasks</span>
        )}
      </div>
      <div className="node-content">
        <h3>{data.label}</h3>
        <code>{data.id}</code>
      </div>
      <div className="node-footer">
        <CalendarDays size={12} />
        <span>
          {data.schedule === "Triggered" ? "Triggered workflow" : data.schedule}
        </span>
        <ArrowUpRight size={13} />
      </div>
    </div>
  );
}

function TaskNode({ data, selected }) {
  const trigger = data.kind === "trigger";
  return (
    <div
      className={`graph-card task-card ${data.runtime ? `runtime-${data.runtime.state}` : ""} ${trigger ? "accent-amber" : "accent-blue"} ${selected ? "is-selected" : ""}`}
    >
      <Pins orientation={data.orientation} />
      <div className="node-heading">
        {trigger ? <GitBranch size={14} /> : <Braces size={14} />}
        <span>
          {trigger
            ? "TRIGGER DAG"
            : data.kind === "python"
              ? "PYTHON TASK"
              : "TASK"}
        </span>
        {data.runtime ? (
          <Status state={data.runtime.state} />
        ) : (
          <span className="definition-dot" />
        )}
      </div>
      <div className="node-content">
        <h3>{data.label}</h3>
      </div>
      <div className="node-footer">
        <span className="pin-label">
          <i />
          {data.upstream.length
            ? `${data.upstream.length} upstream`
            : "Entry task"}
        </span>
        <span className="node-rule">
          {data.retries ? `${data.retries} retries` : "No retries"}
        </span>
      </div>
    </div>
  );
}

const nodeTypes = { workflow: WorkflowNode, task: TaskNode };

function DetailRow({ label, children }) {
  return (
    <div className="detail-row">
      <span>{label}</span>
      <div>{children}</div>
    </div>
  );
}

function Inspector({
  selected,
  dag,
  catalog,
  openDag,
  selectTask,
  close,
  execution,
}) {
  const entity = selected?.data;
  const isTask = selected?.type === "task";
  const context = isTask ? dag : entity;
  const next =
    isTask && entity.target_dag
      ? catalog.dags.find((item) => item.id === entity.target_dag)
      : null;
  return (
    <aside className="inspector" aria-label="Node details">
      <div className="panel-heading">
        <span>INSPECTOR</span>
        <button
          className="icon-button"
          onClick={close}
          aria-label="Hide inspector"
        >
          <PanelRightClose size={16} />
        </button>
      </div>
      {!entity ? (
        <div className="empty-inspector">
          <div className="empty-icon">
            <Waypoints size={28} />
          </div>
          <h2>
            Every connection,
            <br />
            in context.
          </h2>
          <p>
            Select a workflow to inspect its definition, or double-click it to
            explore the tasks inside.
          </p>
          <div className="inspector-tip">
            <Eye size={16} />
            <span>
              Choose an execution workflow and run to overlay live status. DAG
              definitions are maintained in Python.
            </span>
          </div>
        </div>
      ) : (
        <>
          <div className="inspector-title">
            <span
              className={`entity-icon accent-${isTask && entity.kind === "trigger" ? "amber" : accentFor(context?.id || "")}`}
            >
              {isTask ? <Braces size={20} /> : <Workflow size={20} />}
            </span>
            <span className="eyebrow">
              {isTask ? "TASK DEFINITION" : "WORKFLOW DEFINITION"}
            </span>
            <h2>{entity.label}</h2>
            <code>{entity.id}</code>
          </div>
          {entity.runtime &&
            (isTask ? (
              <TaskExecution
                key={`${entity.id}:${entity.runtime.stage?.dag_run_id}:${entity.runtime.instances.map((item) => `${item.map_index}-${item.try_number}-${item.state}`).join(",")}`}
                runtime={entity.runtime}
                taskId={entity.id}
                refresh={execution.refresh}
              />
            ) : (
              <div className="inspector-section">
                <h4>EXECUTION</h4>
                <Status state={entity.runtime.state} />
                {entity.runtime.stage?.dag_run_id && (
                  <code className="runtime-run">
                    {entity.runtime.stage.dag_run_id}
                  </code>
                )}
              </div>
            ))}
          <div className="inspector-section">
            <h4>PROPERTIES</h4>
            <DetailRow label="Type">
              {isTask
                ? entity.kind === "trigger"
                  ? "DAG trigger"
                  : entity.kind === "python"
                    ? "Python task"
                    : entity.operator
                : "Airflow DAG"}
            </DetailRow>
            {isTask ? (
              <>
                <DetailRow label="Retries">{entity.retries}</DetailRow>
                <DetailRow label="Trigger rule">
                  <code>{entity.trigger_rule}</code>
                </DetailRow>
                <DetailRow label="Workflow">{dag.label}</DetailRow>
              </>
            ) : (
              <>
                <DetailRow label="Tasks">{entity.tasks.length}</DetailRow>
                <DetailRow label="Schedule">{entity.schedule}</DetailRow>
                <DetailRow label="Dependencies">
                  {entity.edges.length}
                </DetailRow>
              </>
            )}
          </div>
          {entity.description && (
            <div className="inspector-section">
              <h4>DESCRIPTION</h4>
              <p className="description">{entity.description}</p>
            </div>
          )}
          {isTask && (
            <>
              <div className="inspector-section">
                <h4>
                  INPUT CONNECTIONS <span>{entity.upstream.length}</span>
                </h4>
                {entity.upstream.length ? (
                  entity.upstream.map((id) => (
                    <button
                      key={id}
                      className="connection-item"
                      onClick={() => selectTask(id)}
                    >
                      <i className="tiny-pin" />
                      <span>{taskLabel(id)}</span>
                      <ChevronRight size={13} />
                    </button>
                  ))
                ) : (
                  <p className="muted small">
                    No upstream tasks. This is an entry point.
                  </p>
                )}
              </div>
              <div className="inspector-section">
                <h4>
                  OUTPUT CONNECTIONS{" "}
                  <span>{entity.downstream.length + (next ? 1 : 0)}</span>
                </h4>
                {entity.downstream.map((id) => (
                  <button
                    key={id}
                    className="connection-item"
                    onClick={() => selectTask(id)}
                  >
                    <i className="tiny-pin" />
                    <span>{taskLabel(id)}</span>
                    <ChevronRight size={13} />
                  </button>
                ))}
                {next && (
                  <button
                    className="connection-item trigger-connection"
                    onClick={() => openDag(next.id)}
                  >
                    <GitBranch size={14} />
                    <span>{next.label}</span>
                    <ArrowUpRight size={14} />
                  </button>
                )}
                {!entity.downstream.length && !next && (
                  <p className="muted small">End of this workflow.</p>
                )}
              </div>
            </>
          )}
          <div className="inspector-section">
            <h4>DEFINITION FILE</h4>
            <a
              className="source-link"
              href={`https://github.com/soulseekerr/airflow-risk/blob/main/${context?.file}`}
              target="_blank"
              rel="noreferrer"
            >
              <FileCode2 size={14} />
              <span>{context?.file}</span>
              <ArrowUpRight size={13} />
            </a>
          </div>
          {!isTask && (
            <button
              className="primary-button"
              onClick={() => openDag(entity.id)}
            >
              Explore tasks <ArrowRight size={15} />
            </button>
          )}
        </>
      )}
      <div className="inspector-bottom">
        <LockKeyhole size={13} />
        <span>DAG definitions maintained in code</span>
      </div>
    </aside>
  );
}

function Canvas({
  catalog,
  activeDag,
  orientation,
  selectedId,
  onSelect,
  openDag,
  inspectorOpen,
  execution,
}) {
  const dag = catalog.dags.find((item) => item.id === activeDag);
  const layout = useMemo(
    () => (dag ? taskGraph(dag, orientation) : overviewGraph(catalog)),
    [catalog, dag, orientation],
  );
  const { fitView } = useReactFlow();
  const nodes = useMemo(
    () =>
      layout.nodes.map((node) => ({
        ...decorateNode(node, execution.stages, !!execution.runId),
        selected: node.id === selectedId,
      })),
    [layout.nodes, selectedId, execution.stages, execution.runId],
  );
  useEffect(() => {
    const timeout = setTimeout(
      () => fitView({ padding: 0.1, duration: 280, maxZoom: 1 }),
      60,
    );
    return () => clearTimeout(timeout);
  }, [layout, inspectorOpen, fitView]);
  const edges = useMemo(
    () =>
      layout.edges.map((edge) => ({
        ...edge,
        style: {
          stroke:
            edge.source === selectedId || edge.target === selectedId
              ? "#d8a35f"
              : edge.data?.kind === "trigger"
                ? "#8f87c2"
                : "#768ea9",
          strokeWidth:
            edge.source === selectedId || edge.target === selectedId
              ? 2.2
              : 1.5,
        },
        labelStyle: { fill: "#9892b0", fontSize: 10, fontFamily: "monospace" },
        labelBgStyle: { fill: "#171a21", fillOpacity: 1 },
        labelBgPadding: [7, 4],
      })),
    [layout.edges, selectedId],
  );
  return (
    <>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodeClick={(_, node) => onSelect(node)}
        onNodeDoubleClick={(_, node) =>
          node.type === "workflow" && openDag(node.data.id)
        }
        onPaneClick={() => onSelect(null)}
        nodesDraggable={false}
        nodesConnectable={false}
        edgesReconnectable={false}
        elementsSelectable={true}
        deleteKeyCode={null}
        selectionOnDrag={false}
        panOnDrag={true}
        zoomOnDoubleClick={false}
        minZoom={0.25}
        maxZoom={1.8}
        fitView
        attributionPosition="bottom-left"
        colorMode="dark"
        proOptions={{ hideAttribution: false }}
      >
        <Background
          variant={BackgroundVariant.Dots}
          gap={22}
          size={1.2}
          color="#353a43"
        />
        <Controls showInteractive={false} position="bottom-right" />
        <MiniMap
          style={{ width: 145, height: 88 }}
          position="bottom-left"
          pannable
          zoomable
          nodeColor={(node) =>
            node.data.kind === "trigger"
              ? "#8f7149"
              : node.type === "workflow"
                ? "#4a6572"
                : "#4e6682"
          }
          maskColor="rgba(10, 12, 16, .75)"
        />
      </ReactFlow>
      <button
        className="fit-button"
        onClick={() => fitView({ padding: 0.1, duration: 280 })}
      >
        <Focus size={14} /> Fit graph
      </button>
      <div className="canvas-caption">
        <span className="legend-line" />
        {dag ? "Task dependency" : "Successful DAG trigger"}
        <span className="caption-divider" />
        {nodes.length} {dag ? "tasks" : "workflows"} · {edges.length}{" "}
        connections
      </div>
    </>
  );
}

function Studio({ catalog, reload, loading }) {
  const execution = useExecution();
  const [launchOpen, setLaunchOpen] = useState(false);
  const [activeDag, setActiveDag] = useState(null);
  const [selectedId, setSelectedId] = useState(null);
  const [orientation, setOrientation] = useState("TB");
  const [query, setQuery] = useState("");
  const [inspectorOpen, setInspectorOpen] = useState(true);
  const dag = catalog.dags.find((item) => item.id === activeDag);
  const layout = useMemo(
    () => (dag ? taskGraph(dag, orientation) : overviewGraph(catalog)),
    [catalog, dag, orientation],
  );
  const rawSelected = layout.nodes.find((node) => node.id === selectedId);
  const selected = rawSelected
    ? decorateNode(rawSelected, execution.stages, !!execution.runId)
    : null;
  const stage = execution.stages?.find((item) => item.dag_id === activeDag);
  const missingTasks =
    stage?.tasks.filter(
      (task) => !dag?.tasks.some((item) => item.id === task.task_id),
    ) || [];
  const filtered = catalog.dags.filter((item) =>
    `${item.label} ${item.id}`.toLowerCase().includes(query.toLowerCase()),
  );
  const openDag = useCallback((id) => {
    setActiveDag(id);
    setSelectedId(null);
  }, []);
  const selectNode = useCallback((node) => {
    setSelectedId(node?.id || null);
    if (node) setInspectorOpen(true);
  }, []);
  return (
    <div className="app-shell">
      <header className="app-header">
        <a
          className="brand"
          href="#"
          onClick={(event) => {
            event.preventDefault();
            openDag(null);
          }}
        >
          <span className="brand-mark">
            <Waypoints size={22} />
          </span>
          <strong>
            riskflow<span> / </span>
          </strong>
          <span className="brand-subtitle">DAG EXPLORER</span>
        </a>
        <div className="header-right">
          <span className="environment">
            <i />
            LOCAL DEFINITIONS
          </span>
          <span className="read-mode">
            <LockKeyhole size={13} />
            Code-defined DAGs
          </span>
          <a
            className="github-link"
            href="https://github.com/soulseekerr/airflow-risk"
            target="_blank"
            rel="noreferrer"
          >
            <Code2 size={15} />
            Project
            <ArrowUpRight size={13} />
          </a>
        </div>
      </header>
      <div className="workspace">
        <aside className="sidebar">
          <div className="workspace-label">
            WORKSPACE <span>01</span>
          </div>
          <div className="project-label">
            <span className="project-icon">
              <Layers3 size={19} />
            </span>
            <div>
              <strong>airflow-risk</strong>
              <small>XVA & risk workflows</small>
            </div>
          </div>
          <button
            className={`overview-button ${!activeDag ? "active" : ""}`}
            onClick={() => openDag(null)}
          >
            <Network size={16} />
            Workflow map<span>{catalog.dags.length}</span>
          </button>
          <div className="sidebar-divider" />
          <div className="section-label">
            DAG LIBRARY <span>{catalog.dags.length}</span>
          </div>
          <label className="search-box">
            <Search size={14} />
            <input
              aria-label="Search workflows"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Find a workflow…"
            />
            {query && (
              <button onClick={() => setQuery("")} aria-label="Clear search">
                <X size={12} />
              </button>
            )}
          </label>
          <nav className="dag-list" aria-label="Workflows">
            {filtered.map((item) => (
              <button
                key={item.id}
                className={`dag-item ${activeDag === item.id ? "active" : ""}`}
                onClick={() => openDag(item.id)}
                title={item.id}
              >
                <i className={`dag-color accent-${accentFor(item.id)}`} />
                <span>
                  <strong>{item.label}</strong>
                  <small>{item.id}</small>
                </span>
                <span className="dag-task-count">{item.tasks.length}</span>
              </button>
            ))}
            {!filtered.length && (
              <p className="muted search-empty">No workflows match.</p>
            )}
          </nav>
          <div className="sidebar-bottom">
            <div className="sidebar-note">
              <Eye size={17} />
              <div>
                <strong>Explore and execute.</strong>
                <p>
                  Inspect definitions, follow executions and retry failed tasks.
                  DAG changes stay in code.
                </p>
              </div>
            </div>
            <div className="source-state">
              <span />
              Repository definitions{" "}
              <span className="source-version">v0.1</span>
            </div>
          </div>
        </aside>
        <main className="main-workspace">
          <div className="canvas-toolbar">
            <div className="breadcrumbs">
              <span>Workspace</span>
              <ChevronRight size={13} />
              <strong>{dag ? dag.label : "Workflow map"}</strong>
            </div>
            <div className="toolbar-actions">
              {dag && (
                <div className="segmented" aria-label="Graph direction">
                  <button
                    className={orientation === "TB" ? "active" : ""}
                    onClick={() => setOrientation("TB")}
                    title="Vertical layout"
                    aria-label="Vertical layout"
                  >
                    <ArrowDown size={14} />
                  </button>
                  <button
                    className={orientation === "LR" ? "active" : ""}
                    onClick={() => setOrientation("LR")}
                    title="Horizontal layout"
                    aria-label="Horizontal layout"
                  >
                    <ArrowRight size={14} />
                  </button>
                </div>
              )}
              <button
                className="icon-button"
                aria-label="Reload definitions"
                title="Reload local DAG definitions"
                onClick={reload}
                disabled={loading}
              >
                <RefreshCw size={15} className={loading ? "spin" : ""} />
              </button>
              {!inspectorOpen && (
                <button
                  className="icon-button"
                  aria-label="Show inspector"
                  onClick={() => setInspectorOpen(true)}
                >
                  <PanelRightOpen size={16} />
                </button>
              )}
            </div>
          </div>
          <ExecutionBar
            catalog={catalog}
            execution={execution}
            launch={() => setLaunchOpen(true)}
          />
          <div className="canvas-title-wrap">
            <div className="canvas-title">
              <div>
                <span className="eyebrow">
                  {dag ? "TASK GRAPH" : "PROJECT OVERVIEW"}
                </span>
                <h1>{dag ? dag.label : "The shape of your workflows."}</h1>
                <p>
                  {dag ? (
                    <>
                      <code>{dag.id}</code>
                      <span className="title-divider">/</span>
                      {dag.schedule === "Triggered"
                        ? "Triggered workflow"
                        : dag.schedule}
                    </>
                  ) : (
                    "Your Airflow DAGs, connected. Select a workflow to look inside."
                  )}
                </p>
              </div>
              <span className="definition-badge">
                <CircleDot size={13} />
                {execution.runId ? "Execution view" : "Definition view"}
              </span>
            </div>
          </div>
          {missingTasks.length > 0 && (
            <p className="execution-error">
              {missingTasks.length} task instances from this run are absent from
              the current definition. View the original DAG version in Airflow.
            </p>
          )}
          <div className="graph-canvas" data-testid="graph-canvas">
            <Canvas
              catalog={catalog}
              activeDag={activeDag}
              orientation={orientation}
              selectedId={selectedId}
              onSelect={selectNode}
              openDag={openDag}
              inspectorOpen={inspectorOpen}
              execution={execution}
            />
          </div>
          <footer className="canvas-footer">
            <span>
              <Eye size={12} />
              {execution.runId ? "EXECUTION MONITOR" : "DEFINITION VIEW"}
            </span>
            <span>
              Drag canvas to pan <b>·</b> Scroll to zoom <b>·</b> Select a node
              to inspect
            </span>
            <span className="footer-project">
              {catalog.dags.reduce((sum, item) => sum + item.tasks.length, 0)}{" "}
              tasks in {catalog.dags.length} DAGs
            </span>
          </footer>
        </main>
        {inspectorOpen && (
          <Inspector
            selected={selected}
            dag={dag}
            catalog={catalog}
            openDag={openDag}
            selectTask={setSelectedId}
            execution={execution}
            close={() => setInspectorOpen(false)}
          />
        )}
      </div>
      {launchOpen && (
        <LaunchDialog
          catalog={catalog}
          initialDag={activeDag || execution.workflow}
          close={() => setLaunchOpen(false)}
          launched={execution.launched}
        />
      )}
    </div>
  );
}

export default function App() {
  const [catalog, setCatalog] = useState(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const reload = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const response = await fetch("/api/graph");
      if (!response.ok) {
        const body = await response.json();
        throw new Error(body.detail || "DAG definitions could not be loaded.");
      }
      setCatalog(await response.json());
    } catch (problem) {
      setError(problem.message || "The graph backend could not be reached.");
    } finally {
      setLoading(false);
    }
  }, []);
  useEffect(() => {
    reload();
  }, [reload]);
  if (!catalog)
    return (
      <div className="loading-screen">
        <span className="brand-mark">
          <Waypoints size={28} />
        </span>
        <h1>riskflow</h1>
        <p>{error || "Reading your DAG definitions…"}</p>
        {error && (
          <button className="primary-button" onClick={reload}>
            Try again
          </button>
        )}
      </div>
    );
  return (
    <ReactFlowProvider>
      {error && (
        <div className="error-banner" role="alert">
          {error}
        </div>
      )}
      <Studio catalog={catalog} reload={reload} loading={loading} />
    </ReactFlowProvider>
  );
}
