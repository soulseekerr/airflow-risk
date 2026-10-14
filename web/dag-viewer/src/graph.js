import dagre from "@dagrejs/dagre";

export const taskLabel = (id) => id.replaceAll("_", " ");
export const dagNodeId = (id) => `dag:${id}`;

export function overviewGraph(catalog) {
  const known = new Map(catalog.dags.map((dag) => [dag.id, dag]));
  const main = [
    "xva_generate_inputs",
    "xva_inputs",
    "xva_calculations",
    "xva_reports",
  ].filter((id) => known.has(id));
  const extras = catalog.dags.filter((dag) => !main.includes(dag.id));
  const nodes = main.map((id, index) => ({
    id: dagNodeId(id),
    type: "workflow",
    width: 270,
    height: 154,
    position: { x: index * 355, y: 90 },
    data: { ...known.get(id), orientation: "LR" },
  }));
  const utilityOrder = [
    "xcom_demo",
    "xva_feed_ingestion",
    "xva_counterparty_rerun",
  ];
  const priority = (id) =>
    utilityOrder.includes(id) ? utilityOrder.indexOf(id) : 99;
  extras
    .sort((a, b) => priority(a.id) - priority(b.id) || a.id.localeCompare(b.id))
    .forEach((dag, index) => {
      nodes.push({
        id: dagNodeId(dag.id),
        type: "workflow",
        width: 270,
        height: 154,
        position: {
          x: (index % 4) * 355,
          y: 380 + Math.floor(index / 4) * 260,
        },
        data: { ...dag, orientation: "LR" },
      });
    });
  const edges = catalog.links
    .filter((link) => known.has(link.source) && known.has(link.target))
    .map((link) => ({
      id: `${link.source}:${link.task_id}:${link.target}`,
      source: dagNodeId(link.source),
      target: dagNodeId(link.target),
      sourceHandle: "out",
      targetHandle: "in",
      type: "smoothstep",
      label: "on success",
      data: { kind: "trigger" },
      className: "trigger-edge",
    }));
  return { nodes, edges };
}

export function taskGraph(dag, orientation = "TB") {
  const layout = new dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}));
  layout.setGraph({
    rankdir: orientation,
    nodesep: 85,
    ranksep: orientation === "TB" ? 55 : 100,
    marginx: 30,
    marginy: 20,
  });
  dag.tasks.forEach((task) =>
    layout.setNode(task.id, { width: 300, height: 132 }),
  );
  dag.edges.forEach((edge) => layout.setEdge(edge.source, edge.target));
  dagre.layout(layout);
  const nodes = dag.tasks.map((task) => {
    const position = layout.node(task.id);
    return {
      id: task.id,
      type: "task",
      width: 300,
      height: 132,
      position: { x: position.x - 150, y: position.y - 66 },
      data: { ...task, dag_id: dag.id, orientation },
    };
  });
  const edges = dag.edges.map((edge) => ({
    id: `${edge.source}:${edge.target}`,
    source: edge.source,
    target: edge.target,
    sourceHandle: "out",
    targetHandle: "in",
    type: "smoothstep",
    className: "dependency-edge",
  }));
  return { nodes, edges };
}
