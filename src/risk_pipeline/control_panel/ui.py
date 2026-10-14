"""Streamlit pages for launching and operating the XVA workflows."""

from datetime import date, datetime
from zoneinfo import ZoneInfo
import hashlib
import json
import uuid

import pandas as pd
import streamlit as st

from risk_pipeline.control_panel.airflow_api import PanelError, WORKFLOWS
from risk_pipeline.control_panel.service import create_services, execution_state
from risk_pipeline.xva.generation import current_valuation_date


PAGES = ("Overview", "Launch batch", "Counterparty results", "Correct and rerun", "Executions", "Reports")
FEEDS = ("counterparties", "credit_curves", "master_agreements", "csas", "trades")


def london_time(value):
    """Display aware timestamps in London time while preserving date-only and other values."""
    parsed = value
    if isinstance(value, str) and "T" in value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
    if isinstance(parsed, datetime) and parsed.tzinfo is not None:
        return parsed.astimezone(ZoneInfo("Europe/London")).isoformat()
    return value


def table(rows):
    """Render financial rows with explicit London timestamps and no synthetic row index."""
    return st.dataframe([{key: london_time(value) for key, value in row.items()} for row in rows],
                        hide_index=True, width="stretch")


def services():
    """Keep API credentials and HTTP sessions scoped to this browser's server-side session."""
    if "services" not in st.session_state:
        st.session_state.services = create_services()
    return st.session_state.services


def choose_date(backend, key):
    """Choose a published valuation date or explain how to create the first one."""
    rows = backend.results.dates()
    if not rows:
        st.info("No published valuations yet. Launch a batch to create the first one.")
        return None
    return st.selectbox("Valuation date", [row["valuation_date"] for row in rows], key=key)


def submit(backend, dag_id, configuration):
    """Reuse the same run ID after uncertain network failures, then retain the accepted execution for monitoring."""
    fingerprint = hashlib.sha256(json.dumps([dag_id, configuration], sort_keys=True).encode()).hexdigest()
    if st.session_state.get("pending_fingerprint") != fingerprint:
        st.session_state.pending_fingerprint = fingerprint
        st.session_state.pending_run_id = "xva_ui__" + uuid.uuid4().hex
    run_id = st.session_state.pending_run_id
    try:
        with st.spinner("Submitting workflow…"):
            backend.launch(dag_id, configuration, run_id)
    except PanelError as error:
        st.error(str(error))
        return
    st.session_state.selected_execution = (dag_id, run_id)
    st.session_state.pop("pending_fingerprint", None)
    st.session_state.pop("pending_run_id", None)
    st.success("Workflow submitted. Open Executions to follow its progress.")


def overview(backend):
    """Present one row per published valuation date with calculated and adjusted financial totals."""
    st.subheader("Published valuations")
    rows = backend.results.dates()
    if not rows:
        st.info("No published valuations yet. Use Launch batch to generate inputs and run the full workflow.")
        return
    latest = rows[0]
    columns = st.columns(4)
    for column, label, value in zip(columns, ("Latest valuation", "Counterparties", "Calculated CVA · EUR", "Adjusted CVA · EUR"),
                                     (str(latest["valuation_date"]), f"{latest['counterparty_count']:,}",
                                      f"{latest['cva_calculated']:,.2f}", f"{latest['cva_adjusted']:,.2f}")):
        column.metric(label, value)
    table([{key: value for key, value in row.items() if key != "batch_id"} for row in rows])
    st.caption("Amounts are in EUR. Adjusted CVA includes manual adjustments; calculated CVA is the model result.")


def launch_batch(backend):
    """Collect synthetic portfolio controls and launch the complete generation-to-report chain."""
    st.subheader("Launch a batch")
    st.write("Generate random source data, validate it, calculate PV/exposure/CVA, and produce reports.")
    with st.form("launch_batch"):
        valuation = st.date_input("Valuation date", value=date.fromisoformat(current_valuation_date()))
        left, right = st.columns(2)
        counterparties = left.number_input("Counterparties", min_value=1, max_value=9_999_999, value=2000, step=100)
        trades = right.number_input("Trades", min_value=1, value=10000, step=1000)
        with st.expander("Generation and calculation settings"):
            seed = st.number_input("Random seed", min_value=0, value=42, step=1)
            paths = st.number_input("Simulation paths", min_value=1, value=20, step=1)
            horizon = st.number_input("Horizon in days", min_value=1, value=365, step=1)
            noise = st.slider("Input noise", min_value=0.0, max_value=0.5, value=0.1, step=0.01)
            collateral = st.slider("Fraction of counterparties with collateral", min_value=0.0, max_value=1.0, value=0.7, step=0.05)
        send = st.form_submit_button("Launch batch", type="primary")
    if send:
        configuration = {"valuation_date": valuation.isoformat(), "generation": {
            "counterparty_count": int(counterparties), "trade_count": int(trades), "seed": int(seed),
            "simulation_paths": int(paths), "horizon_days": int(horizon), "noise": noise,
            "collateral_fraction": collateral}}
        submit(backend, "xva_generate_inputs", configuration)


def counterparty_results(backend):
    """Filter published counterparty results and inspect trade PVs and aggregated exposure profiles."""
    st.subheader("Counterparty results")
    valuation = choose_date(backend, "results_date")
    if valuation is None:
        return
    rows = backend.results.counterparties(valuation)
    search = st.text_input("Find a counterparty", placeholder="Name or seven-character code")
    filtered = [row for row in rows if search.casefold() in (row["counterparty_code"] + " " + (row["counterparty_name"] or "")).casefold()]
    if not filtered:
        st.info("No matching counterparties.")
        return
    table(filtered)
    by_code = {row["counterparty_code"]: row for row in filtered}
    code = st.selectbox("Inspect counterparty", list(by_code), format_func=lambda item: f"{item} · {by_code[item]['counterparty_name']}")
    trades, exposures = backend.results.details(valuation, code)
    left, right = st.columns(2)
    left.metric("Calculated CVA · EUR", f"{by_code[code]['value_calc']:,.2f}")
    right.metric("Adjusted CVA · EUR", f"{by_code[code]['value_adj']:,.2f}")
    st.write("Trade PVs · EUR")
    table(trades)
    if exposures:
        frame = pd.DataFrame(exposures)
        profile = frame.groupby("exposure_date")["expected_positive_exposure"].sum()
        st.write("Expected positive exposure · EUR")
        st.line_chart(profile)
        st.caption("Exposure is calculated within each MA, then summed across this counterparty's MAs.")


def correct_counterparty(backend):
    """Submit exact revised feed references for an existing counterparty and valuation date."""
    st.subheader("Correct and rerun a counterparty")
    valuation = choose_date(backend, "correction_date")
    if valuation is None:
        return
    rows = backend.results.counterparties(valuation)
    if not rows:
        st.info("No counterparties are available for this valuation date.")
        return
    names = {row["counterparty_code"]: row["counterparty_name"] for row in rows}
    code = st.selectbox("Counterparty", sorted(names), format_func=lambda item: f"{item} · {names[item]}")
    st.write("Supply complete corrected source versions. Omitted feeds reuse their published versions. The rerun replaces this counterparty's results and resets its adjusted CVA to the calculated value.")
    with st.expander("Available feed versions"):
        feed_key = f"available_feeds_{valuation}"
        if st.button("Load feed versions"):
            st.session_state[feed_key] = backend.results.feeds(valuation)
        if feed_key in st.session_state:
            table(st.session_state[feed_key])
    # These selectors sit outside the form so choosing a feed reveals its input immediately.
    kinds = st.multiselect("Corrected feeds", FEEDS, default=["trades"])
    modes = {kind: st.radio(f"{kind.replace('_', ' ').title()} source", ("Feed ID", "File path"), horizontal=True, key=f"mode_{kind}") for kind in kinds}
    with st.form("counterparty_correction"):
        values = {kind: st.text_input(f"{kind.replace('_', ' ').title()} {modes[kind].lower()}", key=f"feed_{kind}_{modes[kind]}") for kind in kinds}
        st.caption("File paths must be readable by Airflow workers. Shared market, simulation, or reporting-grid changes require a full batch rerun.")
        send = st.form_submit_button("Rerun counterparty", type="primary")
    if send:
        if not kinds or any(not value.strip() for value in values.values()):
            st.error("Supply a source for every selected feed.")
        else:
            feeds = {kind: {"feed_id": value.strip()} if modes[kind] == "Feed ID" else value.strip() for kind, value in values.items()}
            submit(backend, "xva_counterparty_rerun", {"valuation_date": str(valuation), "counterparty_code": code, "feeds": feeds})
    with st.expander("Correction history"):
        table(backend.results.corrections(valuation))


@st.fragment(run_every="10s")
def execution_monitor(backend, dag_id, run_id):
    """Refresh one linked business execution and expose narrowly scoped task diagnostics and retries."""
    try:
        stages = backend.execution(dag_id, run_id)
        st.metric("Execution status", execution_state(stages))
        table([{"Stage": WORKFLOWS[stage["dag_id"]], "Status": stage.get("state", "waiting"),
                       "Started": stage.get("start_date"), "Finished": stage.get("end_date")} for stage in stages])
        for stage in stages:
            tasks = stage["tasks"]
            if not tasks:
                continue
            with st.expander(f"{WORKFLOWS[stage['dag_id']]} · task details"):
                st.caption(f"Run: {stage['dag_run_id']}")
                table([{key: task.get(key) for key in ("task_id", "state", "try_number", "map_index", "start_date", "end_date")} for task in tasks])
                choices = {(task["task_id"], task.get("map_index", -1)): task for task in tasks}
                selected = st.selectbox("Task", list(choices), format_func=lambda item: item[0] + (f" [{item[1]}]" if item[1] >= 0 else ""), key=f"task_{stage['dag_id']}_{run_id}")
                task = choices[selected]
                attempt = st.number_input("Log attempt", min_value=1, max_value=max(1, task.get("try_number") or 1),
                                          value=max(1, task.get("try_number") or 1), key=f"attempt_{stage['dag_id']}_{run_id}_{selected}")
                log_key = f"log_output_{stage['dag_id']}_{stage['dag_run_id']}_{selected}_{attempt}"
                if st.button("Read task logs", key=f"logs_{stage['dag_id']}_{run_id}"):
                    st.session_state[log_key] = backend.api.logs(stage["dag_id"], stage["dag_run_id"], selected[0], int(attempt), selected[1])
                if log_key in st.session_state:
                    st.code(st.session_state[log_key] or "No log entries available yet.", language=None)
                if task.get("state") == "failed":
                    st.caption("Retry clears this task and its downstream tasks in the same run, preserving the original inputs and configuration.")
                    if st.button("Retry task and downstream", key=f"retry_{stage['dag_id']}_{run_id}"):
                        backend.api.retry_task(stage["dag_id"], stage["dag_run_id"], selected[0], selected[1])
                        st.success("Retry requested for the original run. Status refreshes every 10 seconds.")
    except PanelError as error:
        st.error(str(error))


def executions(backend):
    """Select recent root workflows or a manually entered exact run ID and monitor their complete chain."""
    st.subheader("Executions")
    selected = st.session_state.get("selected_execution")
    roots = ("xva_generate_inputs", "xva_inputs", "xva_counterparty_rerun", "xva_calculations", "xva_reports")
    dag_id = st.selectbox("Workflow", roots, index=roots.index(selected[0]) if selected else 0, format_func=lambda item: WORKFLOWS[item])
    runs = backend.api.runs(dag_id)
    options = [run["dag_run_id"] for run in runs]
    if selected and selected[0] == dag_id and selected[1] not in options:
        options.insert(0, selected[1])
    exact = st.text_input("Find an older run by its exact ID", placeholder="Optional")
    if exact.strip():
        run_id = exact.strip()
    elif options:
        labels = {run["dag_run_id"]: f"{(run.get('conf') or {}).get('valuation_date', 'Date in handoff')} · {run.get('state')} · {london_time(run.get('run_after', ''))} · {run['dag_run_id']}" for run in runs}
        run_id = st.selectbox("Execution", options, index=options.index(selected[1]) if selected and selected[0] == dag_id and selected[1] in options else 0,
                              format_func=lambda item: labels.get(item, item))
    else:
        st.info("No runs found for this workflow yet.")
        return
    st.caption("The complete chain refreshes every 10 seconds, including downstream runs outside the recent-run list.")
    execution_monitor(backend, dag_id, run_id)


def reports(backend):
    """Download current report artifacts or request a fresh report run through Airflow."""
    st.subheader("Reports")
    valuation = choose_date(backend, "report_date")
    if valuation is None:
        return
    if st.button("Request fresh reports"):
        configuration = {**backend.results.publication(valuation), "valuation_date": str(valuation)}
        submit(backend, "xva_reports", configuration)
    try:
        files = backend.results.downloads(valuation)
    except PanelError as error:
        st.info(str(error))
        return
    for name, payload in files.items():
        st.download_button(f"Download {name}", data=payload, file_name=name,
                           mime="text/csv" if name.endswith(".csv") else "text/markdown")


def workflow_controls(backend):
    """Let the operator explicitly enable paused workflows, including the daily-input scheduling effect."""
    if st.sidebar.toggle("Workflow controls"):
        for dag_id, label in WORKFLOWS.items():
            state = backend.api.dag(dag_id)
            st.sidebar.caption(f"{label}: {'paused' if state.get('is_paused') else 'enabled'}")
            if state.get("is_paused"):
                button = "Enable daily input schedule" if dag_id == "xva_inputs" else f"Enable {label.lower()}"
                if st.sidebar.button(button, key=f"enable_{dag_id}"):
                    backend.api.unpause(dag_id)
                    st.rerun()


def main():
    """Render the control panel without contacting Airflow or submitting work until its controls require it."""
    st.set_page_config(page_title="XVA control panel", page_icon="📊", layout="wide")
    st.title("XVA control panel")
    st.caption("Valuations, counterparty corrections, and linked execution monitoring · times in Europe/London")
    page = st.sidebar.radio("Navigate", PAGES)
    st.sidebar.button("Refresh")
    try:
        backend = services()
        workflow_controls(backend)
        {"Overview": overview, "Launch batch": launch_batch, "Counterparty results": counterparty_results,
         "Correct and rerun": correct_counterparty, "Executions": executions, "Reports": reports}[page](backend)
    except PanelError as error:
        st.error(str(error))
