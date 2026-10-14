"""Counterparty corrections preserve other outputs and remain safely retryable."""

import copy
from pathlib import Path

import pytest

from risk_pipeline.xva.batch import read_json, run_batch, write_json
from risk_pipeline.xva.calculations import calculate, generate_scenarios
from risk_pipeline.xva.generation import generate_portfolio
from risk_pipeline.xva.postgres import transaction
from risk_pipeline.xva.reports import generate_reports, load_published_results
from risk_pipeline.xva.reruns import calculate_counterparty, check_scope, prepare_counterparty_rerun, publish_counterparty


@pytest.fixture
def published(tmp_path, pg_connection):
    """Publish a short synthetic portfolio for targeted rerun integration checks."""
    inputs = generate_portfolio("2026-10-14", 3, 9, horizon_days=8, simulation_paths=2, collateral_fraction=0)
    source = write_json(tmp_path / "initial.json", inputs)
    manifest = run_batch(source, tmp_path / "outputs", pg_connection)
    return inputs, manifest


def corrected_config(inputs, tmp_path):
    """Write a full revised counterparty feed, changing only the first counterparty's recovery."""
    rows = copy.deepcopy(inputs["counterparties"])
    rows[0]["recovery_rate"] = 0.9
    path = write_json(tmp_path / "corrected-counterparties.json", {"valuation_date": inputs["valuation_date"], "rows": rows})
    return {"valuation_date": inputs["valuation_date"], "counterparty_code": rows[0]["code"],
            "feeds": {"counterparties": path}, "output_root": str(tmp_path / "outputs")}


def database_rows(connection_id):
    """Read output rows including adjustment timestamps for exact preservation comparisons."""
    with transaction(connection_id, readonly=True) as cursor:
        rows = {}
        for table in ("trade_pv", "exposures", "netting_set_cva", "counterparty_xva"):
            cursor.execute(f"SELECT * FROM airflow_xva.{table} ORDER BY 1,2,3,4")
            columns = [column[0] for column in cursor.description]
            rows[table] = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return rows


def test_scoped_calculation_matches_full_batch():
    """Keep identical scenarios and reporting grids even when the chosen trades mature earlier."""
    inputs = generate_portfolio("2026-10-14", 3, 9, horizon_days=8, simulation_paths=2)
    scenarios = generate_scenarios(inputs)
    full = calculate(inputs, scenarios)
    for code in ("0000001", "0000002"):
        scoped = calculate(inputs, scenarios, counterparty_code=code)
        assert scoped == {kind: [row for row in rows if row["counterparty_code"] == code] for kind, rows in full.items()}
    with pytest.raises(ValueError, match="Unknown counterparty"):
        calculate(inputs, scenarios, counterparty_code="UNKNOWN")


@pytest.mark.parametrize("change", ["other_counterparty", "market", "simulation", "maturity", "shared_curve", "missing"])
def test_outside_scope_changes_rejected(change):
    """Require full reruns for settings, shared curves, other counterparties, and grid changes."""
    base = generate_portfolio("2026-10-14", 3, 9, horizon_days=8, simulation_paths=2)
    if change == "shared_curve":
        base["counterparties"][1]["credit_curve_id"] = base["counterparties"][0]["credit_curve_id"]
    updated = copy.deepcopy(base)
    if change == "other_counterparty":
        updated["counterparties"][1]["recovery_rate"] = 0.8
    elif change == "market":
        updated["market"]["flat_rate"] += 0.01
    elif change == "simulation":
        updated["simulation"]["seed"] += 1
    elif change == "maturity":
        updated["trades"][0]["maturity_date"] = "2026-10-25"
    elif change == "shared_curve":
        updated["credit_curves"][0]["points"][0]["survival_probability"] *= 0.99
    else:
        updated["counterparties"].pop(0)
    with pytest.raises(ValueError):
        check_scope(base, updated, "0000001")


def test_correction_preserves_other_rows_and_refreshes_reports(published, tmp_path, pg_connection):
    """Correct one counterparty, preserving other adjustments, non-CVA types, and complete reports."""
    inputs, old_manifest = published
    with transaction(pg_connection) as cursor:
        cursor.execute("UPDATE airflow_xva.counterparty_xva SET value_adj=value_calc+123")
        cursor.execute("""INSERT INTO airflow_xva.counterparty_xva
            (valuation_date,counterparty_code,currency,xva_type,value_calc,value_adj)
            VALUES ('2026-10-14','0000001','EUR','DVA',7,8)""")
    before = database_rows(pg_connection)
    config = corrected_config(inputs, tmp_path)
    handoff = prepare_counterparty_rerun(config, tmp_path, "correction-1", pg_connection)
    assert prepare_counterparty_rerun(config, tmp_path, "correction-1", pg_connection) == handoff
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute("""SELECT f.feed_type,f.feed_id FROM airflow_xva.input_set_feeds f
            JOIN airflow_xva.input_sets s USING(submission_id) WHERE s.batch_id=%s""", (handoff["base_batch_id"],))
        baseline_ids = dict(cursor.fetchall())
        cursor.execute("SELECT feed_type,feed_id FROM airflow_xva.input_set_feeds WHERE submission_id=%s", (handoff["submission_id"],))
        corrected_ids = dict(cursor.fetchall())
        assert corrected_ids["counterparties"] != baseline_ids["counterparties"]
        assert all(corrected_ids[kind] == feed_id for kind, feed_id in baseline_ids.items() if kind != "counterparties")
    result_path = calculate_counterparty(handoff)
    manifest = publish_counterparty(handoff, result_path)
    after = database_rows(pg_connection)
    for table in before:
        assert [row for row in before[table] if row["counterparty_code"] != "0000001"] == [row for row in after[table] if row["counterparty_code"] != "0000001"]
    cva = next(row for row in after["counterparty_xva"] if row["counterparty_code"] == "0000001" and row["xva_type"] == "CVA")
    assert cva["value_calc"] == cva["value_adj"]
    assert cva["value_calc"] != next(row["value_calc"] for row in before["counterparty_xva"] if row["counterparty_code"] == "0000001" and row["xva_type"] == "CVA")
    assert next(row for row in after["counterparty_xva"] if row["xva_type"] == "DVA") == next(row for row in before["counterparty_xva"] if row["xva_type"] == "DVA")
    assert read_json(generate_reports(manifest))["total_cva"] == pytest.approx(sum(row["value_calc"] for row in after["counterparty_xva"] if row["xva_type"] == "CVA"))
    with pytest.raises(ValueError, match="no longer current"):
        load_published_results(old_manifest)
    assert publish_counterparty(handoff, result_path) == manifest
    assert database_rows(pg_connection) == after
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute("SELECT status,old_cva,new_cva,published_at FROM airflow_xva.counterparty_reruns")
        status, old_cva, new_cva, stamp = cursor.fetchone()
        assert status == "published" and stamp and old_cva != new_cva == cva["value_calc"]


def test_stale_prepared_correction_cannot_overwrite_new_batch(published, tmp_path, pg_connection):
    """Reject an in-flight correction after a full-date publication supersedes its base revision."""
    inputs, _ = published
    handoff = prepare_counterparty_rerun(corrected_config(inputs, tmp_path), tmp_path, "stale", pg_connection)
    result_path = calculate_counterparty(handoff)
    changed = copy.deepcopy(inputs)
    changed["market"]["flat_rate"] += 0.001
    run_batch(write_json(tmp_path / "new-full.json", changed), tmp_path / "outputs", pg_connection)
    before = database_rows(pg_connection)
    with pytest.raises(ValueError, match="superseded"):
        publish_counterparty(handoff, result_path)
    assert database_rows(pg_connection) == before


def test_corrupt_correction_rolls_back(published, tmp_path, pg_connection):
    """Leave all current outputs intact when scoped result completeness or reconciliation fails."""
    inputs, _ = published
    handoff = prepare_counterparty_rerun(corrected_config(inputs, tmp_path), tmp_path, "corrupt", pg_connection)
    result_path = calculate_counterparty(handoff)
    result = read_json(result_path)
    result["results"]["trade_pv"].pop()
    write_json(result_path, result)
    before = database_rows(pg_connection)
    with pytest.raises(ValueError, match="Incomplete"):
        publish_counterparty(handoff, result_path)
    assert database_rows(pg_connection) == before


def test_corrected_feed_id_and_invalid_retry(published, tmp_path, pg_connection):
    """Accept immutable feed IDs and reject source mutation within the same Airflow run."""
    from risk_pipeline.xva.feeds import ingest_feed
    inputs, _ = published
    config = corrected_config(inputs, tmp_path)
    path = config["feeds"]["counterparties"]
    feed_id = ingest_feed("counterparties", Path(path).read_text(), "corrected-provider", inputs["valuation_date"], pg_connection)
    config["feeds"]["counterparties"] = {"feed_id": feed_id}
    handoff = prepare_counterparty_rerun(config, tmp_path, "feed-reference", pg_connection)
    assert read_json(handoff["snapshot_path"])["inputs"]["counterparties"][0]["recovery_rate"] == 0.9
    with pytest.raises(ValueError, match="retry changed"):
        prepare_counterparty_rerun({**config, "counterparty_code": "0000002"}, tmp_path, "feed-reference", pg_connection)


def test_invalid_feed_rejection_leaves_current_outputs(published, tmp_path, pg_connection):
    """Persist bad corrected feed validation errors while retaining the last published CVA."""
    inputs, _ = published
    config = corrected_config(inputs, tmp_path)
    feed = read_json(config["feeds"]["counterparties"])
    feed["rows"][0]["recovery_rate"] = 1.2
    write_json(config["feeds"]["counterparties"], feed)
    before = database_rows(pg_connection)
    with pytest.raises(ValueError, match="rejected"):
        prepare_counterparty_rerun(config, tmp_path, "invalid-feed", pg_connection)
    assert database_rows(pg_connection) == before
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute("SELECT COUNT(*) FROM airflow_xva.input_validation_errors")
        assert cursor.fetchone()[0] > 0


def test_trade_replacement_updates_counts_without_leaving_old_rows(published, tmp_path, pg_connection):
    """Replace a trade and add another, updating the scoped rows and complete portfolio report counts."""
    inputs, _ = published
    trades = copy.deepcopy(inputs["trades"])
    removed_id = trades[0]["trade_id"]
    trades[0]["trade_id"] = "CORRECTED_TRADE"
    trades[0]["notional"] *= 1.5
    extra = copy.deepcopy(trades[0])
    extra["trade_id"] = "ADDED_TRADE"
    trades.append(extra)
    path = write_json(tmp_path / "corrected-trades.json", {"valuation_date": inputs["valuation_date"], "rows": trades})
    config = {"valuation_date": inputs["valuation_date"], "counterparty_code": "0000001",
              "feeds": {"trades": path}, "output_root": str(tmp_path / "outputs")}
    handoff = prepare_counterparty_rerun(config, tmp_path, "changed-trades", pg_connection)
    manifest = publish_counterparty(handoff, calculate_counterparty(handoff))
    snapshot, results = load_published_results(manifest)
    assert len(results["trade_pv"]) == len(inputs["trades"]) + 1
    assert removed_id not in {row["trade_id"] for row in results["trade_pv"]}
    full = calculate(snapshot["inputs"], generate_scenarios(snapshot["inputs"]))
    for kind in full:
        key = "trade_id" if kind == "trade_pv" else "counterparty_code" if kind == "counterparty_cva" else "ma_id"
        actual = [{k: v for k, v in row.items() if k != "cva_adj"} for row in results[kind]]
        assert sorted(actual, key=lambda row: (row[key], row.get("date", ""))) == sorted(full[kind], key=lambda row: (row[key], row.get("date", "")))
    assert read_json(manifest)["trade_count"] == len(trades)
    generate_reports(manifest)


def test_database_failure_rolls_back_every_output(published, tmp_path, pg_connection, monkeypatch):
    """Roll back deletions and replacement rows together if publication fails after inserting outputs."""
    import risk_pipeline.xva.reruns as reruns
    inputs, _ = published
    handoff = prepare_counterparty_rerun(corrected_config(inputs, tmp_path), tmp_path, "rollback", pg_connection)
    result_path = calculate_counterparty(handoff)
    before = database_rows(pg_connection)
    insert = reruns.insert_results

    def fail_after_insert(cursor, valuation_date, results):
        """Inject a failure after all scoped replacement rows were written inside the transaction."""
        insert(cursor, valuation_date, results)
        raise RuntimeError("publication interrupted")

    monkeypatch.setattr(reruns, "insert_results", fail_after_insert)
    with pytest.raises(RuntimeError, match="interrupted"):
        publish_counterparty(handoff, result_path)
    assert database_rows(pg_connection) == before
    monkeypatch.setattr(reruns, "insert_results", insert)
    publish_counterparty(handoff, result_path)


def test_last_trade_removal_resets_counterparty_cva_to_zero(published, tmp_path, pg_connection):
    """Remove all trades for one counterparty and delete its old exposure/netting-set outputs."""
    inputs, _ = published
    trades = [copy.deepcopy(t) for t in inputs["trades"] if t["counterparty_code"] != "0000002"]
    path = write_json(tmp_path / "remaining-trades.json", {"valuation_date": inputs["valuation_date"], "rows": trades})
    config = {"valuation_date": inputs["valuation_date"], "counterparty_code": "0000002",
              "feeds": {"trades": path}, "output_root": str(tmp_path / "outputs")}
    handoff = prepare_counterparty_rerun(config, tmp_path, "remove-last-trade", pg_connection)
    manifest = publish_counterparty(handoff, calculate_counterparty(handoff))
    _, results = load_published_results(manifest)
    assert next(row for row in results["counterparty_cva"] if row["counterparty_code"] == "0000002")["cva"] == 0
    for kind in ("trade_pv", "exposures", "netting_set_cva"):
        assert not any(row["counterparty_code"] == "0000002" for row in results[kind])


def test_same_run_publication_retry_recovers_lost_manifest(published, tmp_path, pg_connection):
    """Rebuild a lost publication manifest after commit without resetting a later manual adjustment."""
    inputs, _ = published
    handoff = prepare_counterparty_rerun(corrected_config(inputs, tmp_path), tmp_path, "recover-manifest", pg_connection)
    result_path = calculate_counterparty(handoff)
    manifest = publish_counterparty(handoff, result_path)
    Path(manifest).unlink()
    with transaction(pg_connection) as cursor:
        cursor.execute("UPDATE airflow_xva.counterparty_xva SET value_adj=value_calc+10 WHERE counterparty_code='0000001'")
    before = database_rows(pg_connection)
    assert publish_counterparty(handoff, result_path) == manifest
    assert Path(manifest).is_file()
    assert database_rows(pg_connection) == before
    generate_reports(manifest)


@pytest.mark.parametrize("configuration", [{}, {"valuation_date": 42, "counterparty_code": "0000001"},
                                         {"valuation_date": "2026-10-14", "counterparty_code": "0000001", "feeds": {}},
                                         {"valuation_date": "2026-10-14", "counterparty_code": "0000001", "feeds": {"unknown": "file"}}])
def test_invalid_rerun_configuration_rejected_before_database_access(tmp_path, configuration):
    """Require an explicit date, counterparty, and known corrected feed types before staging."""
    with pytest.raises(ValueError, match="requires"):
        prepare_counterparty_rerun(configuration, tmp_path, "invalid-request")


def test_missing_publication_requires_full_batch(tmp_path, pg_connection):
    """Prevent corrections from creating an incomplete first publication for an unknown valuation date."""
    config = {"valuation_date": "2026-10-14", "counterparty_code": "0000001", "feeds": {"trades": "unused"}}
    with pytest.raises(ValueError, match="No published batch"):
        prepare_counterparty_rerun(config, tmp_path, "unpublished-date", pg_connection)


def test_mutating_accepted_baseline_rows_is_rejected(published, tmp_path, pg_connection):
    """Detect changes to immutable baseline rows even when they belong to the requested counterparty."""
    inputs, _ = published
    with transaction(pg_connection) as cursor:
        cursor.execute("UPDATE airflow_xva.input_trades SET notional=notional*2 WHERE counterparty_code='0000001'")
    with pytest.raises(ValueError, match="Published input rows changed"):
        prepare_counterparty_rerun(corrected_config(inputs, tmp_path), tmp_path, "mutated-baseline", pg_connection)
