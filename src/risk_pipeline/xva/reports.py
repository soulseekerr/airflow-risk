"""Rebuild reports from published results without recalculating valuations."""

import argparse
import csv
import io
import math
from pathlib import Path

from risk_pipeline.xva.batch import read_json, validate_snapshot, write_json, write_text
from risk_pipeline.xva.calculations import reconcile
from risk_pipeline.xva.workflow import require_publication
from risk_pipeline.xva.postgres import transaction


REPORT_VERSION = "v2"


def load_published_results(publication_path):
    manifest = read_json(publication_path)
    require_publication({"batch_id": manifest["batch_id"], "publication_path": publication_path})
    if manifest.get("storage") != "postgres" or not manifest.get("connection_id"):
        raise ValueError("Reports require a PostgreSQL publication; republish old SQLite batches")
    with transaction(manifest["connection_id"], readonly=True) as cursor:
        cursor.execute("SELECT input_snapshot, is_current FROM airflow_xva.batches WHERE batch_id = %s", (manifest["batch_id"],))
        row = cursor.fetchone()
        if row is None:
            raise ValueError("Published batch is missing from the results database")
        if not row[1]:
            raise ValueError("Batch is no longer current for its valuation date; report the current published batch")
        snapshot = validate_snapshot(row[0])
        if snapshot["batch_id"] != manifest["batch_id"]:
            raise ValueError("Database snapshot belongs to another batch")
        valuation_date = snapshot["inputs"]["valuation_date"]
        if manifest.get("valuation_date", valuation_date) != valuation_date:
            raise ValueError("Publication valuation date does not match its batch")
        queries = {
            "trade_pv": "SELECT trade_id, ma_id, counterparty_code, 'EUR' AS currency, pv FROM airflow_xva.trade_pv WHERE valuation_date = %s ORDER BY trade_id",
            "exposures": "SELECT ma_id, exposure_date AS date, counterparty_code, currency, expected_positive_exposure, discount_factor, survival_probability, cva_increment FROM airflow_xva.exposures WHERE valuation_date = %s ORDER BY ma_id, exposure_date",
            "netting_set_cva": "SELECT ma_id, counterparty_code, currency, cva FROM airflow_xva.netting_set_cva WHERE valuation_date = %s ORDER BY ma_id",
            "counterparty_cva": "SELECT counterparty_code, currency, value_calc AS cva, value_adj AS cva_adj FROM airflow_xva.counterparty_xva WHERE valuation_date = %s AND xva_type = 'CVA' ORDER BY counterparty_code",
        }
        results = {}
        for key, query in queries.items():
            cursor.execute(query, (valuation_date,))
            columns = [column[0] for column in cursor.description]
            results[key] = [dict(zip(columns, row)) for row in cursor.fetchall()]
        for exposure in results["exposures"]:
            exposure["date"] = exposure["date"].isoformat()
    reconcile(results, snapshot["inputs"])
    total_cva = sum(r["cva"] for r in results["counterparty_cva"])
    if (len(results["trade_pv"]) != manifest["trade_count"]
            or len(results["netting_set_cva"]) != manifest["netting_set_count"]
            or not math.isclose(total_cva, manifest["total_cva"], rel_tol=1e-10, abs_tol=1e-8)):
        raise ValueError("Database results do not match the publication manifest")
    return snapshot, results


def write_csv(path, rows, fields):
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    return write_text(path, stream.getvalue())


def generate_reports(publication_path):
    snapshot, results = load_published_results(publication_path)
    batch_id, inputs = snapshot["batch_id"], snapshot["inputs"]
    directory = Path(publication_path).resolve().parent / "reports" / REPORT_VERSION
    prefix = {"batch_id": batch_id, "valuation_date": inputs["valuation_date"]}
    counterparties = {c["code"]: c for c in inputs["counterparties"]}
    summaries = []
    for cva in results["counterparty_cva"]:
        code = cva["counterparty_code"]
        trades = [t for t in results["trade_pv"] if t["counterparty_code"] == code]
        pv = sum(t["pv"] for t in trades)
        summaries.append({**prefix, "counterparty_code": code, "counterparty_name": counterparties[code]["name"],
                          "currency": cva["currency"], "trade_count": len(trades), "pv_t0": pv,
                          "cva": cva["cva"], "cva_adj": cva["cva_adj"], "cva_adjusted_pv": pv - cva["cva_adj"]})
    outputs = {}
    outputs["counterparty_summary"] = write_csv(directory / "counterparty_summary.csv", summaries,
        ["batch_id", "valuation_date", "counterparty_code", "counterparty_name", "currency", "trade_count", "pv_t0", "cva", "cva_adj", "cva_adjusted_pv"])
    fields = {
        "trade_pv": ["trade_id", "ma_id", "counterparty_code", "pv"],
        "exposures": ["ma_id", "date", "counterparty_code", "currency", "expected_positive_exposure", "discount_factor", "survival_probability", "cva_increment"],
        "netting_set_cva": ["ma_id", "counterparty_code", "currency", "cva"],
    }
    for key, columns in fields.items():
        outputs[key] = write_csv(directory / f"{key}.csv", [{**prefix, **{column: r[column] for column in columns}} for r in results[key]], ["batch_id", "valuation_date", *columns])
    total_pv = sum(r["pv"] for r in results["trade_pv"])
    total_cva = sum(r["cva"] for r in results["counterparty_cva"])
    total_cva_adj = sum(r["cva_adj"] for r in results["counterparty_cva"])
    summary = ["# XVA batch report", "", f"Batch: `{batch_id}`", "", f"Valuation date: {inputs['valuation_date']}",
               f"Currency: {inputs['reporting_currency']}", f"Model: `{snapshot['model_version']}`", "",
               f"Clean PV t0: {total_pv:,.2f}", f"Calculated CVA (loss): {total_cva:,.2f}",
               f"Adjusted CVA (loss): {total_cva_adj:,.2f}",
               f"CVA-adjusted PV: {total_pv - total_cva_adj:,.2f}", "",
               "| Counterparty code | Name | Trades | PV t0 | Calculated CVA | Adjusted CVA | Adjusted PV |",
               "| --- | --- | ---: | ---: | ---: | ---: | ---: |"]
    for row in summaries:
        name = row["counterparty_name"].replace("|", "\\|").replace("\n", " ")
        summary.append(f"| {row['counterparty_code']} | {name} | {row['trade_count']} | {row['pv_t0']:,.2f} | {row['cva']:,.2f} | {row['cva_adj']:,.2f} | {row['cva_adjusted_pv']:,.2f} |")
    summary.extend(["", "Exposure profiles are reported by MA; values from distinct MAs are not netted.",
                    "CVA uses independent default and exposure with the documented demo pricing model.", ""])
    outputs["summary"] = write_text(directory / "summary.md", "\n".join(summary))
    return write_json(directory / "report_manifest.json", {**prefix, "status": "reported", "report_version": REPORT_VERSION,
                                                           "total_pv_t0": total_pv, "total_cva": total_cva, "total_cva_adj": total_cva_adj, "outputs": outputs})


def main():
    parser = argparse.ArgumentParser(description="Generate reports for a published XVA batch")
    parser.add_argument("--publication", required=True)
    args = parser.parse_args()
    print(Path(generate_reports(args.publication)).read_text(), end="")


if __name__ == "__main__":
    main()
