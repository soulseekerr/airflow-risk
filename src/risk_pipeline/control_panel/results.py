"""Read-only PostgreSQL queries and constrained report downloads for the XVA UI."""

import os
from pathlib import Path

from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection
from airflow.sdk.exceptions import AirflowNotFoundException as SDKConnectionNotFound

from risk_pipeline.control_panel.airflow_api import PanelError
from risk_pipeline.xva.batch import read_json
from risk_pipeline.xva.postgres import DEFAULT_CONNECTION_ID, transaction
from risk_pipeline.xva.reports import REPORT_VERSION


class ResultsStore:
    """Read published date-level results using the existing Airflow PostgreSQL connection."""

    def __init__(self, connection_id=DEFAULT_CONNECTION_ID, output_root=None):
        """Resolve the same shared artifact root used by the Airflow workers."""
        self.connection_id = connection_id
        home = Path(os.getenv("AIRFLOW_HOME", str(Path.home() / "airflow"))).expanduser().resolve()
        root = Path(output_root or os.getenv("XVA_OUTPUT_ROOT", str(home / "xva"))).expanduser()
        self.output_root = (root if root.is_absolute() else home / root).resolve()

    @classmethod
    def from_environment(cls):
        """Allow a read-only connection alias without exposing its credentials to the browser."""
        return cls(os.getenv("XVA_UI_POSTGRES_CONN_ID", DEFAULT_CONNECTION_ID))

    def query(self, sql, parameters=(), required="batches"):
        """Return named rows in a read-only transaction, treating an uninitialized schema as empty."""
        try:
            # The worker SDK skips the metastore in standalone processes such as Streamlit.
            airflow_connection = Connection.get_connection_from_secrets(self.connection_id)
            with transaction(self.connection_id, readonly=True, airflow_connection=airflow_connection) as cursor:
                cursor.execute("SELECT to_regclass(%s)", (f"airflow_xva.{required}",))
                if cursor.fetchone()[0] is None:
                    return []
                cursor.execute(sql, parameters)
                columns = [column[0] for column in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except (AirflowNotFoundException, SDKConnectionNotFound):
            raise PanelError(f"Airflow connection '{self.connection_id}' was not found. Start Streamlit with the same AIRFLOW_HOME and metadata database configuration as Airflow, or set AIRFLOW_CONN_{self.connection_id.upper()}.") from None
        except Exception:
            raise PanelError("Results database unavailable. Check the Streamlit server's Airflow connection configuration and database read permissions.") from None

    def dates(self):
        """Show current publications with totals that cannot multiply across joined trade/XVA rows."""
        return self.query("""SELECT b.valuation_date,b.batch_id,b.currency,
            (SELECT COUNT(*) FROM airflow_xva.counterparty_xva x
             WHERE x.valuation_date=b.valuation_date AND x.xva_type='CVA') AS counterparty_count,
            (SELECT COUNT(*) FROM airflow_xva.trade_pv p WHERE p.valuation_date=b.valuation_date) AS trade_count,
            (SELECT COALESCE(SUM(p.pv),0) FROM airflow_xva.trade_pv p WHERE p.valuation_date=b.valuation_date) AS pv_t0,
            (SELECT COALESCE(SUM(x.value_calc),0) FROM airflow_xva.counterparty_xva x
             WHERE x.valuation_date=b.valuation_date AND x.xva_type='CVA') AS cva_calculated,
            (SELECT COALESCE(SUM(x.value_adj),0) FROM airflow_xva.counterparty_xva x
             WHERE x.valuation_date=b.valuation_date AND x.xva_type='CVA') AS cva_adjusted
            FROM airflow_xva.batches b WHERE b.is_current ORDER BY b.valuation_date DESC""")

    def counterparties(self, valuation_date):
        """Read counterparty names from the current frozen inputs and their calculated/adjusted CVA."""
        return self.query("""WITH names AS (
              SELECT c->>'code' AS code,c->>'name' AS name
              FROM airflow_xva.batches b CROSS JOIN LATERAL
                   jsonb_array_elements(b.input_snapshot->'inputs'->'counterparties') c
              WHERE b.valuation_date=%s AND b.is_current),
            pv AS (SELECT counterparty_code,COUNT(*) AS trade_count,SUM(pv) AS pv_t0
                   FROM airflow_xva.trade_pv WHERE valuation_date=%s GROUP BY counterparty_code)
            SELECT x.counterparty_code,n.name AS counterparty_name,COALESCE(p.trade_count,0) AS trade_count,
                   COALESCE(p.pv_t0,0) AS pv_t0,x.value_calc,x.value_adj,x.updated_at
            FROM airflow_xva.counterparty_xva x LEFT JOIN names n ON n.code=x.counterparty_code
            LEFT JOIN pv p ON p.counterparty_code=x.counterparty_code
            WHERE x.valuation_date=%s AND x.xva_type='CVA' ORDER BY x.value_calc DESC,x.counterparty_code""",
            (valuation_date, valuation_date, valuation_date), required="counterparty_xva")

    def details(self, valuation_date, code):
        """Load bounded counterparty-specific trade PVs and MA exposure profiles."""
        parameters = (valuation_date, code)
        trades = self.query("""SELECT trade_id,ma_id,pv FROM airflow_xva.trade_pv
            WHERE valuation_date=%s AND counterparty_code=%s ORDER BY trade_id""", parameters, required="trade_pv")
        exposures = self.query("""SELECT ma_id,exposure_date,expected_positive_exposure,cva_increment
            FROM airflow_xva.exposures WHERE valuation_date=%s AND counterparty_code=%s
            ORDER BY exposure_date,ma_id""", parameters, required="exposures")
        return trades, exposures

    def feeds(self, valuation_date):
        """List recent exact feed IDs so a correction can choose versions without guessing arrival order."""
        return self.query("""SELECT feed_type,feed_id,source,status,received_at
            FROM airflow_xva.input_feed_submissions WHERE valuation_date=%s
            ORDER BY received_at DESC LIMIT 200""", (valuation_date,), required="input_feed_submissions")

    def corrections(self, valuation_date):
        """Read prepared and published correction history without altering its audit timestamps."""
        return self.query("""SELECT counterparty_code,status,old_cva,new_cva,
            new_cva-old_cva AS cva_change,prepared_at,published_at,rerun_id
            FROM airflow_xva.counterparty_reruns WHERE valuation_date=%s
            ORDER BY prepared_at DESC LIMIT 100""", (valuation_date,), required="counterparty_reruns")

    def publication(self, valuation_date):
        """Return a current publication accessible at the shared artifact root."""
        current = next((row for row in self.dates() if row["valuation_date"].isoformat() == str(valuation_date)), None)
        if not current:
            raise PanelError("No current publication exists for this valuation date.")
        batch_id = current["batch_id"]
        path = self.output_root / batch_id / "published.json"
        self._within(path, self.output_root)
        try:
            manifest = read_json(path)
        except (OSError, ValueError):
            raise PanelError("Publication files are unavailable. Mount the workers' shared XVA_OUTPUT_ROOT on the Streamlit server at the same path.") from None
        if manifest.get("batch_id") != batch_id or manifest.get("status") != "published":
            raise PanelError("Publication files do not match the current database revision.")
        return {"batch_id": batch_id, "publication_path": str(path)}

    def _within(self, path, directory):
        """Reject path traversal and symlink escapes in stored artifact metadata."""
        if not path.resolve().is_relative_to(directory.resolve()):
            raise PanelError("A report path is outside the configured artifact directory.")

    def downloads(self, valuation_date):
        """Read only the expected report files from the current batch's report directory."""
        publication = self.publication(valuation_date)
        directory = Path(publication["publication_path"]).parent / "reports" / REPORT_VERSION
        self._within(directory, self.output_root)
        try:
            self._within(directory / "report_manifest.json", directory)
            manifest = read_json(directory / "report_manifest.json")
            if manifest.get("batch_id") != publication["batch_id"] or manifest.get("status") != "reported":
                raise PanelError("Report files do not match the current published revision.")
            files = {}
            allowed = {"counterparty_summary.csv", "trade_pv.csv", "exposures.csv", "netting_set_cva.csv", "summary.md"}
            for value in manifest["outputs"].values():
                path = Path(value)
                self._within(path, directory)
                if path.name not in allowed:
                    raise PanelError("Unexpected report file in the report manifest.")
                files[path.name] = path.read_bytes()
            return files
        except (OSError, ValueError, KeyError, TypeError):
            raise PanelError("Reports are not available at the shared artifact root. Request reports or check the shared mount.") from None
