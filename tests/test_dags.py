from pathlib import Path

from airflow.dag_processing.dagbag import DagBag


DAGS_FOLDER = Path(__file__).resolve().parents[1] / "dags"


def test_no_dag_import_errors():
    dag_bag = DagBag(
        dag_folder=str(DAGS_FOLDER),
    )

    assert dag_bag.import_errors == {}


def test_xcom_demo_dag_exists():
    dag_bag = DagBag(
        dag_folder=str(DAGS_FOLDER),
    )

    dag = dag_bag.get_dag("xcom_demo")

    assert dag is not None