# airflow-risk

See [README_PROJECT.md](README_PROJECT.md) for the original demo overview and
[README_XVA.md](README_XVA.md) for the new PV, positive exposure, and CVA batch.

Use the [Streamlit control panel](README_UI.md) to launch batches, monitor linked
workflows, inspect CVA results, and submit counterparty corrections.

```
airflow-risk-demo/
│
├── dags/
│   └── xcom_demo.py
│
├── src/
│   └── risk_pipeline/
│       └── __init__.py
│
├── tests/
│
├── pyproject.toml
├── requirements.txt
├── .gitignore
└── README.md
```

---
## Start Project

python3 -m venv .venv
source .venv/bin/activate
which python 
python -m pip install --upgrade pip -- 26.2.1

AIRFLOW_VERSION=3.3.2

PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

echo $PYTHON_VERSION -- 3.13

pip install \
  "apache-airflow[postgres,slack,celery]==${AIRFLOW_VERSION}" \
  --constraint \
  "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip -e install .

---
## Check airflow setup

export AIRFLOW__CORE__DAGS_FOLDER="$HOME/Projects/GitHub/airflow-risk-trunk/airflow-risk/dags"


airflow config get-value core dags_folder


airflow db migrate

airflow api-server --port 8080
airflow scheduler
airflow dag-processor
or airflow standalone (for demo purpose)

airflow standalone
airflow dags list
airflow dags list-import-errors
pip install graphviz

export AIRFLOW_HOME="$HOME/airflow"
echo $AIRFLOW_HOME
airflow info
cat "$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"

export AIRFLOW__CORE__TEST_CONNECTION=Enabled
airflow connections test risk_postgres
stop the currently running standalone Airflow then restart

---
## Tests

python -m pytest -v -s
pytest tests/test_risk_calculations.py -v
pytest tests/test_risk_calculations.py::test_large_trade_threshold -v

python -m pytest \
  --cov=risk_pipeline \
  --cov-report=term-missing \
  -v
