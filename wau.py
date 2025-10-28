from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime


SNOWFLAKE_CONN_ID = Variable.get("snowflake_conn", default_var="snowflake_conn")
DATABASE = Variable.get("snowflake_database", default_var="USER_DB_GIRAFFE")

SCHEMA_RAW = "RAW"
STAGE_NAME = Variable.get("blob_stage_name", default_var="BLOB_STAGE")
S3_URL = Variable.get("blob_stage_url",default_var="s3://s3-geospatial/readonly/")

USER_TBL = "USER_SESSION_CHANNEL"
TS_TBL = "SESSION_TIMESTAMP"

USER_CSV = Variable.get("user_csv_name",default_var="user_session_channel.csv")

TS_CSV = Variable.get("ts_csv_name",default_var="session_timestamp.csv")



log = LoggingMixin().log

def get_conn_cursor():
    """Helper: create Snowflake connection."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    return conn, conn.cursor()

# --------------------------
# TASKS
# --------------------------

@task
def setup_objects():
    """Create schema, stage, file format, and tables if not exist."""
    conn, cur = get_conn_cursor()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{SCHEMA_RAW};")
        cur.execute(f"""
        CREATE OR REPLACE FILE FORMAT {DATABASE}.{SCHEMA_RAW}.CSV_FF
        TYPE = CSV
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"';
        """)
        cur.execute(f"""
        CREATE OR REPLACE STAGE {DATABASE}.{SCHEMA_RAW}.{STAGE_NAME}
        URL = '{S3_URL}'
        FILE_FORMAT = {DATABASE}.{SCHEMA_RAW}.CSV_FF;
        """)
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA_RAW}.{USER_TBL} (
            USERID INT NOT NULL,
            SESSIONID VARCHAR(32) PRIMARY KEY,
            CHANNEL VARCHAR(32) DEFAULT 'direct'
        );
        """)
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA_RAW}.{TS_TBL} (
            SESSIONID VARCHAR(32) PRIMARY KEY,
            TS TIMESTAMP
        );
        """)
        cur.execute("COMMIT;")
        log.info("Setup complete: schema, stage, file format, and tables ready.")
    except Exception:
        cur.execute("ROLLBACK;")
        raise
    finally:
        cur.close()
        conn.close()

@task
def load_user_table():
    """Load user_session_channel CSV from S3 into Snowflake."""
    conn, cur = get_conn_cursor()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"TRUNCATE TABLE {DATABASE}.{SCHEMA_RAW}.{USER_TBL};")
        cur.execute(f"""
        COPY INTO {DATABASE}.{SCHEMA_RAW}.{USER_TBL}
        FROM @{DATABASE}.{SCHEMA_RAW}.{STAGE_NAME}/{USER_CSV}
        ON_ERROR = 'ABORT_STATEMENT'
        FORCE = TRUE;
        """)
        cur.execute("COMMIT;")
        log.info(f"Loaded RAW.{USER_TBL} from {USER_CSV}")
    except Exception:
        cur.execute("ROLLBACK;")
        raise
    finally:
        cur.close()
        conn.close()

@task
def load_ts_table():
    """Load session_timestamp CSV from S3 into Snowflake."""
    conn, cur = get_conn_cursor()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"TRUNCATE TABLE {DATABASE}.{SCHEMA_RAW}.{TS_TBL};")
        cur.execute(f"""
        COPY INTO {DATABASE}.{SCHEMA_RAW}.{TS_TBL}
        FROM @{DATABASE}.{SCHEMA_RAW}.{STAGE_NAME}/{TS_CSV}
        ON_ERROR = 'ABORT_STATEMENT'
        FORCE = TRUE;
        """)
        cur.execute("COMMIT;")
        log.info(f"Loaded RAW.{TS_TBL} from {TS_CSV}")
    except Exception:
        cur.execute("ROLLBACK;")
        raise
    finally:
        cur.close()
        conn.close()

@task
def validate_counts():
    """Log table row counts."""
    conn, cur = get_conn_cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {DATABASE}.{SCHEMA_RAW}.{USER_TBL};")
        user_cnt = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*) FROM {DATABASE}.{SCHEMA_RAW}.{TS_TBL};")
        ts_cnt = cur.fetchone()[0]
        log.info(f"Row counts — {USER_TBL}: {user_cnt}, {TS_TBL}: {ts_cnt}")
    finally:
        cur.close()
        conn.close()

# --------------------------
# DAG DEFINITION
# --------------------------
with DAG(
    dag_id="wau_etl",
    start_date=datetime(2025, 10, 1),
    schedule="15 4 * * *",  # every day at 04:15 UTC
    catchup=False,
    tags=["snowflake", "etl", "raw", "wau_etl"],
) as dag:

    setup = setup_objects()
    load_user = load_user_table()
    load_ts = load_ts_table()
    check = validate_counts()

    trigger_elt = TriggerDagRunOperator(
        task_id="trigger_session_summary_elt",
        trigger_dag_id="session_summary_elt",
        wait_for_completion=False,
    )

    setup >> [load_user, load_ts] >> check >> trigger_elt