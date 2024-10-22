from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()


@task
def Create_session_summary():
    try:
        cur = return_snowflake_conn()
        cur.execute("BEGIN;")
        sql = (
            """
        CREATE OR REPLACE TABLE dev.analytics.session_summary AS
        SELECT 
            us.session_id, 
            us.user_id, 
            st.session_start, 
            st.session_end, 
            us.channel
        FROM 
            dev.raw_data.user_session_channel us
        JOIN 
            dev.raw_data.session_timestamp st 
        ON 
            us.session_id = st.session_id
        WHERE us.session_id IS NOT NULL;
        """,
        )
        cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print("Failed to create stage!!!!!!")
        raise e


@task
def delete_deduplicate():
    try:
        cur = return_snowflake_conn()
        cur.execute("BEGIN;")
        sql = """
        DELETE FROM dev.analytics.session_summary
        WHERE session_id IN (
            SELECT session_id FROM (
                SELECT session_id, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY session_start DESC) AS row_num
                FROM analytics.session_summary
            ) WHERE row_num > 1
        );
        """
        cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print("Failed to create stage!!!!!!")
        raise e


# Define the DAG
default_args = {
    "start_date": datetime(2024, 10, 15),
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="Run merge",
    default_args=default_args,
    schedule_interval=None,
    tags=["ETL"],
) as dag:

    create_session_summary = Create_session_summary
    deduplicate_session_summary = delete_deduplicate

    create_session_summary >> deduplicate_session_summary
