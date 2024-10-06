from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


# def return_snowflake_conn(**kwargs):
#   hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
#   conn = hook.get_conn()

#   print(conn.account)
#   print(conn)

#   con=conn.cursor()
#   return con


@task
def extract(url):
  r = requests.get(url)
  data = r.json()
  return data

@task
def transform(data):
  results = [] # empyt list for now to hold the 90 days of stock info (open, high, low, close, volume)
  for d in data["Time Series (Daily)"]: # here d is a date: "YYYY-MM-DD"
    stock_info = data["Time Series (Daily)"][d]
    stock_info['date'] = d
    stock_info['symbol'] = 'TSM'
    results.append(stock_info)
    if len(results) == 90:
      break
  return results

@task
def load_v2(con,records):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    con= conn.cursor()
    target_table = "STOCK.raw_data.TSM_stock"
    try:
        con.execute("BEGIN;")
        con.execute(f"CREATE OR REPLACE TABLE {target_table}(date DATE PRIMARY KEY,open FLOAT,high FLOAT,low FLOAT,close FLOAT,volume  INTEGER,symbol  VARCHAR);")
        for r in records:
          open = r["1. open"]
          high = r["2. high"]
          low = r["3. low"]
          close = r["4. close"]
          volume = r["5. volume"]
          date = r["date"]
          symbol = r["symbol"]
          insert_sql = f"INSERT INTO {target_table} (date, open, high, low, close, volume, symbol) VALUES ('{date}', {open}, {high}, {low}, {close}, {volume}, '{symbol}');"
          con.execute(insert_sql)
        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id = 'TSM_ETL',
    start_date = datetime(2024,9,21),
    catchup=False,
    tags=['ETL'],
    schedule = '0 14 * * 1-5'
) as dag:
    url = Variable.get("TSM_url")
    # con = return_snowflake_conn()
    symbol='TSM'
    data=extract(url)
    TSM_90d=transform(data)
    load_task=load_v2(1,TSM_90d)
