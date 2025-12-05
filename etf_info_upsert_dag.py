
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import pendulum
import pandas as pd
import json

from etf_info_etl_scripts.etf_info_logic import crawl_jpx_data, add_yield_to_df, upsert_to_mysql

@dag(
    dag_id='daily_etf_info_data_upsert',
    start_date=days_ago(1),
    # Chạy hàng ngày lúc 1 giờ sáng (DANG NONE-KHONGCHAY)
    schedule_interval=None, 
    catchup=False,
    tags=['etl', 'jpx', 'finance'],
    default_args={'owner': 'airflow', 'retries': 1}
)
def etf_etl_pipeline():
    
    # Task 1: Crawl và xử lý dữ liệu cơ bản (Trích xuất & Biến đổi 1)
    @task(task_id='extract_and_transform_base')
    def task_crawl_data():
        df_base = crawl_jpx_data()
        # Chuyển DataFrame sang JSON để lưu vào XCom (truyền giữa các tasks)
        return df_base.to_json(date_format='iso') 

    # Task 2: Lấy Yield (Biến đổi 2)
    @task(task_id='get_and_add_yield')
    def task_add_yield(df_json: str):
        df = pd.read_json(df_json)
        df_with_yield = add_yield_to_df(df)
        return df_with_yield.to_json(date_format='iso')

    # Task 3: UPSERT vào MySQL 
    @task(task_id='load_to_mysql_upsert')
    def task_load_data(df_with_yield_json: str):
        df = pd.read_json(df_with_yield_json)
        
        if 'id' in df.columns:
            df = df.drop(columns=['id']) 
        upsert_to_mysql(df)
        
    # --- Định nghĩa luồng Task (Dependencies) ---
    
    base_df_json = task_crawl_data()
    final_df_json = task_add_yield(base_df_json)
    task_load_data(final_df_json)

# Khởi tạo DAG
etf_upsert_dag = etf_etl_pipeline()