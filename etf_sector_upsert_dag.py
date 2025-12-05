from airflow.decorators import dag, task
from airflow.utils import dates
import pendulum
import pandas as pd
import json

# Import logic từ file logic đã tạo
from etf_info_etl_scripts.etf_sector_logic import extract_transform_load_sector_data

# --- Cấu hình DAG ---
@dag(
    dag_id='daily_etf_sector_data_upsert',
    start_date=dates.days_ago(1),
    schedule="@daily",
    catchup=False,
    tags=['etf', 'sector', 'selenium'],
)
def etf_sector_pipeline():
    
    # TASK 1: RUN TOÀN BỘ QUY TRÌNH ETL VÀ LOAD
    @task(task_id='etl_and_load_sector_data')
    def run_etl_load():
        """
        Thực hiện toàn bộ quá trình Crawl (Lần 1 & Retry), Xử lý, 
        và UPSERT trực tiếp vào MySQL.
        """
        # Do quy trình crawl bằng Selenium rất phức tạp và cần giữ Driver giữa các hàm,
        # chúng ta gói gọn ETL và Load vào một Task duy nhất.
        extract_transform_load_sector_data()
        
    # --- Định nghĩa Luồng Công việc ---
    run_etl_load()

# Khởi tạo DAG
etf_sector_dag = etf_sector_pipeline()