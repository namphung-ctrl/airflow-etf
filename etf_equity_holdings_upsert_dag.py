from airflow.decorators import dag, task
from airflow.utils import dates
import pendulum
import pandas as pd
import json

# Import logic từ file logic đã tạo
from etf_info_etl_scripts.etf_equity_holdings_logic import extract_transform_load_valuation_data

# --- Cấu hình DAG ---
@dag(
    dag_id='daily_etf_equity_holdings_data_upsert',
    start_date=dates.days_ago(1),
    schedule_interval='0 1 * * *',
    catchup=False,
    tags=['etf', 'valuation', 'selenium'],
)
def etf_valuation_pipeline():
    
    # TASK 1: RUN TOÀN BỘ QUY TRÌNH ETL VÀ LOAD
    @task(task_id='etl_and_load_valuation_metrics')
    def run_etl_load():
        """
        Thực hiện toàn bộ quá trình Crawl (Lần 1 & Retry), Xử lý, 
        và UPSERT trực tiếp vào MySQL.
        """
        # Gói gọn ETL và Load vào một Task duy nhất vì cần duy trì phiên Selenium
        extract_transform_load_valuation_data()
        
    # --- Định nghĩa Luồng Công việc ---
    run_etl_load()

# Khởi tạo DAG

etf_valuation_dag = etf_valuation_pipeline()
