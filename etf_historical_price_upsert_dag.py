from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import pendulum
import pandas as pd

# Import logic từ file logic đã tạo
from etf_info_etl_scripts.etf_historical_price_logic import transform_price_data, upsert_price_to_mysql

# --- Cấu hình DAG ---
@dag(
    dag_id='daily_etf_historical_price_data_upsert',
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=['etf', 'price', 'history'],
)
def etf_price_history_pipeline():
    
    # TASK 1: EXTRACT, CRAWL, VÀ TRANSFORM
    @task(task_id='extract_transform_price_data')
    def extract_transform_task():
        """
        Thực hiện crawl song song từ ICE API và Yahoo Finance, 
        hợp nhất và chuẩn hóa dữ liệu OHLC và volume.
        """
        df_price = transform_price_data()
        
        # Chuyển DataFrame sang JSON để lưu vào XCom (truyền giữa các tasks)
        # Sử dụng orient='records' để tối ưu cho các DataFrame lớn.
        return df_price.to_json(orient='records', date_format='iso')

    # TASK 2: LOAD (UPSERT)
    @task(task_id='load_price_data_to_mysql_upsert')
    def load_task(df_price_json: str):
        """
        Đọc JSON từ XCom và UPSERT dữ liệu giá vào MySQL.
        """
        if df_price_json == "[]":
            print("Không có dữ liệu giá để tải.")
            return

        # Đọc lại DataFrame từ JSON
        df = pd.read_json(df_price_json, orient='records')

        # Gọi hàm UPSERT
        upsert_price_to_mysql(df)
            
    # --- Định nghĩa Luồng Công việc ---
    
    processed_df_json = extract_transform_task()
    load_task(processed_df_json)

# Khởi tạo DAG
etf_price_dag = etf_price_history_pipeline()