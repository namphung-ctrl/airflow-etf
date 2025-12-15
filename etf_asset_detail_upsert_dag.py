from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import pendulum
import pandas as pd

# 1. IMPORT LOGIC TỪ FILE etf_asset_logic.py

from etf_info_etl_scripts.etf_asset_detail_logic import (
    extract_and_transform_all_assets,
    insert_asset_detail_to_mysql
)

# --- Cấu hình DAG ---
@dag(
    dag_id='daily_etf_asset_detail_data_insert',
    start_date=days_ago(1),
    schedule_interval='0 1 * * *',
    catchup=False,
    tags=['etf', 'asset', 'daily'],
)
def etf_asset_detail_pipeline():
    
    # 2. TASK 1: EXTRACT VÀ TRANSFORM (E & T)
    @task(task_id='extract_and_transform_asset_data')
    def extract_transform_task():
        """
        Lấy tất cả ETF codes, crawl chi tiết tài sản (holdings) 
        và hợp nhất thành một DataFrame duy nhất.
        """
        # Hàm này trả về DataFrame đã xử lý
        df_asset_detail = extract_and_transform_all_assets()
        return df_asset_detail

    # 3. TASK 2: LOAD DỮ LIỆU VÀO MYSQL (L)
    @task(task_id='load_asset_data_to_mysql')
    def load_task(df_asset_detail: pd.DataFrame):
        """
        Chèn DataFrame chi tiết tài sản vào bảng etf_asset_detail_train.
        """
        if df_asset_detail is not None and not df_asset_detail.empty:
            insert_asset_detail_to_mysql(df_asset_detail)
        else:
            print("Không có dữ liệu chi tiết tài sản để tải.")
            
    # --- Định nghĩa Luồng Công việc ---
    
    # 4. Thiết lập Dependency: E&T phải chạy trước L
    processed_df = extract_transform_task()
    load_task(processed_df)

# Khởi tạo DAG

etf_asset_detail_pipeline()
