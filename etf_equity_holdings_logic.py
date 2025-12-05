import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import time
import random
import numpy as np
import mysql.connector
import requests

# --- Cấu hình Cố định ---
API_URL = 'https://inav.ice.com/api/1/tse/iopv/table?type=etf&language=en'
HEADERS = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://inav.ice.com'}
INITIAL_WAIT_TIMEOUT = 30 # Timeout cho lần crawl ban đầu
RETRY_WAIT_TIMEOUT = 40  # Timeout cho lần retry

# --- Khởi tạo Dữ liệu gốc (Được thay thế bằng tham số trong Airflow) ---

def get_etf_codes():
    """Lấy danh sách các ETF code theo đúng thứ tự API trả về."""
    try:
        r = requests.get(API_URL, headers=HEADERS, timeout=10)
        r.raise_for_status()
        rows = r.json().get("rows", [])
        return [row["code"] for row in rows if row.get("code")]
    except requests.RequestException as e:
        print(f"⚠️ Lỗi khi lấy ETF codes từ API: {e}")
        return []

# --------------------------------------------------------
# HÀM 1 & 2: CRAWL LẦN 1 & RETRY
# --------------------------------------------------------

def init_webdriver():
    """Khởi tạo và cấu hình WebDriver."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--log-level=3")
    try:
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    except WebDriverException as e:
        print(f"❌ Lỗi WebDriver: ChromeDriver không thể khởi tạo: {e}")
        raise

def crawl_valuation_metrics(codes_list, timeout):
    """Thực hiện crawl chỉ số định giá với thời gian chờ cho trước."""
    data_list = []
    MISSING_CODES = []
    driver = init_webdriver()

    total_codes = len(codes_list)
    print(f"\n--- Bắt đầu Crawl ({total_codes} mã, Timeout: {timeout}s) ---")

    for index, base_code in enumerate(codes_list):
        base_code = str(base_code)
        full_code = f"{base_code}.T"
        url = f"https://finance.yahoo.com/quote/{full_code}/holdings/"

        etf_data = {
            'etf_code': base_code, 'price/earnings': pd.NA, 'price/book': pd.NA,
            'price/sales': pd.NA, 'price/cashflow': pd.NA
        }
        crawled_count = 0

        try:
            driver.get(url)
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.XPATH, "//section[@data-testid='equity-holdings']//table"))
            )

            equity_tbody = driver.find_element(
                By.XPATH,
                "//section[@data-testid='equity-holdings']//table/tbody"
            )
            rows = equity_tbody.find_elements(By.TAG_NAME, "tr")

            for row in rows:
                try:
                    metric_name = row.find_element(By.XPATH, "./td[1]").text
                    metric_value = row.find_element(By.XPATH, "./td[2]").text

                    if metric_name == 'Price/Earnings': etf_data['price/earnings'] = metric_value; crawled_count += 1
                    elif metric_name == 'Price/Book': etf_data['price/book'] = metric_value; crawled_count += 1
                    elif metric_name == 'Price/Sales': etf_data['price/sales'] = metric_value; crawled_count += 1
                    elif metric_name == 'Price/Cashflow': etf_data['price/cashflow'] = metric_value; crawled_count += 1
                    
                    if crawled_count == 4: break

                except NoSuchElementException:
                    continue
            
            if crawled_count < 4 and timeout == INITIAL_WAIT_TIMEOUT:
                MISSING_CODES.append(base_code) # Chỉ thêm vào MISSING_CODES ở lần crawl đầu
                
        except TimeoutException:
            if timeout == INITIAL_WAIT_TIMEOUT: MISSING_CODES.append(base_code)
        except Exception:
            if timeout == INITIAL_WAIT_TIMEOUT: MISSING_CODES.append(base_code)
        
        data_list.append(etf_data)
        time.sleep(1.5)

    driver.quit()
    df_result = pd.DataFrame(data_list)
    
    # Ở lần crawl ban đầu, trả về danh sách mã bị sót
    if timeout == INITIAL_WAIT_TIMEOUT:
        return df_result, MISSING_CODES
    
    # Ở lần retry, chỉ trả về DataFrame kết quả
    return df_result

# --------------------------------------------------------
# HÀM 3: HỢP NHẤT, XỬ LÝ VÀ CHUẨN HÓA DỮ LIỆU
# --------------------------------------------------------

def process_and_merge(df_initial, df_retry, df_all_codes):
    """
    Hợp nhất dữ liệu ban đầu và dữ liệu retry, sau đó chuẩn hóa kiểu dữ liệu.
    """
    
    # 1. Chuẩn hóa tên cột
    df_initial.rename(columns={'price/earnings': 'price_earnings', 'price/book': 'price_book', 
                                'price/sales': 'price_sales', 'price/cashflow': 'price_cashflow'}, inplace=True)
    df_retry.rename(columns={'price/earnings': 'price_earnings', 'price/book': 'price_book', 
                              'price/sales': 'price_sales', 'price/cashflow': 'price_cashflow'}, inplace=True)
    
    # 2. Chuẩn bị cho Combine/Update
    cols_to_convert = ['price_earnings', 'price_book', 'price_sales', 'price_cashflow']
    
    # Chuyển đổi giá trị sang float, đặt NA/None thành index
    df_initial.set_index('etf_code', inplace=True)
    df_retry.set_index('etf_code', inplace=True)

    # 3. Sử dụng update: Giá trị KHÔNG NA từ df_retry sẽ thay thế NA trong df_initial
    df_initial.update(df_retry, overwrite=False) 
    
    # 4. Finalize và Sắp xếp
    df_final_updated = df_initial.reset_index()
    
    # Chuyển đổi kiểu dữ liệu sang số (coerce sẽ biến lỗi thành NaN)
    for col in cols_to_convert:
        df_final_updated[col] = pd.to_numeric(df_final_updated[col], errors='coerce')

    # Sắp xếp lại theo thứ tự gốc
    code_order = df_all_codes['code'].astype(str).tolist()
    code_map = {code: i for i, code in enumerate(code_order)}
    df_final_updated['order'] = df_final_updated['etf_code'].map(code_map)
    df_final_updated.sort_values(by=['order'], inplace=True)
    df_final_updated.drop(columns=['order'], inplace=True)
    
    print(f"✅ Hoàn tất hợp nhất và xử lý. Tổng số hàng cuối cùng: {len(df_final_updated)}")
    
    return df_final_updated

# --------------------------------------------------------
# HÀM 4: TẢI (UPSERT) DỮ LIỆU
# --------------------------------------------------------

def upsert_to_mysql(df: pd.DataFrame, table_name: str = 'etf_equity_holdings_train'):
    """Thực hiện UPSERT dựa trên Khóa DUY NHẤT (etf_code)."""
    
    cols_to_use = ['etf_code', 'price_earnings', 'price_book', 'price_sales', 'price_cashflow']
    
    # ⚠️ QUAN TRỌNG: BẢNG CẦN KHÓA UNIQUE TRÊN etf_code
    
    cnx = None
    cursor = None
    try:
        cnx = mysql.connector.connect(
            user='root',
            password='Phunghainam2004!',
            host='127.0.0.1', 
            database='train'
        )
        cursor = cnx.cursor()

        col_str = ", ".join([f"`{c}`" for c in cols_to_use])
        placeholder_str = ", ".join(["%s"] * len(cols_to_use))
        update_str = ", ".join([f"`{c}` = VALUES(`{c}`)" for c in cols_to_use])

        sql = f"""
        INSERT INTO `{table_name}` ({col_str})
        VALUES ({placeholder_str})
        ON DUPLICATE KEY UPDATE
        {update_str}
        """

        df_to_insert = df[cols_to_use]
        data_to_insert = [
            tuple(None if pd.isna(x) else x for x in row) 
            for row in df_to_insert.itertuples(index=False, name=None)
        ]
        
        cursor.executemany(sql, data_to_insert)
        cnx.commit()
        print(f"✅ Đã UPSERT {cursor.rowcount} dòng vào bảng `{table_name}` thành công!")

    except mysql.connector.Error as err:
        print(f"❌ Lỗi MySQL: {err}")
    finally:
        if cursor:
            cursor.close()
        if cnx and cnx.is_connected():
            cnx.close()
            
# --------------------------------------------------------
# HÀM CHÍNH ĐỂ GỌI TRONG AIRFLOW DAG
# --------------------------------------------------------
def extract_transform_load_valuation_data():
    """Hàm wrapper chạy toàn bộ luồng ETL và UPSERT."""
    
    # 1. Lấy tất cả codes
    codes = get_etf_codes()
    df_all_codes = pd.DataFrame(codes, columns=["code"])
    codes_to_process = df_all_codes['code'].astype(str).tolist()
    
    if not codes_to_process:
        print("Không có mã ETF nào để xử lý.")
        return pd.DataFrame()

    # 2. Crawl Lần 1 (Initial)
    df_result_initial, missing_codes = crawl_valuation_metrics(codes_to_process, INITIAL_WAIT_TIMEOUT)

    # 3. Crawl Lần 2 (Retry)
    if missing_codes:
        df_retry_result = crawl_valuation_metrics(missing_codes, RETRY_WAIT_TIMEOUT)
    else:
        df_retry_result = pd.DataFrame()

    # 4. Hợp nhất, Sắp xếp và Xử lý
    df_final_valuation_data = process_and_merge(df_result_initial, df_retry_result, df_all_codes)
    
    # 5. UPSERT vào DB
    if not df_final_valuation_data.empty:
        upsert_to_mysql(df_final_valuation_data)
        
    return df_final_valuation_data