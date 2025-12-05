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
INITIAL_WAIT_TIMEOUT = 20
RETRY_WAIT_TIMEOUT = 30

# --------------------------------------------------------
# HÀM 1: CRAWL LẦN 1 (TIMEOUT 20s)
# --------------------------------------------------------

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

def crawl_initial(codes_list):
    """Thực hiện crawl lần 1 với timeout 20s."""
    FULL_DATA_LIST = []
    MISSING_CODES = []
    
    # Cấu hình WebDriver (Cần đảm bảo ChromeDriver có trong môi trường Docker)
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # Khởi tạo WebDriver
    try:
        driver = webdriver.Chrome(options=chrome_options)
    except WebDriverException as e:
        print(f"❌ Lỗi WebDriver: ChromeDriver không thể khởi tạo. Đảm bảo nó có trong PATH.")
        return pd.DataFrame(), []

    total_codes = len(codes_list)
    print(f"\n--- Bắt đầu Crawl lần 1 ({total_codes} mã) ---")
    
    for index, base_code in enumerate(codes_list):
        base_code = str(base_code)
        full_code = f"{base_code}.T"
        url = f"https://finance.yahoo.com/quote/{full_code}/holdings/"
        
        try:
            driver.get(url)
            WebDriverWait(driver, INITIAL_WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.XPATH, "//h3[text()='Sector Weightings']"))
            )

            content_divs = driver.find_elements(
                By.XPATH, 
                "//div[contains(@class, 'content')]//a[contains(@data-ylk, 'elm:sector')]/ancestor::div[contains(@class, 'content')]"
            )
            
            if not content_divs:
                MISSING_CODES.append(base_code)
                continue
                
            for div in content_divs:
                try:
                    sector_weighting = div.find_element(By.XPATH, ".//a").text
                    percent = div.find_element(By.XPATH, ".//span[contains(@class, 'data')]").text
                    
                    FULL_DATA_LIST.append({
                        'code': base_code,
                        'sector_weightings': sector_weighting,
                        'percent': percent
                    })
                except NoSuchElementException:
                    continue
        
        except TimeoutException:
            MISSING_CODES.append(base_code)
        except Exception:
            MISSING_CODES.append(base_code)
            
        time.sleep(random.uniform(1, 2))
        
    driver.quit()
    df_result = pd.DataFrame(FULL_DATA_LIST)
    return df_result, MISSING_CODES

# --------------------------------------------------------
# HÀM 2: CRAWL LẦN 2 (RETRY, TIMEOUT 30s)
# --------------------------------------------------------

def crawl_retry(missing_codes):
    """Thực hiện crawl lần 2 (Retry) với timeout 30s."""
    data_list_retry = []
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=chrome_options)
    
    total_codes = len(missing_codes)
    print(f"\n--- Bắt đầu Retry ({total_codes} mã, Timeout: {RETRY_WAIT_TIMEOUT}s) ---")
    
    for base_code in missing_codes:
        full_code = f"{base_code}.T"
        url = f"https://finance.yahoo.com/quote/{full_code}/holdings/"
        
        try:
            driver.get(url)
            WebDriverWait(driver, RETRY_WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.XPATH, "//h3[text()='Sector Weightings']"))
            )

            content_divs = driver.find_elements(
                By.XPATH, 
                "//div[contains(@class, 'content')]//a[contains(@data-ylk, 'elm:sector')]/ancestor::div[contains(@class, 'content')]"
            )
            
            if not content_divs:
                continue
                
            for div in content_divs:
                try:
                    sector_weighting = div.find_element(By.XPATH, ".//a").text
                    percent = div.find_element(By.XPATH, ".//span[contains(@class, 'data')]").text
                    
                    data_list_retry.append({
                        'code': base_code,
                        'sector_weightings': sector_weighting,
                        'percent': percent
                    })
                except NoSuchElementException:
                    continue
        
        except TimeoutException:
            pass
        except Exception:
            pass
            
        time.sleep(1)
        
    driver.quit()
    
    df_retry_result = pd.DataFrame(data_list_retry)
    return df_retry_result

# --------------------------------------------------------
# HÀM 3: HỢP NHẤT, SẮP XẾP VÀ XỬ LÝ DỮ LIỆU
# --------------------------------------------------------

def merge_sort_and_transform(df_result, df_retry_result, df_all_codes):
    """Hợp nhất, sắp xếp và xử lý cuối cùng (đổi tên/kiểu dữ liệu)."""
    
    df_final = pd.concat([df_result, df_retry_result], ignore_index=True)

    # Sắp xếp theo thứ tự mã gốc
    code_order = df_all_codes['code'].astype(str).tolist()
    code_map = {code: i for i, code in enumerate(code_order)}

    df_final['order'] = df_final['code'].map(code_map)
    df_final_sorted = df_final.sort_values(by=['order']).reset_index(drop=True)
    df_final_sorted = df_final_sorted.drop(columns=['order'])
    
    # Xử lý cuối cùng (Xóa % và đổi tên cột)
    df_final_sorted['percent'] = df_final_sorted['percent'].astype(str).str.replace("%","", regex=False).astype(float)
    df_final_sorted.rename(columns={'code': 'etf_code', 'sector_weightings': 'sector'}, inplace=True)
    
    print(f"✅ Hoàn tất xử lý. Tổng số hàng cuối cùng: {len(df_final_sorted)}")
    
    return df_final_sorted[['etf_code', 'sector', 'percent']]

# --------------------------------------------------------
# HÀM 4: TẢI (UPSERT) DỮ LIỆU
# --------------------------------------------------------

def upsert_to_mysql(df: pd.DataFrame, table_name: str = 'etf_sectors_train'):
    """Thực hiện UPSERT dựa trên Khóa Hợp Thành (etf_code, sector)."""
    
    cols_to_use = ['etf_code', 'sector', 'percent']
    
    # ⚠️ QUAN TRỌNG: BẢNG CẦN KHÓA UNIQUE TRÊN (etf_code, sector)
    
    cnx = None
    cursor = None
    try:
        # 1. Kết nối MySQL (Nên dùng Airflow Hook)
        cnx = mysql.connector.connect(
            user='root',
            password='Phunghainam2004!',
            host='127.0.0.1', # Thay bằng host Airflow Worker có thể truy cập
            database='train'
        )
        cursor = cnx.cursor()

        # 2. Tạo cú pháp SQL cho UPSERT
        col_str = ", ".join([f"`{c}`" for c in cols_to_use])
        placeholder_str = ", ".join(["%s"] * len(cols_to_use))
        
        # Chỉ cập nhật cột 'percent'
        sql = f"""
        INSERT INTO `{table_name}` ({col_str})
        VALUES ({placeholder_str})
        ON DUPLICATE KEY UPDATE
        `percent` = VALUES(`percent`)
        """

        # 3. Chuẩn bị dữ liệu và thực thi
        df_to_insert = df[cols_to_use]
        data_to_insert = [
            tuple(None if pd.isna(x) else x for x in row) 
            for row in df_to_insert.itertuples(index=False, name=None)
        ]
        
        cursor.executemany(sql, data_to_insert)
        
        # 4. Commit và Đóng kết nối
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
def extract_transform_load_sector_data():
    """Hàm wrapper chạy toàn bộ luồng ETL và UPSERT."""
    # 1. Lấy tất cả codes
    codes = get_etf_codes()
    df_all_codes = pd.DataFrame(codes, columns=["code"])
    codes_to_process = df_all_codes['code'].astype(str).tolist()
    
    if not codes_to_process:
        print("Không có mã ETF nào để xử lý.")
        return pd.DataFrame()

    # 2. Crawl Lần 1
    df_result, missing_codes_initial = crawl_initial(codes_to_process)

    # 3. Crawl Lần 2 (Retry)
    df_retry_result = crawl_retry(missing_codes_initial)

    # 4. Hợp nhất, Sắp xếp và Xử lý
    df_final_sector_data = merge_sort_and_transform(df_result, df_retry_result, df_all_codes)
    
    # 5. UPSERT vào DB
    if not df_final_sector_data.empty:
        upsert_to_mysql(df_final_sector_data)
        
    return df_final_sector_data