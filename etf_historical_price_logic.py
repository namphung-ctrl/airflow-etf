import pandas as pd
import requests
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import mysql.connector
from io import StringIO
from datetime import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook

# --- Cấu hình Cố định ---
API_URL = 'https://inav.ice.com/api/1/tse/iopv/table?type=etf&language=en'
HEADERS = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://inav.ice.com'}
MAX_WORKERS = 12
CURRENT_YEAR = 2025 # Năm cần gán (nên tự động lấy năm hiện tại)

# --------------------------------------------------------
# HÀM 1A: CRAWL THỜI GIAN VÀ VOLUME (Từ ICE API)
# --------------------------------------------------------

def get_etf_code_time():
    """Lấy danh sách code, thời gian và volume từ ICE API."""
    try:
        r = requests.get(API_URL, headers=HEADERS, timeout=10)
        r.raise_for_status()
        rows = r.json().get("rows", [])
        
        data = []
        for row in rows:
            code = row.get("code")
            time_str = row.get("time") # Ví dụ: "12/02 11:27"
            volume = row.get("volume")
            if code and time_str:
                data.append({"code": code, "time": time_str, "volume": volume})
        
        return pd.DataFrame(data)
    except requests.RequestException as e:
        print(f"⚠️ Lỗi khi lấy dữ liệu ETF từ API: {e}")
        return pd.DataFrame()

# --------------------------------------------------------
# HÀM 1B: CRAWL OHLC VÀ CURRENCY (Từ YFINANCE)
# --------------------------------------------------------

def fetch_data_yf(etf_code):
    """
    Tải OHLC (Previous Close) và Currency, xử lý lỗi thiếu dữ liệu lịch sử.
    """
    ticker = f"{etf_code}.T"
    result = {"code": etf_code, "open": None, "high": None, "low": None, "close": None, "currency": None}
    
    try:
        stock = yf.Ticker(ticker)
        
        # 1. Lấy Currency
        info = stock.info
        result["currency"] = info.get("currency")
        
        # 2. Lấy OHLC (dữ liệu 2 ngày để có Previous Close)
        hist = stock.history(period="2d", interval="1d")
        
        if len(hist) >= 2:
            today_data = hist.iloc[-1]
            previous_day_data = hist.iloc[-2]

            result["open"] = today_data["Open"]
            result["high"] = today_data["High"]
            result["low"] = today_data["Low"]
            result["close"] = previous_day_data["Close"] # Previous Close
            
        elif len(hist) == 1:
            last_available_data = hist.iloc[-1]
            result["open"] = last_available_data["Open"]
            result["high"] = last_available_data["High"]
            result["low"] = last_available_data["Low"]
            result["close"] = last_available_data["Close"] # Last Available Close

    except Exception as e:
        print(f"YF Error for {etf_code}: {e}")
        pass
        
    return result

# --------------------------------------------------------
# HÀM 2: XỬ LÝ VÀ CHUẨN HÓA DỮ LIỆU
# --------------------------------------------------------

def transform_price_data():
    """
    Thực hiện crawl song song, hợp nhất và xử lý (đổi tên/format) dữ liệu.
    """
    df_1 = get_etf_code_time()
    if df_1.empty:
        return pd.DataFrame()

    codes_list = df_1['code'].tolist()
    total_codes = len(codes_list)
    
    print(f"\n1. Bắt đầu tải song song OHLC/Currency cho {total_codes} codes...")

    final_results = []
    
    # Tải song song
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_data_yf, code): code for code in codes_list}

        
        for i, future in enumerate(as_completed(futures)):
            final_results.append(future.result())

            # OPTIONAL: Ghi log nhẹ nhàng cho Airflow
            if (i + 1) % 50 == 0: # Ghi log mỗi 50 mã
                print(f"[{i + 1}/{total_codes}] Đã xử lý {i + 1} mã.")

    # Hợp nhất dữ liệu
    df_new_data = pd.DataFrame(final_results)
    df_price_final = df_1.merge(df_new_data, on='code', how='left')

    # --- Bắt đầu XỬ LÝ (Áp dụng logic từ HÀM 2 của bạn) ---
    
    # 1. Chuyển đổi cột giá trị sang số (giữ nguyên None/NaN)
    for col in ['open', 'high', 'low', 'close']:
        df_price_final[col] = pd.to_numeric(df_price_final[col], errors='coerce')
    
    # 2. Xử lý cột time/date
    # * Tự động lấy năm hiện tại
    current_year = datetime.now().year
    
    # * Thêm năm vào chuỗi thời gian gốc
    df_price_final['time_with_year'] = df_price_final['time'].astype(str) + '/' + str(current_year)
    
    # * Chuyển đổi sang datetime
    df_price_final['date'] = pd.to_datetime(
        df_price_final['time_with_year'], 
        format='%m/%d %H:%M/%Y', 
        errors='coerce'
    )
    
    # 3. Xử lý volume
    df_price_final['volume'] = df_price_final['volume'].astype(str).str.replace(',', '', regex=False)
    df_price_final['volume'] = pd.to_numeric(df_price_final['volume'], errors='coerce')
    
    # 4. Đổi tên và sắp xếp cột cuối cùng
    df_price_final.rename(columns={'code': 'etf_code'}, inplace=True)
    
    # * Loại bỏ các cột trung gian
    df_price_final.drop(columns=['time', 'time_with_year'], inplace=True) 

    final_columns = ['date', 'etf_code', 'open', 'high', 'low', 'close', 'volume', 'currency']
    
    # Chỉ giữ lại các cột cần thiết, fill NaN nếu thiếu
    for col in final_columns:
        if col not in df_price_final.columns:
            df_price_final[col] = np.nan

    return df_price_final[final_columns].copy()

# --------------------------------------------------------
# HÀM 3: TẢI (UPSERT) DỮ LIỆU VÀO MYSQL
# --------------------------------------------------------

def upsert_price_to_mysql(df: pd.DataFrame, table_name: str = 'etf_historical_price_train'):
    """
    Thực hiện UPSERT dữ liệu từ DataFrame vào MySQL.
    Khóa UNIQUE giả định là (date, etf_code).

    """
    # 1. Khởi tạo Hook bằng Conn Id đã tạo trong Airflow UI
    mysql_hook = MySqlHook(mysql_conn_id='admin')   
    # ⚠️ QUAN TRỌNG: BẢNG CẦN KHÓA UNIQUE TRÊN (date, etf_code) ĐỂ UPSERT
    
    cols_to_use = [
        'date', 'etf_code', 'open', 'high', 'low', 'close', 'volume', 'currency'
    ]
    
    cnx = None
    cursor = None
    try:
        # 1. Kết nối MySQL (Nên dùng Airflow Hook)
        cnx = mysql.connector.connect(
            user='root',
            password='Phunghainam2004!',
            host='127.0.0.1', # Hoặc 'host.docker.internal'
            database='train'
        )
        cursor = cnx.cursor()

        # 2. Tạo cú pháp SQL cho UPSERT
        col_str = ", ".join([f"`{c}`" for c in cols_to_use])
        placeholder_str = ", ".join(["%s"] * len(cols_to_use))
        update_str = ", ".join([f"`{c}` = VALUES(`{c}`)" for c in cols_to_use])

        sql = f"""
        INSERT INTO `{table_name}` ({col_str})
        VALUES ({placeholder_str})
        ON DUPLICATE KEY UPDATE
        {update_str}
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
        print(f"✅ Đã UPSERT {len(data_to_insert)} bản ghi vào bảng `{table_name}`.")

    except mysql.connector.Error as err:
        print(f"❌ Lỗi MySQL trong quá trình UPSERT: {err}")
    
    finally:
        if cursor:
            cursor.close()
        if cnx and cnx.is_connected():
            cnx.close()