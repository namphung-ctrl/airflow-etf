import requests
import pandas as pd
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import mysql.connector

# --- Cấu hình Cố định ---
API_URL = 'https://inav.ice.com/api/1/tse/iopv/table?type=etf&language=en'
HEADERS = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://inav.ice.com'}
URL_TEMPLATE = "https://inav.ice.com/pcf-download/{code}.csv"
MAX_WORKERS = 12
FINAL_DETAIL_COLUMNS = [
    "Code", "Name", "Exchange", "Currency", "Shares_Amount", "Stock_Price", 
    "Value", "etf_code", "Fund_Date", "Shares_Outstanding", "ISIN"
]

# --------------------------------------------------------
# HÀM 1: CRAWL VÀ PHÂN TÍCH DỮ LIỆU
# --------------------------------------------------------

def get_etf_codes():
    """Lấy danh sách các ETF code hợp lệ từ API."""
    try:
        r = requests.get(API_URL, headers=HEADERS, timeout=10)
        r.raise_for_status()
        rows = r.json().get("rows", [])
        return [row["code"] for row in rows if row.get("code", "")]
    except requests.RequestException as e:
        print(f"⚠️ Lỗi khi lấy ETF codes từ API: {e}")
        return []

def parse_etf_csv(etf_code):
    """Tải và phân tích file CSV cho một ETF code, tính toán Value và thêm metadata."""
    url = URL_TEMPLATE.format(code=etf_code)
    shares_out = np.nan
    fund_date = np.nan
    
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        text = r.text
        
        # 1. Đọc metadata (dòng 2)
        meta = pd.read_csv(StringIO(text), skiprows=1, nrows=1, header=None)
        
        if meta.shape[1] >= 4:  
            shares_out = meta.iloc[0, 3]
        if meta.shape[1] >= 5:  
            fund_date = meta.iloc[0, 4]

        # 2. Đọc holdings (bắt đầu từ dòng 4)
        df = pd.read_csv(StringIO(text), skiprows=3)

        # 3. Đổi tên cột và chuẩn hóa
        df.rename(columns={
            "Shares Amount": "Shares_Amount",
            "Stock Price": "Stock_Price"
        }, inplace=True, errors='ignore')

        if 'Shares_Amount' not in df.columns or 'Stock_Price' not in df.columns:
              raise KeyError("Cột 'Shares_Amount' hoặc 'Stock_Price' không tìm thấy trong file CSV.")
        
        # 4. Chuyển đổi numeric
        df["Shares_Amount"] = pd.to_numeric(df["Shares_Amount"], errors="coerce")
        df["Stock_Price"] = pd.to_numeric(df["Stock_Price"], errors="coerce")
        df = df.dropna(subset=["Shares_Amount", "Stock_Price"])

        # 5. Tính toán và thêm metadata
        if df.empty:
            return None
            
        df["Value"] = df["Shares_Amount"] * df["Stock_Price"]
        df["ETF_Code"] = str(etf_code)
        df["Shares_Outstanding"] = shares_out
        df["Fund_Date"] = fund_date
        
        # 6. Sắp xếp thứ tự cột
        return df

    except requests.exceptions.HTTPError as e:
        print(f"⚠️ Lỗi HTTP cho {etf_code}: Không tìm thấy file CSV ({e})")
        return None
    except KeyError as e:
        print(f"⚠️ Lỗi Key/Cột cho {etf_code}: Cột dữ liệu chính bị thiếu. {e}")
        return None
    except Exception as e:
        print(f"⚠️ Lỗi phân tích cú pháp/khác cho {etf_code}: {e}")
        return None

def extract_and_transform_all_assets():
    """Hàm chính: Lấy codes, tải, phân tích, hợp nhất và chuẩn hóa."""
    codes_list = get_etf_codes()
    if not codes_list:
        print("Không có ETF codes nào được tải xuống.")
        return pd.DataFrame(columns=FINAL_DETAIL_COLUMNS)

    results_map = {}
    print(f"Đang xử lý {len(codes_list)} ETF codes...")

    # Sử dụng đa luồng (Threading) để tăng tốc độ crawl
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(parse_etf_csv, code): code for code in codes_list}
        
        for i, future in enumerate(as_completed(futures), 1):
            code = futures[future]
            df = future.result()
            
            if df is not None:
                results_map[code] = df
            
            # Airflow logging sẽ thay thế in ra console
            # print(f"[{i}/{len(codes_list)}] Đã xử lý code {code}.", end='\r', flush=True)

    sorted_data = []
    for code in codes_list:
        if code in results_map:
            sorted_data.append(results_map[code])

    if not sorted_data:
        print("Không có DataFrame hợp lệ nào để hiển thị.")
        return pd.DataFrame(columns=FINAL_DETAIL_COLUMNS)
        
    df_final_detail = pd.concat(sorted_data, ignore_index=True)
    
    # --- Áp dụng XỬ LÝ SAU CRAWL ---
    df_final_detail.rename(columns={
        'Code' : 'code', 'Name' : 'name', 'Exchange' : 'exchange', 
        'Currency' : 'currency', 'Shares_Amount' : 'shares_amount', 
        'Stock_Price' : 'stock_price', 'Value' : 'value', 
        'Shares_Outstanding' : 'shares_outstanding', 'Fund_Date' : 'fund_date', 
        'ISIN' : 'isin', 'ETF_Code' : 'etf_code'
    }, inplace=True, errors='ignore')

    # Chuyển đổi kiểu dữ liệu
    df_final_detail['shares_amount'] = pd.to_numeric(df_final_detail['shares_amount'], errors='coerce').fillna(0).astype(np.int64)
    df_final_detail['shares_outstanding'] = pd.to_numeric(df_final_detail['shares_outstanding'], errors='coerce').fillna(0).astype(np.int64)
    df_final_detail['fund_date'] = pd.to_datetime(df_final_detail['fund_date'], format='%Y%m%d', errors='coerce')
    
    # Sắp xếp lại cột theo thứ tự cuối cùng
    df_final_detail = df_final_detail[
        [ "code" , "name", "exchange", "currency", "shares_amount", 
          "stock_price" , "value" , "etf_code" , "fund_date", "shares_outstanding" , "isin"]]
    
    print(f"\n✅ Xong. Đã tạo DataFrame df_final_detail với {len(df_final_detail)} holdings.")
    return df_final_detail

# --------------------------------------------------------
# HÀM 3: TẢI (INSERT) DỮ LIỆU
# --------------------------------------------------------

def insert_asset_detail_to_mysql(df: pd.DataFrame, table_name: str = 'etf_asset_detail_train'):
    """
    Thực hiện INSERT dữ liệu từ DataFrame vào MySQL. 
    
    """
    
    # 1. Định nghĩa các cột để INSERT
    cols_to_use = [
        "code", "name", "exchange", "currency", "shares_amount", 
        "stock_price", "value", "etf_code", "fund_date", 
        "shares_outstanding", "isin"
    ]
    
    cnx = None
    cursor = None
    try:
        # 2. Kết nối MySQL
        cnx = mysql.connector.connect(
            user='root',
            password='Phunghainam2004!',
            host='127.0.0.1', # Hoặc 'host.docker.internal'
            database='train'
        )
        cursor = cnx.cursor()

        # 3. Tạo cú pháp SQL cho INSERT đơn thuần
        col_str = ", ".join([f"`{c}`" for c in cols_to_use])
        placeholder_str = ", ".join(["%s"] * len(cols_to_use))

        sql = f"INSERT INTO `{table_name}` ({col_str}) VALUES ({placeholder_str})"

        # 4. Chuẩn bị dữ liệu và thực thi
        df_to_insert = df[cols_to_use].copy()
        
        # Xử lý các giá trị NaN/NaT thành None cho MySQL
        data_to_insert = [
            tuple(None if pd.isna(x) else x for x in row) 
            for row in df_to_insert.itertuples(index=False, name=None)
        ]
        
        # Thực thi INSERT hàng loạt
        cursor.executemany(sql, data_to_insert)
        
        # 5. Commit và Đóng kết nối
        cnx.commit()
        print(f"✅ Đã INSERT {len(data_to_insert)} bản ghi mới vào bảng `{table_name}`.")

    except mysql.connector.Error as err:
        print(f"❌ Lỗi MySQL trong quá trình INSERT: {err}")
    
    finally:
        if cursor:
            cursor.close()
        if cnx and cnx.is_connected():
            cnx.close()