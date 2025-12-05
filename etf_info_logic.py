import pandas as pd
import requests
from bs4 import BeautifulSoup
import yfinance as yf
import mysql.connector

def crawl_jpx_data():
    """Crawl dữ liệu ETF từ JPX, xử lý sơ bộ và trả về DataFrame chưa có Yield."""
    
    url = 'https://www.jpx.co.jp/english/equities/products/etfs/issues/01.html'
    crawl = requests.get(url)
    soup = BeautifulSoup(crawl.text,'html.parser')
    
    def get_all_codes(soup):
        table = soup.find("table")
        codes = []
        if table:
             for tr in table.find_all("tr")[1:]:
                 cols = tr.find_all("td")
                 if len(cols) >= 3:  
                     codes.append(cols[2].get_text(strip=True))
        return codes
        
    def parse_etf_table(soup):
        table = soup.find("table")
        columns = ["Listing Date", "Index", "Code", "Fund Name",
                   "Management Company", "Trading Unit", "Trust Fee", "Tags"]
        if table is None:
            return pd.DataFrame(columns=columns)
        
        rows = []
        for tr in table.find_all("tr")[1:]:
            cols = tr.find_all("td")
            if len(cols) < 7:
                continue
            fund_td = cols[3]
            fund_name = fund_td.find(text=True, recursive=False).strip()
            tags = [a.get_text(strip=True) for a in fund_td.find_all("a")]

            row = [
                cols[0].get_text(" ", strip=True), cols[1].get_text(" ", strip=True),
                cols[2].get_text(" ", strip=True), fund_name,
                cols[4].get_text(" ", strip=True), cols[5].get_text(" ", strip=True),
                cols[6].get_text(" ", strip=True), ", ".join(tags)
            ]
            rows.append(row)
        return pd.DataFrame(rows, columns=columns)
        
    all_code_order = get_all_codes(soup)
    category_links = []
    for ul in soup.select("ul.input-btn-list.column6-boxIn"):
         for a in ul.select("a"):
             href = a.get("href", "").strip()
             if "/english/equities/products/etfs/issues/01-" in href:
                 category_links.append((a.get_text(strip=True), "https://www.jpx.co.jp" + href))
    
    final_df = pd.DataFrame()
    for cat_name, cat_url in category_links:
        html = requests.get(cat_url).text
        soup_cat = BeautifulSoup(html, "html.parser")
        df_cat = parse_etf_table(soup_cat)
        if not df_cat.empty:
            df_cat["Category"] = cat_name
            final_df = pd.concat([final_df, df_cat], ignore_index=True)

    # Sắp xếp và đổi tên cột (Tất cả logic tiền xử lý)
    final_df["Code"] = pd.Categorical(final_df["Code"], categories=all_code_order, ordered=True)
    final_df = final_df.sort_values("Code").reset_index(drop=True)

    final_df = final_df.rename(columns={'Listing Date': 'listing_date', 'Index': 'index_name', 
                                        'Code': 'etf_code', 'Fund Name': 'etfn_name', 
                                        'Management Company': 'management_company', 
                                        'Trading Unit': 'trading_unit', 'Trust Fee': 'trust_fee', 
                                        'Category': 'etf_type', 'yield': 'distribution_yield'})
    
    final_df['listing_date'] = pd.to_datetime(final_df['listing_date'], errors='coerce', format='mixed')
    final_df['etf_code'] = final_df['etf_code'].astype(dtype=object)
    
    return final_df.drop(columns=['Tags']) # Loại bỏ cột Tags nếu không dùng trong DB

def get_yield(ticker):
    """Lấy yield cho một ticker."""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        if "dividendYield" in info and info["dividendYield"] is not None:
            return info["dividendYield"]
        return None
    except:
        return None

def add_yield_to_df(df: pd.DataFrame):
    """Thêm cột distribution_yield vào DataFrame."""
    yields = [get_yield(f"{code}.T") for code in df["etf_code"]] 
    df["distribution_yield"] = yields
    return df

def upsert_to_mysql(df: pd.DataFrame, table_name: str = 'etf_info_train'):
    """Thực hiện UPSERT dữ liệu vào MySQL. (Sử dụng code từ cell 7)"""
    
    
    cols_to_use = [
        'listing_date', 'etf_code', 'etfn_name', 'index_name', 
        'trading_unit', 'management_company', 'trust_fee', 
        'distribution_yield', 'etf_type'
    ]
    
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
        print(f"✅ Đã UPSERT {len(data_to_insert)} bản ghi vào bảng `{table_name}`.")

    except mysql.connector.Error as err:
        print(f"❌ Lỗi MySQL trong quá trình UPSERT: {err}")
    
    finally:
        if cursor:
            cursor.close()
        if cnx and cnx.is_connected():
            cnx.close()