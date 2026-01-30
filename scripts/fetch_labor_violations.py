#!/usr/bin/env python3
"""
違反勞動法令事業單位查詢系統 - 資料下載腳本

資料來源: https://announcement.mol.gov.tw/
策略: 遍歷所有縣市/單位，下載 CSV 格式資料
"""

import io
import time
import zipfile
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

# 設定
BASE_URL = "https://announcement.mol.gov.tw"
DOWNLOAD_URL = f"{BASE_URL}/Download/"
BASE_DIR = Path(__file__).parent.parent
RAW_DIR = BASE_DIR / "data" / "raw_violations"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

# 縣市/單位代碼
CITY_CODES = {
    "台北市": "63",
    "新北市": "65",
    "桃園市": "68",
    "台中市": "66",
    "台南市": "67",
    "高雄市": "64",
    "宜蘭縣": "02",
    "新竹縣": "04",
    "苗栗縣": "05",
    "彰化縣": "07",
    "南投縣": "08",
    "雲林縣": "09",
    "嘉義縣": "10",
    "屏東縣": "13",
    "台東縣": "14",
    "花蓮縣": "15",
    "澎湖縣": "16",
    "基隆市": "17",
    "新竹市": "25",
    "嘉義市": "26",
    "金門縣": "23",
    "連江縣": "24",
    "產業園區管理局": "96",
    "新竹科學園區": "97",
    "中部科學園區": "92",
    "南部科學園區": "95",
    "勞動部勞工保險局": "BL",
    "勞動部勞動基金運用局": "BA",
    "職業安全衛生署": "CA",
    "勞動部": "00",
}


def get_csrf_token(session: requests.Session) -> str:
    """從首頁取得 CSRF token"""
    response = session.get(BASE_URL, verify=False)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    csrf_input = soup.find("input", {"name": "_csrf_token"})

    if csrf_input and csrf_input.get("value"):
        return csrf_input["value"]

    raise ValueError("無法取得 CSRF token")


def download_city_data(
    session: requests.Session, csrf_token: str, city_name: str, city_code: str
) -> bytes | None:
    """下載指定縣市的資料"""
    data = {
        "_csrf_token": csrf_token,
        "CITYNO": city_code,
        "UNITNAME": "",
        "DOCstartDate": "",
        "DOCEndDate": "",
        "REGNUMBER": "",
        "REGNO": "",
        "downloadType": "3",  # CSV
        "sortName3": "",
        "sortName1": "",
        "sortName2": "",
        "Page3": "1",
        "Page1": "1",
        "Page2": "1",
    }

    try:
        response = session.post(DOWNLOAD_URL, data=data, verify=False, timeout=120)
        response.raise_for_status()

        # 檢查是否為 ZIP 檔案
        content_type = response.headers.get("content-type", "")
        if "zip" in content_type or response.content[:4] == b"PK\x03\x04":
            return response.content
        else:
            print(f"  警告: {city_name} 回傳非 ZIP 格式")
            return None

    except requests.RequestException as e:
        print(f"  錯誤: {e}")
        return None


def extract_csvs_from_zip(zip_content: bytes, city_name: str) -> list[pd.DataFrame]:
    """從 ZIP 檔案中提取 CSV"""
    dfs = []

    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            for filename in zf.namelist():
                if filename.endswith(".csv"):
                    with zf.open(filename) as f:
                        content = f.read()

                        # 嘗試不同編碼
                        for encoding in ["utf-8-sig", "utf-8", "big5", "cp950"]:
                            try:
                                # 預處理：移除第一行標題，並修復跨行的欄位名稱
                                text = content.decode(encoding)
                                lines = text.split("\n")

                                # 跳過 "違反雇主清冊" 標題行
                                if lines[0].strip().replace('"', "") == "違反雇主清冊":
                                    lines = lines[1:]

                                # 修復跨行的欄位名稱（欄位名稱內含換行符）
                                # 標準欄位：編號,縣市／單位別,公告日期,事業單位名稱(負責人)自然人姓名,處分日期,...
                                fixed_lines = []
                                i = 0
                                while i < len(lines):
                                    line = lines[i]
                                    # 檢查是否是不完整的行（引號未閉合）
                                    quote_count = line.count('"')
                                    while quote_count % 2 != 0 and i + 1 < len(lines):
                                        i += 1
                                        line = line + lines[i]
                                        quote_count = line.count('"')
                                    fixed_lines.append(line)
                                    i += 1

                                # 移除尾部多餘的逗號（資料行可能比標題多一欄）
                                fixed_lines = [
                                    line.rstrip(",") for line in fixed_lines
                                ]

                                fixed_text = "\n".join(fixed_lines)

                                df = pd.read_csv(
                                    io.StringIO(fixed_text),
                                    on_bad_lines="skip",
                                )

                                # 確認有資料
                                if len(df) > 0 and len(df.columns) > 1:
                                    # 清理欄位名稱中的換行符
                                    df.columns = [
                                        col.replace("\n", "").replace("\r", "")
                                        for col in df.columns
                                    ]
                                    df["來源縣市"] = city_name
                                    df["來源檔案"] = filename

                                    # 判斷分類（A=勞基法等, B=就服法等, C=勞退等）
                                    if "-A-" in filename:
                                        df["法規分類"] = "勞動基準法等"
                                    elif "-B-" in filename:
                                        df["法規分類"] = "就業服務法等"
                                    elif "-C-" in filename:
                                        df["法規分類"] = "勞工退休金條例等"

                                    dfs.append(df)
                                break
                            except (UnicodeDecodeError, pd.errors.ParserError):
                                continue
    except zipfile.BadZipFile:
        print(f"  警告: {city_name} ZIP 檔案損壞")

    return dfs


def main():
    """主程式"""
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    print("=" * 60)
    print("違反勞動法令事業單位查詢系統 - 資料下載")
    print("=" * 60)
    print(f"來源: {BASE_URL}")
    print(f"縣市/單位數: {len(CITY_CODES)}")
    print()

    # 確保目錄存在
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # 建立 session
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
    )

    # 取得 CSRF token
    print("取得 CSRF token...", end=" ")
    csrf_token = get_csrf_token(session)
    print("完成")
    print()

    all_dfs = []
    stats = {}

    for i, (city_name, city_code) in enumerate(CITY_CODES.items(), 1):
        print(f"[{i}/{len(CITY_CODES)}] 下載 {city_name} ({city_code})...", end=" ")

        zip_content = download_city_data(session, csrf_token, city_name, city_code)

        if zip_content:
            # 儲存原始 ZIP
            zip_path = RAW_DIR / f"violations_{city_code}_{city_name}.zip"
            with open(zip_path, "wb") as f:
                f.write(zip_content)

            # 提取 CSV
            dfs = extract_csvs_from_zip(zip_content, city_name)
            total_rows = sum(len(df) for df in dfs)
            stats[city_name] = total_rows
            all_dfs.extend(dfs)
            print(f"取得 {total_rows} 筆 ({len(dfs)} 個 CSV)")
        else:
            stats[city_name] = 0
            print("無資料")

        # 避免過度請求
        time.sleep(1)

    print()
    print(f"總共取得: {sum(len(df) for df in all_dfs)} 筆資料")

    if not all_dfs:
        print("未取得任何資料，結束程式")
        return

    # 合併所有 DataFrame
    print()
    print("合併資料中...")

    # 分類合併（因為不同類別可能有不同欄位）
    # 根據欄位特徵分類
    df_all = pd.concat(all_dfs, ignore_index=True)

    # 資料清理
    # 移除完全空白的列
    df_all = df_all.dropna(how="all")

    # 排序
    if "處分日期" in df_all.columns:
        df_all = df_all.sort_values("處分日期", ascending=False)

    df_all = df_all.reset_index(drop=True)

    # 匯出合併 CSV
    output_file = PROCESSED_DIR / "違反勞動法令_全台彙整.csv"
    df_all.to_csv(output_file, encoding="utf-8-sig", index=False)

    print()
    print("=" * 60)
    print("完成!")
    print(f"輸出檔案: {output_file}")
    print(f"資料筆數: {len(df_all)}")
    print("=" * 60)

    # 輸出各年度檔案
    print()
    print("輸出各年度檔案...")
    if "處分日期" in df_all.columns:
        # 解析民國年 (格式: 114/12/26)
        def parse_roc_year(date_str):
            if pd.isna(date_str):
                return None
            try:
                parts = str(date_str).strip().replace("-", "/").split("/")
                if len(parts) >= 1:
                    roc_year = int(parts[0])
                    if roc_year < 200:
                        return roc_year + 1911
                    return roc_year
            except (ValueError, IndexError):
                pass
            return None

        df_all["_year"] = df_all["處分日期"].apply(parse_roc_year)

        year_stats = {}
        for year in sorted(df_all["_year"].dropna().unique()):
            year = int(year)
            year_df = df_all[df_all["_year"] == year].drop(columns=["_year"]).copy()
            year_file = PROCESSED_DIR / f"違反勞動法令_{year}.csv"
            year_df.to_csv(year_file, encoding="utf-8-sig", index=False)
            year_stats[year] = len(year_df)
            print(f"  {year}: {len(year_df)} 筆 → {year_file.name}")

        # 日期不明的資料
        unknown_df = df_all[df_all["_year"].isna()].drop(columns=["_year"])
        if len(unknown_df) > 0:
            unknown_file = PROCESSED_DIR / "違反勞動法令_日期不明.csv"
            unknown_df.to_csv(unknown_file, encoding="utf-8-sig", index=False)
            print(f"  日期不明: {len(unknown_df)} 筆 → {unknown_file.name}")

        # 移除暫時欄位
        df_all = df_all.drop(columns=["_year"])

    # 顯示縣市統計
    print()
    print("各縣市/單位資料筆數:")
    for city, count in sorted(stats.items(), key=lambda x: -x[1]):
        if count > 0:
            print(f"  {city}: {count} 筆")


if __name__ == "__main__":
    main()
