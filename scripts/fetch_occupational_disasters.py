#!/usr/bin/env python3
"""
重大職業災害公開網資料下載腳本

資料來源: https://pacs.osha.gov.tw/api/v1/getdangerocupation
API 限制: 單次查詢上限 200 筆
策略: 按半年分批查詢，確保完整取得所有資料
"""

import json
import time
import urllib3
from pathlib import Path

import pandas as pd
import requests

# 關閉 SSL 警告（台灣政府網站憑證設定問題）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 設定
API_URL = "https://pacs.osha.gov.tw/api/v1/getdangerocupation"
BASE_DIR = Path(__file__).parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

# 查詢區間（半年為單位，2018-2025）
DATE_RANGES = [
    ("20180101", "20180630"),
    ("20180701", "20181231"),
    ("20190101", "20190630"),
    ("20190701", "20191231"),
    ("20200101", "20200630"),
    ("20200701", "20201231"),
    ("20210101", "20210630"),
    ("20210701", "20211231"),
    ("20220101", "20220630"),
    ("20220701", "20221231"),
    ("20230101", "20230630"),
    ("20230701", "20231231"),
    ("20240101", "20240630"),
    ("20240701", "20241231"),
    ("20250101", "20250630"),
]


def fetch_data(start_date: str, end_date: str) -> list:
    """
    從 API 取得指定日期範圍的資料

    Args:
        start_date: 起始日期 (YYYYMMDD)
        end_date: 結束日期 (YYYYMMDD)

    Returns:
        資料列表
    """
    params = {
        "info_PostdateS": start_date,
        "info_PostdateE": end_date,
    }

    try:
        response = requests.get(API_URL, params=params, timeout=30, verify=False)
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else []
    except requests.RequestException as e:
        print(f"  錯誤: {e}")
        return []


def save_raw_json(data: list, start_date: str, end_date: str) -> None:
    """儲存原始 JSON 資料"""
    filename = RAW_DIR / f"disasters_{start_date}_{end_date}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    """主程式"""
    print("=" * 60)
    print("重大職業災害公開網資料下載")
    print("=" * 60)
    print(f"API: {API_URL}")
    print(f"查詢區間數: {len(DATE_RANGES)}")
    print()

    # 確保目錄存在
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    all_data = []

    for i, (start_date, end_date) in enumerate(DATE_RANGES, 1):
        print(f"[{i}/{len(DATE_RANGES)}] 查詢 {start_date} ~ {end_date}...", end=" ")

        data = fetch_data(start_date, end_date)
        count = len(data)
        print(f"取得 {count} 筆")

        if data:
            save_raw_json(data, start_date, end_date)
            all_data.extend(data)

        # 避免過度請求
        time.sleep(0.5)

    print()
    print(f"總共取得: {len(all_data)} 筆資料")

    if not all_data:
        print("未取得任何資料，結束程式")
        return

    # 轉換為 DataFrame
    df = pd.DataFrame(all_data)

    # 資料清理
    print()
    print("資料清理中...")

    # 移除重複（根據序號和發生日期）
    original_count = len(df)
    df = df.drop_duplicates(subset=["序號", "發生日期", "事業單位"], keep="first")
    duplicates_removed = original_count - len(df)
    if duplicates_removed > 0:
        print(f"  移除重複資料: {duplicates_removed} 筆")

    # 轉換日期格式
    if "發生日期" in df.columns:
        df["發生日期"] = pd.to_datetime(df["發生日期"], format="%Y%m%d", errors="coerce")
        df["發生日期"] = df["發生日期"].dt.strftime("%Y-%m-%d")

    # 排序（按年度和發生日期）
    df = df.sort_values(["年度", "發生日期"], ascending=[False, False])

    # 重新編號
    df = df.reset_index(drop=True)
    df.index = df.index + 1
    df.index.name = "編號"

    # 匯出 CSV
    output_file = PROCESSED_DIR / "重大職業災害_2018至今.csv"
    df.to_csv(output_file, encoding="utf-8-sig")

    print()
    print("=" * 60)
    print("完成!")
    print(f"輸出檔案: {output_file}")
    print(f"資料筆數: {len(df)}")
    print(f"資料期間: {df['年度'].min()} ~ {df['年度'].max()}")
    print("=" * 60)

    # 顯示統計
    print()
    print("各年度資料筆數:")
    year_counts = df["年度"].value_counts().sort_index()
    for year, count in year_counts.items():
        print(f"  {year}: {count} 筆")


if __name__ == "__main__":
    main()
