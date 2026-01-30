#!/usr/bin/env python3
"""
勞動部 WEBSERVICES API 資料下載腳本

資料來源: https://apiservice.mol.gov.tw/OdService/rest/datastore/{resourceID}
用於與 pacs.osha.gov.tw API 資料比對
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
BASE_URL = "https://apiservice.mol.gov.tw/OdService/rest/datastore"
BASE_DIR = Path(__file__).parent.parent
RAW_DIR = BASE_DIR / "data" / "raw_mol"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

# 各年度的 resourceID（從 data.gov.tw 網頁取得）
RESOURCE_IDS = {
    "107": "A17000000J-030238-3CY",  # 2018
    "108": "A17000000J-030238-7A6",  # 2019
    "109": "A17000000J-030238-s71",  # 2020
    "110": "A17000000J-030238-w1t",  # 2021
    "111": "A17000000J-030238-FEk",  # 2022
    "112": "A17000000J-030238-wfS",  # 2023
    "latest": "A17000000J-030238-kNH",  # 最新
}


def fetch_data(resource_id: str, limit: int = 1000, offset: int = 0) -> list:
    """
    從勞動部 WEBSERVICES API 取得資料

    Args:
        resource_id: 資源 ID
        limit: 每次取得筆數上限
        offset: 起始位置

    Returns:
        資料列表
    """
    url = f"{BASE_URL}/{resource_id}"
    params = {"limit": limit, "offset": offset}

    try:
        response = requests.get(url, params=params, timeout=30, verify=False)
        response.raise_for_status()
        data = response.json()

        if data.get("success") and "result" in data:
            return data["result"].get("records", [])
        return []
    except requests.RequestException as e:
        print(f"  錯誤: {e}")
        return []


def fetch_all_data(resource_id: str) -> list:
    """
    取得指定資源的所有資料（處理分頁）

    Args:
        resource_id: 資源 ID

    Returns:
        完整資料列表
    """
    all_data = []
    offset = 0
    limit = 1000

    while True:
        data = fetch_data(resource_id, limit=limit, offset=offset)
        if not data:
            break

        all_data.extend(data)

        # 如果取得的資料少於 limit，表示已經沒有更多資料
        if len(data) < limit:
            break

        offset += limit
        time.sleep(0.3)

    return all_data


def save_raw_json(data: list, year: str) -> None:
    """儲存原始 JSON 資料"""
    filename = RAW_DIR / f"mol_{year}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    """主程式"""
    print("=" * 60)
    print("勞動部 WEBSERVICES API 資料下載")
    print("=" * 60)
    print(f"來源: {BASE_URL}")
    print(f"年度數: {len(RESOURCE_IDS)}")
    print()

    # 確保目錄存在
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    all_data = []
    stats = {}

    for year, resource_id in RESOURCE_IDS.items():
        print(f"下載 {year} 年度資料...", end=" ")

        data = fetch_all_data(resource_id)
        count = len(data)
        stats[year] = count
        print(f"取得 {count} 筆")

        if data:
            save_raw_json(data, year)
            # 加入年度標記（如果沒有）
            for record in data:
                if "年度" not in record or not record["年度"]:
                    record["年度"] = year if year != "latest" else "最新"
            all_data.extend(data)

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

    # 欄位對應（MOL 的欄位名稱可能不同）
    column_mapping = {
        "場所-肇災處": "場所",
        "罹災人數": "罹災人數（數量）",
    }
    df = df.rename(columns=column_mapping)

    # 移除重複
    original_count = len(df)
    df = df.drop_duplicates(subset=["序號", "發生日期", "事業單位"], keep="first")
    duplicates_removed = original_count - len(df)
    if duplicates_removed > 0:
        print(f"  移除重複資料: {duplicates_removed} 筆")

    # 排序
    df = df.sort_values(["年度", "發生日期"], ascending=[False, False])
    df = df.reset_index(drop=True)
    df.index = df.index + 1
    df.index.name = "編號"

    # 匯出 CSV
    output_file = PROCESSED_DIR / "重大職業災害_MOL_WEBSERVICES.csv"
    df.to_csv(output_file, encoding="utf-8-sig")

    print()
    print("=" * 60)
    print("完成!")
    print(f"輸出檔案: {output_file}")
    print(f"資料筆數: {len(df)}")
    print("=" * 60)

    # 顯示統計
    print()
    print("各年度資料筆數:")
    for year, count in stats.items():
        print(f"  {year}: {count} 筆")


if __name__ == "__main__":
    main()
