#!/usr/bin/env python3
"""
事業單位違反職業安全衛生法令資料下載腳本

資料來源: https://data.gov.tw/dataset/155978
API: https://apiservice.mol.gov.tw/OdService/download/A17000000J-030466-h0a
"""

import urllib3
from pathlib import Path

import pandas as pd
import requests

# 關閉 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 設定
DOWNLOAD_URL = "https://apiservice.mol.gov.tw/OdService/download/A17000000J-030466-h0a"
BASE_DIR = Path(__file__).parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"


def download_csv() -> pd.DataFrame:
    """下載 CSV 資料"""
    print(f"下載資料: {DOWNLOAD_URL}")

    response = requests.get(DOWNLOAD_URL, timeout=120, verify=False)
    response.raise_for_status()

    # 解析 CSV
    from io import StringIO

    df = pd.read_csv(StringIO(response.text), encoding="utf-8")

    return df


def parse_year(date_val) -> int | None:
    """從 YYYYMMDD 格式解析年份"""
    if pd.isna(date_val):
        return None
    try:
        date_str = str(int(date_val))
        if len(date_str) >= 4:
            return int(date_str[:4])
    except (ValueError, TypeError):
        pass
    return None


def main():
    """主程式"""
    print("=" * 60)
    print("事業單位違反職業安全衛生法令資料下載")
    print("=" * 60)
    print()

    # 確保目錄存在
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # 下載資料
    df = download_csv()
    print(f"下載完成: {len(df)} 筆")
    print()

    # 檢查欄位
    print("欄位:")
    for col in df.columns:
        print(f"  - {col}")
    print()

    # 資料清理
    df = df.dropna(how="all")
    df = df.reset_index(drop=True)

    # 排序（按處分日期）
    if "處分日期" in df.columns:
        df = df.sort_values("處分日期", ascending=False)

    # 匯出合併 CSV
    output_file = PROCESSED_DIR / "違反職安法令_全部.csv"
    df.to_csv(output_file, encoding="utf-8-sig", index=False)

    print("=" * 60)
    print("完成!")
    print(f"輸出檔案: {output_file}")
    print(f"資料筆數: {len(df)}")
    print("=" * 60)

    # 輸出各年度檔案
    print()
    print("輸出各年度檔案...")

    if "處分日期" in df.columns:
        df["_year"] = df["處分日期"].apply(parse_year)

        year_stats = {}
        for year in sorted(df["_year"].dropna().unique()):
            year = int(year)
            year_df = df[df["_year"] == year].drop(columns=["_year"]).copy()
            year_file = PROCESSED_DIR / f"違反職安法令_{year}.csv"
            year_df.to_csv(year_file, encoding="utf-8-sig", index=False)
            year_stats[year] = len(year_df)
            print(f"  {year}: {len(year_df)} 筆 → {year_file.name}")

        # 日期不明
        unknown = df[df["_year"].isna()].drop(columns=["_year"])
        if len(unknown) > 0:
            unknown_file = PROCESSED_DIR / "違反職安法令_日期不明.csv"
            unknown.to_csv(unknown_file, encoding="utf-8-sig", index=False)
            print(f"  日期不明: {len(unknown)} 筆 → {unknown_file.name}")

    print()
    print("各年度統計:")
    for year, count in sorted(year_stats.items()):
        print(f"  {year}: {count} 筆")


if __name__ == "__main__":
    main()
