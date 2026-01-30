#!/usr/bin/env python3
"""
比對兩個資料來源的差異

來源 1: pacs.osha.gov.tw API
來源 2: 勞動部 WEBSERVICES API
"""

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"


def load_data():
    """載入兩個資料來源"""
    pacs_file = PROCESSED_DIR / "重大職業災害_2018至今.csv"
    mol_file = PROCESSED_DIR / "重大職業災害_MOL_WEBSERVICES.csv"

    pacs_df = pd.read_csv(pacs_file, encoding="utf-8-sig")
    mol_df = pd.read_csv(mol_file, encoding="utf-8-sig")

    return pacs_df, mol_df


def normalize_year(year_val):
    """將年度統一為西元年"""
    if pd.isna(year_val):
        return None
    year_str = str(year_val).strip()
    if year_str in ["最新", "latest"]:
        return 2025
    try:
        year_int = int(float(year_str))
        # 民國年轉西元年
        if year_int < 200:
            return year_int + 1911
        return year_int
    except ValueError:
        return None


def compare_counts(pacs_df: pd.DataFrame, mol_df: pd.DataFrame):
    """比較各年度筆數"""
    print("=" * 70)
    print("各年度資料筆數比較")
    print("=" * 70)
    print(f"{'年度':<15} {'pacs API':<12} {'MOL WEBSERVICES':<18} {'差異':<10}")
    print("-" * 70)

    # 標準化年度
    pacs_df = pacs_df.copy()
    mol_df = mol_df.copy()
    pacs_df["年度_標準"] = pacs_df["年度"].apply(normalize_year)
    mol_df["年度_標準"] = mol_df["年度"].apply(normalize_year)

    pacs_counts = pacs_df.groupby("年度_標準").size()
    mol_counts = mol_df.groupby("年度_標準").size()

    all_years = sorted(set(pacs_counts.index) | set(mol_counts.index))

    total_pacs = 0
    total_mol = 0

    for year in all_years:
        if year is None:
            continue
        roc_year = year - 1911
        pacs_count = pacs_counts.get(year, 0)
        mol_count = mol_counts.get(year, 0)

        diff = pacs_count - mol_count
        diff_str = f"+{diff}" if diff > 0 else str(diff) if diff < 0 else "0"

        print(
            f"{int(year)} ({roc_year}年)    {pacs_count:<12} {mol_count:<18} {diff_str:<10}"
        )
        total_pacs += pacs_count
        total_mol += mol_count

    print("-" * 70)
    print(f"{'總計':<15} {total_pacs:<12} {total_mol:<18} {total_pacs - total_mol:<10}")
    print()

    return pacs_df, mol_df


def compare_fields(pacs_df: pd.DataFrame, mol_df: pd.DataFrame):
    """比較欄位差異"""
    print("=" * 70)
    print("欄位比較")
    print("=" * 70)

    pacs_cols = set(pacs_df.columns)
    mol_cols = set(mol_df.columns)

    common = pacs_cols & mol_cols
    pacs_only = pacs_cols - mol_cols
    mol_only = mol_cols - pacs_cols

    print(f"共同欄位 ({len(common)}): {', '.join(sorted(common))}")
    print()
    if pacs_only:
        print(f"僅 pacs API 有 ({len(pacs_only)}): {', '.join(sorted(pacs_only))}")
    if mol_only:
        print(f"僅 MOL 有 ({len(mol_only)}): {', '.join(sorted(mol_only))}")
    print()


def normalize_date(date_val):
    """標準化日期格式為 YYYYMMDD"""
    if pd.isna(date_val):
        return None
    date_str = str(date_val).strip()
    # 移除 - 符號
    date_str = date_str.replace("-", "")
    return date_str[:8] if len(date_str) >= 8 else date_str


def find_unique_records(pacs_df: pd.DataFrame, mol_df: pd.DataFrame):
    """找出各來源獨有的記錄"""
    print("=" * 70)
    print("記錄比對（基於事業單位+發生日期）")
    print("=" * 70)

    pacs_df = pacs_df.copy()
    mol_df = mol_df.copy()

    # 標準化日期
    pacs_df["日期_標準"] = pacs_df["發生日期"].apply(normalize_date)
    mol_df["日期_標準"] = mol_df["發生日期"].apply(normalize_date)

    # 建立唯一識別（事業單位 + 標準化日期）
    pacs_df["key"] = pacs_df["事業單位"].astype(str) + "_" + pacs_df["日期_標準"].astype(
        str
    )
    mol_df["key"] = mol_df["事業單位"].astype(str) + "_" + mol_df["日期_標準"].astype(str)

    pacs_keys = set(pacs_df["key"])
    mol_keys = set(mol_df["key"])

    common_keys = pacs_keys & mol_keys
    pacs_only = pacs_keys - mol_keys
    mol_only = mol_keys - pacs_keys

    print(f"兩者皆有: {len(common_keys)} 筆")
    print(f"僅 pacs API 有: {len(pacs_only)} 筆")
    print(f"僅 MOL WEBSERVICES 有: {len(mol_only)} 筆")
    print()

    # 顯示部分僅 pacs 有的記錄
    if pacs_only:
        print("僅 pacs API 有的前 5 筆記錄:")
        pacs_unique = pacs_df[pacs_df["key"].isin(list(pacs_only)[:5])][
            ["事業單位", "發生日期", "災害類型", "年度"]
        ]
        print(pacs_unique.to_string(index=False))
        print()

    # 顯示部分僅 MOL 有的記錄
    if mol_only:
        print("僅 MOL WEBSERVICES 有的前 5 筆記錄:")
        mol_unique = mol_df[mol_df["key"].isin(list(mol_only)[:5])][
            ["事業單位", "發生日期", "災害類型", "年度"]
        ]
        print(mol_unique.to_string(index=False))


def main():
    """主程式"""
    print()
    print("=" * 70)
    print("重大職業災害資料來源比對報告")
    print("=" * 70)
    print()

    pacs_df, mol_df = load_data()

    print(f"pacs API 總筆數: {len(pacs_df)}")
    print(f"MOL WEBSERVICES 總筆數: {len(mol_df)}")
    print()

    pacs_df, mol_df = compare_counts(pacs_df, mol_df)
    compare_fields(pacs_df, mol_df)
    find_unique_records(pacs_df, mol_df)


if __name__ == "__main__":
    main()
