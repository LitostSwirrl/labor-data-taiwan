# 勞動相關資料彙整專案

## 專案說明
收集台灣勞動相關公開資料，包括重大職業災害、違反勞動法令等資料。

## 資料來源

### 1. 重大職業災害公開網 (已完成)
- API: `https://pacs.osha.gov.tw/api/v1/getdangerocupation`
- 資料範圍: 2018 年至今
- 輸出: `data/processed/重大職業災害_2018至今.csv`

### 2. 待處理資料來源
- 違反勞動法令事業單位查詢系統
- 事業單位違反職業安全衛生法令資料
- 職場重大災害檢查訊息

## 執行腳本

```bash
# 下載重大職業災害資料
python3 scripts/fetch_occupational_disasters.py
```

## 專案結構

```
勞動相關資料彙整/
├── CLAUDE.md              # 專案說明（本檔案）
├── plan.md                # 實施計畫
├── scripts/
│   └── fetch_occupational_disasters.py
├── data/
│   ├── raw/               # 原始 JSON
│   └── processed/         # 處理後 CSV
└── .gitignore
```

## 技術細節

### API 參數
| 參數 | 說明 | 範例 |
|------|------|------|
| `info_PostdateS` | 起始日期 | `20200101` |
| `info_PostdateE` | 結束日期 | `20201231` |
| `info_addr` | 縣市 | `新北市` |
| `info_q` | 關鍵字 | `墜落` |

### 注意事項
- API 單次查詢上限 200 筆，腳本使用半年分批查詢
- 需關閉 SSL 驗證（政府網站憑證問題）
