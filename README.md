# JPX-jQuants API 上場銘柄一覧取得ツール

このツールは、JPX-jQuants APIを使用して日本の上場銘柄一覧を取得し、テキストファイルやCSVファイルに出力するPythonスクリプトです。

## 必要な環境

- Python 3.7以上
- requests ライブラリ

## セットアップ

1. 必要なライブラリをインストール:
```bash
pip install -r requirements.txt
```

2. J-Quants APIアカウントの準備:
   - [J-Quants API](https://jpx-jquants.com/)でアカウントを作成
   - メールアドレスとパスワードを準備

## 使用方法

```bash
python jpx_listed_stocks.py
```

実行すると、メールアドレスとパスワードの入力が求められます。
認証成功後、上場銘柄一覧が取得され、以下のファイルが生成されます:

- `listed_stocks_YYYYMMDD_HHMMSS.txt` - テキスト形式
- `listed_stocks_YYYYMMDD_HHMMSS.csv` - CSV形式

## 取得可能なデータ

- 銘柄コード
- 会社名（日本語・英語）
- セクター情報
- 市場区分
- 上場日

## 注意事項

- J-Quants APIの利用規約に従ってご利用ください
- 無料プランでは過去2年分のデータのみ取得可能です
- APIの利用回数制限にご注意ください