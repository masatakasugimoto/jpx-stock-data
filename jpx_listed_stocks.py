#!/usr/bin/env python3
"""
JPX-jQuants APIを使用して上場銘柄一覧を取得し、テキストファイルに出力するスクリプト
"""

import requests
import json
import csv
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional


class JQuantsAPI:
    """J-Quants API client class"""
    
    BASE_URL = "https://api.jquants.com/v1"
    
    def __init__(self, email: str, password: str):
        self.email = email
        self.password = password
        self.refresh_token = None
        self.id_token = None
    
    def authenticate(self) -> bool:
        """認証を行い、トークンを取得する"""
        # ステップ1: リフレッシュトークンを取得
        auth_url = f"{self.BASE_URL}/token/auth_user"
        auth_data = {
            "mailaddress": self.email,
            "password": self.password
        }
        headers = {"Content-Type": "application/json"}
        
        try:
            response = requests.post(auth_url, json=auth_data, headers=headers)
            print(f"認証レスポンスコード: {response.status_code}")
            
            if response.status_code != 200:
                print(f"認証失敗: {response.status_code} - {response.text}")
                return False
            
            auth_result = response.json()
            print(f"認証レスポンス: {auth_result}")
            
            self.refresh_token = auth_result.get("refreshToken")
            
            if not self.refresh_token:
                print("リフレッシュトークンの取得に失敗しました")
                return False
            
            # ステップ2: IDトークンを取得
            return self._get_id_token()
            
        except requests.exceptions.RequestException as e:
            print(f"認証エラー: {e}")
            return False
    
    def _get_id_token(self) -> bool:
        """IDトークンを取得する"""
        if not self.refresh_token:
            return False
        
        token_url = f"{self.BASE_URL}/token/auth_refresh"
        params = {"refreshtoken": self.refresh_token}
        
        try:
            response = requests.post(token_url, params=params)
            print(f"IDトークン取得レスポンスコード: {response.status_code}")
            
            if response.status_code != 200:
                print(f"IDトークン取得失敗: {response.status_code} - {response.text}")
                return False
            
            token_result = response.json()
            print(f"IDトークンレスポンス: {token_result}")
            
            self.id_token = token_result.get("idToken")
            
            if not self.id_token:
                print("IDトークンの取得に失敗しました")
                return False
            
            print("IDトークン取得成功")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"IDトークン取得エラー: {e}")
            return False
    
    def get_listed_info(self, date: Optional[str] = None, code: Optional[str] = None) -> Optional[List[Dict]]:
        """上場銘柄一覧を取得する
        
        Args:
            date: 取得日 (YYYY-MM-DD形式、省略時は最新)
            code: 銘柄コード (省略時は全銘柄)
        
        Returns:
            上場銘柄情報のリスト
        """
        if not self.id_token:
            print("認証が必要です")
            return None
        
        url = f"{self.BASE_URL}/listed/info"
        headers = {"Authorization": f"Bearer {self.id_token}"}
        params = {}
        
        if date:
            params["date"] = date
        if code:
            params["code"] = code
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            result = response.json()
            return result.get("info", [])
            
        except requests.exceptions.RequestException as e:
            print(f"データ取得エラー: {e}")
            return None
    
    def get_daily_quotes(self, code: str, from_date: str, to_date: str, 
                        pagination_key: Optional[str] = None) -> Optional[Dict]:
        """株価四本値データを取得する
        
        Args:
            code: 銘柄コード
            from_date: 開始日 (YYYY-MM-DD形式)
            to_date: 終了日 (YYYY-MM-DD形式)
            pagination_key: ページネーションキー (省略可)
        
        Returns:
            株価データの辞書（daily_quotes: データリスト, pagination_key: 次のページキー）
        """
        if not self.id_token:
            print("認証が必要です")
            return None
        
        url = f"{self.BASE_URL}/prices/daily_quotes"
        headers = {"Authorization": f"Bearer {self.id_token}"}
        params = {
            "code": code,
            "from": from_date,
            "to": to_date
        }
        
        if pagination_key:
            params["pagination_key"] = pagination_key
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            result = response.json()
            return {
                "daily_quotes": result.get("daily_quotes", []),
                "pagination_key": result.get("pagination_key")
            }
            
        except requests.exceptions.RequestException as e:
            print(f"株価データ取得エラー (コード: {code}): {e}")
            return None


def format_stock_code(code: str) -> str:
    """株式コードを4桁形式に変換する（末尾の0を削除）"""
    if code and code.endswith('0') and len(code) == 5:
        return code[:-1]
    return code

def save_to_text_file(data: List[Dict], filename: str) -> bool:
    """取得したデータをテキストファイルに保存する"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"上場銘柄一覧 - 取得日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
            
            for stock in data:
                code = format_stock_code(stock.get('Code', 'N/A'))
                f.write(f"コード: {code}\n")
                f.write(f"名称: {stock.get('CompanyName', 'N/A')}\n") 
                f.write(f"名称（英語）: {stock.get('CompanyNameEnglish', 'N/A')}\n")
                f.write(f"セクター: {stock.get('Sector17CodeName', 'N/A')}\n")
                f.write(f"市場区分: {stock.get('MarketCode', 'N/A')}\n")
                f.write(f"上場日: {stock.get('ListingDate', 'N/A')}\n")
                f.write("-" * 60 + "\n")
        
        print(f"データを {filename} に保存しました")
        return True
        
    except Exception as e:
        print(f"ファイル保存エラー: {e}")
        return False


def save_to_csv_file(data: List[Dict], filename: str) -> bool:
    """取得したデータをCSVファイルに保存する"""
    if not data:
        return False
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            fieldnames = ['Code', 'CompanyName', 'CompanyNameEnglish', 
                         'Sector17CodeName', 'MarketCode', 'ListingDate']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            writer.writeheader()
            for stock in data:
                # 株式コードを4桁形式に変換
                formatted_stock = stock.copy()
                formatted_stock['Code'] = format_stock_code(stock.get('Code', 'N/A'))
                
                writer.writerow({
                    field: formatted_stock.get(field, 'N/A') for field in fieldnames
                })
        
        print(f"CSVデータを {filename} に保存しました")
        return True
        
    except Exception as e:
        print(f"CSV保存エラー: {e}")
        return False


def save_stock_prices_to_csv(stock_data: List[Dict], filename: str) -> bool:
    """株価データをCSVファイルに保存する"""
    if not stock_data:
        return False
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            fieldnames = ['Code', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 
                         'TurnoverValue', 'AdjustmentFactor', 'AdjustmentOpen', 
                         'AdjustmentHigh', 'AdjustmentLow', 'AdjustmentClose', 'AdjustmentVolume']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            writer.writeheader()
            for quote in stock_data:
                # 株式コードを4桁形式に変換
                formatted_quote = quote.copy()
                formatted_quote['Code'] = format_stock_code(quote.get('Code', 'N/A'))
                
                writer.writerow({
                    field: formatted_quote.get(field, 'N/A') for field in fieldnames
                })
        
        print(f"株価データを {filename} に保存しました")
        return True
        
    except Exception as e:
        print(f"株価CSV保存エラー: {e}")
        return False


def get_all_stock_codes(client: 'JQuantsAPI') -> List[str]:
    """全銘柄のコード一覧を取得する"""
    print("上場銘柄一覧を取得中...")
    listed_stocks = client.get_listed_info()
    
    if not listed_stocks:
        print("銘柄一覧の取得に失敗しました")
        return []
    
    codes = []
    for stock in listed_stocks:
        code = stock.get('Code')
        if code:
            # 4桁形式に変換
            formatted_code = format_stock_code(code)
            codes.append(formatted_code)
    
    print(f"{len(codes)}銘柄のコードを取得しました")
    return codes


def is_business_day(date: datetime) -> bool:
    """営業日（土日祝日を除く平日）かどうかを判定する"""
    # 土日を除外
    if date.weekday() >= 5:  # 土曜日=5, 日曜日=6
        return False
    
    # 日本の祝日を除外（主要な祝日のみ）
    year = date.year
    month = date.month
    day = date.day
    
    # 固定祝日
    fixed_holidays = [
        (1, 1),   # 元日
        (2, 11),  # 建国記念の日
        (4, 29),  # 昭和の日
        (5, 3),   # 憲法記念日
        (5, 4),   # みどりの日
        (5, 5),   # こどもの日
        (8, 11),  # 山の日
        (11, 3),  # 文化の日
        (11, 23), # 勤労感謝の日
        (12, 23), # 天皇誕生日（2019年以降）
    ]
    
    if (month, day) in fixed_holidays:
        return False
    
    # 年末年始
    if (month == 12 and day >= 29) or (month == 1 and day <= 3):
        return False
    
    # その他の移動祝日は簡略化（完全ではないが主要なケースをカバー）
    # 成人の日（1月第2月曜日）
    if month == 1 and date.weekday() == 0:  # 月曜日
        jan_first = datetime(year, 1, 1)
        first_monday = 7 - jan_first.weekday() + 1
        if first_monday > 7:
            first_monday -= 7
        second_monday = first_monday + 7
        if day == second_monday:
            return False
    
    # 海の日（7月第3月曜日）
    if month == 7 and date.weekday() == 0:  # 月曜日
        jul_first = datetime(year, 7, 1)
        first_monday = 7 - jul_first.weekday() + 1
        if first_monday > 7:
            first_monday -= 7
        third_monday = first_monday + 14
        if day == third_monday:
            return False
    
    # 敬老の日（9月第3月曜日）
    if month == 9 and date.weekday() == 0:  # 月曜日
        sep_first = datetime(year, 9, 1)
        first_monday = 7 - sep_first.weekday() + 1
        if first_monday > 7:
            first_monday -= 7
        third_monday = first_monday + 14
        if day == third_monday:
            return False
    
    # 体育の日/スポーツの日（10月第2月曜日）
    if month == 10 and date.weekday() == 0:  # 月曜日
        oct_first = datetime(year, 10, 1)
        first_monday = 7 - oct_first.weekday() + 1
        if first_monday > 7:
            first_monday -= 7
        second_monday = first_monday + 7
        if day == second_monday:
            return False
    
    return True


def get_business_days_range(days: int = 30) -> tuple[str, str]:
    """営業日ベースで過去N日分の期間を取得する"""
    end_date = datetime.now()
    business_days_found = 0
    current_date = end_date
    
    # 現在日が営業日でない場合、直近の営業日まで遡る
    while not is_business_day(current_date):
        current_date -= timedelta(days=1)
    
    end_date = current_date
    
    # 営業日をN日分遡る
    while business_days_found < days:
        current_date -= timedelta(days=1)
        if is_business_day(current_date):
            business_days_found += 1
    
    start_date = current_date
    
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')


def get_all_stock_prices(client: 'JQuantsAPI', codes: List[str], days: int = 30) -> List[Dict]:
    """全銘柄の過去N営業日分の株価データを取得する"""
    from_date, to_date = get_business_days_range(days)
    
    print(f"株価データ取得期間: {from_date} ～ {to_date} (営業日ベース)")
    
    all_quotes = []
    total_codes = len(codes)
    
    for i, code in enumerate(codes, 1):
        print(f"進行状況: {i}/{total_codes} - コード: {code}")
        
        # APIレート制限対策で少し待機
        if i > 1:
            time.sleep(0.1)
        
        # 元の5桁コードに戻してAPI呼び出し（APIは5桁コードを期待している可能性）
        api_code = code + "0" if len(code) == 4 else code
        
        quotes_result = client.get_daily_quotes(api_code, from_date, to_date)
        
        if quotes_result and quotes_result["daily_quotes"]:
            for quote in quotes_result["daily_quotes"]:
                # 営業日のみフィルタリング
                quote_date = datetime.strptime(quote.get('Date', ''), '%Y-%m-%d')
                if is_business_day(quote_date):
                    # 4桁コードを設定
                    quote["Code"] = code
                    all_quotes.append(quote)
        
        # エラーが発生した場合でも継続
        if i % 100 == 0:
            print(f"  処理済み: {i}銘柄")
    
    print(f"総取得レコード数: {len(all_quotes)} (営業日のみ)")
    return all_quotes


def main():
    """メイン処理"""
    print("JPX-jQuants API 株価データ取得ツール")
    print("=" * 50)
    
    # 認証情報
    email = "masadon999@gmail.com"
    password = "Suikano191919"
    
    # APIクライアントの初期化と認証
    client = JQuantsAPI(email, password)
    
    print("認証中...")
    if not client.authenticate():
        print("認証に失敗しました")
        sys.exit(1)
    
    print("認証成功！")
    
    # タイムスタンプ
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 選択メニュー
    print("\n実行する処理を選択してください:")
    print("1. 上場銘柄一覧のみ取得")
    print("2. 過去30日分の全銘柄株価データ取得")
    print("3. 両方実行")
    print("4. テスト用: 最初の50銘柄のみ株価データ取得")
    
    choice = input("選択 (1-4): ").strip()
    
    if choice in ["1", "3"]:
        # 上場銘柄一覧の取得
        print("\n上場銘柄一覧を取得中...")
        listed_stocks = client.get_listed_info()
        
        if not listed_stocks:
            print("銘柄一覧の取得に失敗しました")
        else:
            print(f"{len(listed_stocks)}件の銘柄情報を取得しました")
            
            # テキストファイルとして保存
            text_filename = f"listed_stocks_{timestamp}.txt"
            save_to_text_file(listed_stocks, text_filename)
            
            # CSVファイルとしても保存
            csv_filename = f"listed_stocks_{timestamp}.csv"
            save_to_csv_file(listed_stocks, csv_filename)
    
    if choice in ["2", "3", "4"]:
        # 全銘柄のコード一覧を取得
        stock_codes = get_all_stock_codes(client)
        
        if not stock_codes:
            print("銘柄コードの取得に失敗しました")
            sys.exit(1)
        
        # テストモードの場合は最初の50銘柄のみ
        if choice == "4":
            stock_codes = stock_codes[:50]
            print(f"\nテストモード: 最初の{len(stock_codes)}銘柄の過去30営業日分株価データを取得中...")
        else:
            print(f"\n{len(stock_codes)}銘柄の過去30営業日分株価データを取得中...")
            print("※この処理には時間がかかります")
        
        print("※土日祝日は除外されます")
        
        stock_prices = get_all_stock_prices(client, stock_codes, days=30)
        
        if stock_prices:
            # 株価データをCSVで保存
            if choice == "4":
                stock_prices_filename = f"stock_prices_test_50stocks_{timestamp}.csv"
            else:
                stock_prices_filename = f"stock_prices_30days_{timestamp}.csv"
            save_stock_prices_to_csv(stock_prices, stock_prices_filename)
        else:
            print("株価データの取得に失敗しました")
    
    if choice not in ["1", "2", "3", "4"]:
        print("無効な選択です")
        sys.exit(1)
    
    print("\n処理完了！")


if __name__ == "__main__":
    main()