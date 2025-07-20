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
    
    def get_statements(self, code: Optional[str] = None, date: Optional[str] = None) -> Optional[List[Dict]]:
        """財務情報を取得する
        
        Args:
            code: 銘柄コード (省略時は全銘柄)
            date: 決算日 (YYYY-MM-DD形式、省略時は最新)
        
        Returns:
            財務情報のリスト
        """
        if not self.id_token:
            print("認証が必要です")
            return None
        
        url = f"{self.BASE_URL}/fins/statements"
        headers = {"Authorization": f"Bearer {self.id_token}"}
        params = {}
        
        if code:
            params["code"] = code
        if date:
            params["date"] = date
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            result = response.json()
            return result.get("statements", [])
            
        except requests.exceptions.RequestException as e:
            print(f"財務データ取得エラー: {e}")
            return None
    
    def get_announcement(self, code: Optional[str] = None, from_date: Optional[str] = None, 
                        to_date: Optional[str] = None) -> Optional[List[Dict]]:
        """決算発表予定日を取得する
        
        Args:
            code: 銘柄コード (省略時は全銘柄)
            from_date: 開始日 (YYYY-MM-DD形式)
            to_date: 終了日 (YYYY-MM-DD形式)
        
        Returns:
            決算発表予定日のリスト
        """
        if not self.id_token:
            print("認証が必要です")
            return None
        
        url = f"{self.BASE_URL}/fins/announcement"
        headers = {"Authorization": f"Bearer {self.id_token}"}
        params = {}
        
        if code:
            params["code"] = code
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            result = response.json()
            return result.get("announcement", [])
            
        except requests.exceptions.RequestException as e:
            print(f"決算発表予定日取得エラー: {e}")
            return None
    
    def get_margin_balance(self, code: Optional[str] = None, date: Optional[str] = None) -> Optional[List[Dict]]:
        """信用取引週末残高を取得する
        
        Args:
            code: 銘柄コード (省略時は全銘柄)
            date: 日付 (YYYY-MM-DD形式、省略時は最新)
        
        Returns:
            信用取引週末残高のリスト
        """
        if not self.id_token:
            print("認証が必要です")
            return None
        
        url = f"{self.BASE_URL}/markets/weekly_margin_interest"
        headers = {"Authorization": f"Bearer {self.id_token}"}
        params = {}
        
        if code:
            params["code"] = code
        if date:
            params["date"] = date
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            result = response.json()
            
            # 考えられるキー名を試行
            for key in ["weekly_margin_interest", "margin_interest", "weekly_margin", "margin_balance"]:
                if key in result:
                    return result.get(key, [])
            
            return result.get("weekly_margin_interest", [])
            
        except requests.exceptions.RequestException as e:
            print(f"信用取引週末残高取得エラー: {e}")
            return None
    
    def get_short_selling_by_sector(self, date: Optional[str] = None) -> Optional[List[Dict]]:
        """業種別空売り比率を取得する
        
        Args:
            date: 日付 (YYYY-MM-DD形式、省略時は最新の営業日)
        
        Returns:
            業種別空売り比率のリスト
        """
        if not self.id_token:
            print("認証が必要です")
            return None
        
        headers = {"Authorization": f"Bearer {self.id_token}"}
        
        # 日付が指定されていない場合は最新の営業日を設定
        if not date:
            today = datetime.now()
            # 直近の営業日を取得
            current_date = today
            while not is_business_day(current_date):
                current_date -= timedelta(days=1)
            date = current_date.strftime('%Y-%m-%d')
        
        # 必須パラメータを含めてリクエスト
        params = {"date": date}
        
        url = f"{self.BASE_URL}/markets/short_selling"
        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                result = response.json()
                # 様々なレスポンスキーを試行
                for key in ["short_selling", "short_selling_by_sector", "sector_short_selling", "data"]:
                    if key in result:
                        return result.get(key, [])
                return result if isinstance(result, list) else []
            elif response.status_code == 403:
                print("業種別空売り比率: プランでアクセス制限されています")
                return None
            elif response.status_code == 400:
                print(f"業種別空売り比率: パラメータエラー - {response.text}")
                return None
            else:
                print(f"業種別空売り比率エラー: {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"業種別空売り比率取得エラー: {e}")
            return None
    
    def get_short_selling_balance(self, code: Optional[str] = None, date: Optional[str] = None) -> Optional[List[Dict]]:
        """空売り残高報告を取得する
        
        Args:
            code: 銘柄コード (省略時は全銘柄)
            date: 日付 (YYYY-MM-DD形式、省略時は最新)
        
        Returns:
            空売り残高報告のリスト
        """
        if not self.id_token:
            print("認証が必要です")
            return None
        
        url = f"{self.BASE_URL}/markets/short_selling_positions"
        headers = {"Authorization": f"Bearer {self.id_token}"}
        params = {}
        
        if code:
            params["code"] = code
        if date:
            params["date"] = date
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            result = response.json()
            return result.get("short_selling_positions", [])
            
        except requests.exceptions.RequestException as e:
            print(f"空売り残高報告取得エラー: {e}")
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


def save_statements_to_csv(statements_data: List[Dict], filename: str) -> bool:
    """財務データをCSVファイルに保存する"""
    if not statements_data:
        return False
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            # 財務データの主要フィールド
            fieldnames = ['Code', 'DisclosedDate', 'DisclosedTime', 'LocalCode', 
                         'DisclosureNumber', 'TypeOfDocument', 'TypeOfCurrentPeriod',
                         'CurrentPeriodStartDate', 'CurrentPeriodEndDate', 'CurrentFiscalYearStartDate',
                         'CurrentFiscalYearEndDate', 'NextFiscalYearStartDate', 'NextFiscalYearEndDate',
                         'NetSales', 'OperatingProfit', 'OrdinaryProfit', 'Profit',
                         'EarningsPerShare', 'TotalAssets', 'Equity', 'EquityToAssetRatio',
                         'BookValuePerShare', 'CashFlowsFromOperatingActivities',
                         'CashFlowsFromInvestingActivities', 'CashFlowsFromFinancingActivities',
                         'CashAndEquivalents']
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            writer.writeheader()
            for statement in statements_data:
                # 株式コードを4桁形式に変換
                formatted_statement = statement.copy()
                formatted_statement['Code'] = format_stock_code(statement.get('Code', 'N/A'))
                
                writer.writerow({
                    field: formatted_statement.get(field, 'N/A') for field in fieldnames
                })
        
        print(f"財務データを {filename} に保存しました")
        return True
        
    except Exception as e:
        print(f"財務CSV保存エラー: {e}")
        return False


def save_announcements_to_csv(announcements_data: List[Dict], filename: str) -> bool:
    """決算発表予定日データをCSVファイルに保存する"""
    if not announcements_data:
        return False
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            fieldnames = ['Code', 'Date', 'CompanyName', 'FiscalYear', 'SectorName',
                         'FiscalQuarter', 'Section']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            writer.writeheader()
            for announcement in announcements_data:
                # 株式コードを4桁形式に変換
                formatted_announcement = announcement.copy()
                formatted_announcement['Code'] = format_stock_code(announcement.get('Code', 'N/A'))
                
                writer.writerow({
                    field: formatted_announcement.get(field, 'N/A') for field in fieldnames
                })
        
        print(f"決算発表予定日データを {filename} に保存しました")
        return True
        
    except Exception as e:
        print(f"決算発表予定日CSV保存エラー: {e}")
        return False


def save_margin_balance_to_csv(margin_data: List[Dict], filename: str) -> bool:
    """信用取引週末残高データをCSVファイルに保存する"""
    if not margin_data:
        return False
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            # 実際のデータ構造に基づいてフィールド名を動的に決定
            if margin_data:
                actual_fieldnames = list(margin_data[0].keys())
                writer = csv.DictWriter(f, fieldnames=actual_fieldnames)
                
                writer.writeheader()
                for margin in margin_data:
                    # 株式コードを4桁形式に変換（Codeフィールドが存在する場合）
                    formatted_margin = margin.copy()
                    if 'Code' in formatted_margin:
                        formatted_margin['Code'] = format_stock_code(margin.get('Code', 'N/A'))
                    
                    writer.writerow(formatted_margin)
            
        print(f"信用取引週末残高データを {filename} に保存しました")
        return True
        
    except Exception as e:
        print(f"信用取引週末残高CSV保存エラー: {e}")
        return False


def save_short_selling_by_sector_to_csv(short_selling_data: List[Dict], filename: str) -> bool:
    """業種別空売り比率データをCSVファイルに保存する"""
    if not short_selling_data:
        return False
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            # 実際のデータ構造に基づいてフィールド名を動的に決定
            if short_selling_data:
                actual_fieldnames = list(short_selling_data[0].keys())
                writer = csv.DictWriter(f, fieldnames=actual_fieldnames)
                
                writer.writeheader()
                for data in short_selling_data:
                    writer.writerow(data)
        
        print(f"業種別空売り比率データを {filename} に保存しました")
        return True
        
    except Exception as e:
        print(f"業種別空売り比率CSV保存エラー: {e}")
        return False


def save_short_selling_balance_to_csv(balance_data: List[Dict], filename: str) -> bool:
    """空売り残高報告データをCSVファイルに保存する"""
    if not balance_data:
        return False
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            # 実際のデータ構造に基づいてフィールド名を動的に決定
            if balance_data:
                actual_fieldnames = list(balance_data[0].keys())
                writer = csv.DictWriter(f, fieldnames=actual_fieldnames)
                
                writer.writeheader()
                for balance in balance_data:
                    # 株式コードを4桁形式に変換（Codeフィールドが存在する場合）
                    formatted_balance = balance.copy()
                    if 'Code' in formatted_balance:
                        formatted_balance['Code'] = format_stock_code(balance.get('Code', 'N/A'))
                    
                    writer.writerow(formatted_balance)
        
        print(f"空売り残高報告データを {filename} に保存しました")
        return True
        
    except Exception as e:
        print(f"空売り残高報告CSV保存エラー: {e}")
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


def get_days_selection() -> int:
    """ユーザーから取得日数を選択させる"""
    print("\n取得する営業日数を選択してください:")
    print("1. 5営業日")
    print("2. 30営業日（デフォルト）")
    print("3. 1年（約250営業日）")
    print("4. 5年（約1250営業日）")
    
    while True:
        choice = input("選択 (1-4): ").strip()
        
        if choice == "1":
            return 5
        elif choice == "2":
            return 30
        elif choice == "3":
            return 250  # 1年間の営業日数
        elif choice == "4":
            return 1250  # 5年間の営業日数
        else:
            print("1-4の範囲で選択してください")


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
    print("3. 財務情報取得")
    print("4. 決算発表予定日取得")
    print("5. 信用取引週末残高取得")
    print("6. 業種別空売り比率取得")
    print("7. 空売り残高報告取得")
    print("8. 全データ取得（すべてのデータ）")
    print("")
    print("--- テスト用（最初の50銘柄のみ） ---")
    print("9. テスト用: 株価データのみ")
    print("10. テスト用: 財務情報のみ") 
    print("11. テスト用: 決算発表予定日のみ")
    print("12. テスト用: 信用取引週末残高のみ")
    print("13. テスト用: 空売り残高報告のみ")
    print("14. テスト用: 全データ取得")
    
    choice = input("選択 (1-14): ").strip()
    
    # テストモードかどうかを判定
    is_test_mode = choice in ["9", "10", "11", "12", "13", "14"]
    test_limit = 50 if is_test_mode else None
    
    if choice in ["1", "8", "14"]:
        # 上場銘柄一覧の取得
        if is_test_mode:
            print(f"\nテストモード: 上場銘柄一覧を取得中（最初の{test_limit}件のみ）...")
        else:
            print("\n上場銘柄一覧を取得中...")
        
        listed_stocks = client.get_listed_info()
        
        if not listed_stocks:
            print("銘柄一覧の取得に失敗しました")
        else:
            # テストモードの場合は最初の50件のみ
            if is_test_mode:
                listed_stocks = listed_stocks[:test_limit]
            
            print(f"{len(listed_stocks)}件の銘柄情報を取得しました")
            
            # テキストファイルとして保存
            if is_test_mode:
                text_filename = f"listed_stocks_test_{timestamp}.txt"
                csv_filename = f"listed_stocks_test_{timestamp}.csv"
            else:
                text_filename = f"listed_stocks_{timestamp}.txt"
                csv_filename = f"listed_stocks_{timestamp}.csv"
            
            save_to_text_file(listed_stocks, text_filename)
            save_to_csv_file(listed_stocks, csv_filename)
    
    if choice in ["2", "8", "9", "14"]:
        # 取得日数を選択
        days = get_days_selection()
        
        # 全銘柄のコード一覧を取得
        stock_codes = get_all_stock_codes(client)
        
        if not stock_codes:
            print("銘柄コードの取得に失敗しました")
            sys.exit(1)
        
        # テストモードの場合は最初の50銘柄のみ
        if is_test_mode:
            stock_codes = stock_codes[:test_limit]
            print(f"\nテストモード: 最初の{len(stock_codes)}銘柄の過去{days}営業日分株価データを取得中...")
        else:
            print(f"\n{len(stock_codes)}銘柄の過去{days}営業日分株価データを取得中...")
            print("※この処理には時間がかかります")
        
        print("※土日祝日は除外されます")
        
        stock_prices = get_all_stock_prices(client, stock_codes, days=days)
        
        if stock_prices:
            # 株価データをCSVで保存
            if is_test_mode:
                stock_prices_filename = f"stock_prices_test_50stocks_{days}days_{timestamp}.csv"
            else:
                stock_prices_filename = f"stock_prices_{days}days_{timestamp}.csv"
            save_stock_prices_to_csv(stock_prices, stock_prices_filename)
        else:
            print("株価データの取得に失敗しました")
    
    if choice in ["3", "8", "10", "14"]:
        # 財務情報取得
        if is_test_mode:
            # テストモード: 最初の50銘柄の財務情報のみ取得
            stock_codes = get_all_stock_codes(client)
            if stock_codes:
                test_codes = stock_codes[:test_limit]
                print(f"\nテストモード: 最初の{len(test_codes)}銘柄の財務情報を取得中...")
                
                all_statements = []
                for i, code in enumerate(test_codes, 1):
                    print(f"進行状況: {i}/{len(test_codes)} - コード: {code}")
                    if i > 1:
                        time.sleep(0.1)  # APIレート制限対策
                    
                    # 5桁コードでAPI呼び出し
                    api_code = code + "0" if len(code) == 4 else code
                    statements = client.get_statements(code=api_code)
                    
                    if statements:
                        for statement in statements:
                            statement['Code'] = code  # 4桁コードに変換
                            all_statements.append(statement)
                
                if all_statements:
                    print(f"{len(all_statements)}件の財務情報を取得しました")
                    statements_filename = f"financial_statements_test_{timestamp}.csv"
                    save_statements_to_csv(all_statements, statements_filename)
                else:
                    print("財務情報の取得に失敗しました")
            else:
                print("銘柄コードの取得に失敗しました")
        else:
            # 通常モード: 全銘柄の財務情報取得
            print("\n財務情報を取得中...")
            statements = client.get_statements()
            
            if statements:
                print(f"{len(statements)}件の財務情報を取得しました")
                statements_filename = f"financial_statements_{timestamp}.csv"
                save_statements_to_csv(statements, statements_filename)
            else:
                print("財務情報の取得に失敗しました")
    
    if choice in ["4", "8", "11", "14"]:
        # 決算発表予定日取得
        if is_test_mode:
            print(f"\nテストモード: 決算発表予定日を取得中（最初の{test_limit}件のみ）...")
        else:
            print("\n決算発表予定日を取得中...")
        
        # 今後3ヶ月分の決算発表予定を取得
        today = datetime.now()
        from_date = today.strftime('%Y-%m-%d')
        to_date = (today + timedelta(days=90)).strftime('%Y-%m-%d')
        
        announcements = client.get_announcement(from_date=from_date, to_date=to_date)
        
        if announcements:
            # テストモードの場合は最初の50件のみ
            if is_test_mode:
                announcements = announcements[:test_limit]
                announcements_filename = f"earnings_announcements_test_{timestamp}.csv"
            else:
                announcements_filename = f"earnings_announcements_{timestamp}.csv"
            
            print(f"{len(announcements)}件の決算発表予定を取得しました")
            save_announcements_to_csv(announcements, announcements_filename)
        else:
            print("決算発表予定日の取得に失敗しました")
    
    if choice in ["5", "8", "12", "14"]:
        # 信用取引週末残高取得
        if is_test_mode:
            # テストモード: 最初の50銘柄の信用取引データのみ取得
            stock_codes = get_all_stock_codes(client)
            if stock_codes:
                test_codes = stock_codes[:test_limit]
                print(f"\nテストモード: 最初の{len(test_codes)}銘柄の信用取引週末残高を取得中...")
                
                all_margin_data = []
                for i, code in enumerate(test_codes, 1):
                    print(f"進行状況: {i}/{len(test_codes)} - コード: {code}")
                    if i > 1:
                        time.sleep(0.1)  # APIレート制限対策
                    
                    # 5桁コードでAPI呼び出し
                    api_code = code + "0" if len(code) == 4 else code
                    margin_data = client.get_margin_balance(code=api_code)
                    
                    if margin_data:
                        for data in margin_data:
                            data['Code'] = code  # 4桁コードに変換
                            all_margin_data.append(data)
                
                if all_margin_data:
                    print(f"{len(all_margin_data)}件の信用取引週末残高を取得しました")
                    margin_filename = f"margin_balance_test_{timestamp}.csv"
                    save_margin_balance_to_csv(all_margin_data, margin_filename)
                else:
                    print("信用取引週末残高の取得に失敗しました")
            else:
                print("銘柄コードの取得に失敗しました")
        else:
            # 通常モード: 全銘柄の信用取引週末残高取得
            print("\n信用取引週末残高を取得中...")
            margin_data = client.get_margin_balance()
            
            if margin_data:
                print(f"{len(margin_data)}件の信用取引週末残高を取得しました")
                margin_filename = f"margin_balance_{timestamp}.csv"
                save_margin_balance_to_csv(margin_data, margin_filename)
            else:
                print("信用取引週末残高の取得に失敗しました")
    
    if choice in ["6", "8", "14"]:
        # 業種別空売り比率取得
        print("\n業種別空売り比率を取得中...")
        short_selling_data = client.get_short_selling_by_sector()
        
        if short_selling_data:
            print(f"{len(short_selling_data)}件の業種別空売り比率を取得しました")
            if is_test_mode:
                short_selling_filename = f"short_selling_by_sector_test_{timestamp}.csv"
            else:
                short_selling_filename = f"short_selling_by_sector_{timestamp}.csv"
            save_short_selling_by_sector_to_csv(short_selling_data, short_selling_filename)
        else:
            print("業種別空売り比率の取得に失敗しました（スキップして処理を継続します）")
    
    if choice in ["7", "8", "13", "14"]:
        # 空売り残高報告取得
        if is_test_mode:
            # テストモード: 最初の50銘柄の空売り残高のみ取得
            stock_codes = get_all_stock_codes(client)
            if stock_codes:
                test_codes = stock_codes[:test_limit]
                print(f"\nテストモード: 最初の{len(test_codes)}銘柄の空売り残高報告を取得中...")
                
                all_balance_data = []
                for i, code in enumerate(test_codes, 1):
                    print(f"進行状況: {i}/{len(test_codes)} - コード: {code}")
                    if i > 1:
                        time.sleep(0.1)  # APIレート制限対策
                    
                    # 5桁コードでAPI呼び出し
                    api_code = code + "0" if len(code) == 4 else code
                    balance_data = client.get_short_selling_balance(code=api_code)
                    
                    if balance_data:
                        for data in balance_data:
                            data['Code'] = code  # 4桁コードに変換
                            all_balance_data.append(data)
                
                if all_balance_data:
                    print(f"{len(all_balance_data)}件の空売り残高報告を取得しました")
                    balance_filename = f"short_selling_balance_test_{timestamp}.csv"
                    save_short_selling_balance_to_csv(all_balance_data, balance_filename)
                else:
                    print("空売り残高報告の取得に失敗しました")
            else:
                print("銘柄コードの取得に失敗しました")
        else:
            # 通常モード: 全銘柄の空売り残高報告取得
            print("\n空売り残高報告を取得中...")
            balance_data = client.get_short_selling_balance()
            
            if balance_data:
                print(f"{len(balance_data)}件の空売り残高報告を取得しました")
                balance_filename = f"short_selling_balance_{timestamp}.csv"
                save_short_selling_balance_to_csv(balance_data, balance_filename)
            else:
                print("空売り残高報告の取得に失敗しました")
    
    if choice not in ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14"]:
        print("無効な選択です")
        sys.exit(1)
    
    print("\n処理完了！")


if __name__ == "__main__":
    main()