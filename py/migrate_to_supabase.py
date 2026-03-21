import sqlite3
import json
import http.client
import sys
from typing import List, Dict, Any, Final, Optional

class SupabaseMigrator:
    """
    SQLite3からSupabaseへデータを移行するスタンドアロンスクリプト。
    標準ライブラリのみを使用し、リクエストのチャンク分割機能を搭載。
    """

    def __init__(self, project_url: str, api_key: str) -> None:
        # URLからホスト名のみを抽出
        self.host: str = project_url.replace("https://", "").replace("http://", "").split("/")[0]
        self.api_key: str = api_key
        self.base_path: str = "/rest/v1/"
        self.headers: Dict[str, str] = {
            "apikey": self.api_key,
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal"
        }

    def fetch_data(self, db_path: str, table: str) -> List[Dict[str, Any]]:
        """SQLiteからデータを辞書形式で取得"""
        try:
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(f"SELECT * FROM {table}")
                return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"Database Error: {e}")
            return []

    def migrate(self, table_name: str, data: List[Dict[str, Any]], chunk_size: int = 100) -> None:
        """データを分割してSupabaseへPOST"""
        if not data:
            print("No data to migrate.")
            return

        conn = http.client.HTTPSConnection(self.host)
        
        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size]
            payload = json.dumps(chunk)
            
            endpoint = f"{self.base_path}{table_name}"
            conn.request("POST", endpoint, body=payload, headers=self.headers)
            
            response = conn.getresponse()
            # 読み飛ばし処理（接続維持のため）
            response.read()
            
            if response.status in (200, 201):
                print(f"Chunk {i//chunk_size + 1} pushed successfully.")
            else:
                print(f"Error at Chunk {i//chunk_size + 1}: Status {response.status}")
        
        conn.close()

if __name__ == "__main__":
    # --- ユーザー定義パラメータ ---
    DB_FILE: Final[str] = "furugiya_momotaro.db"
    TARGET_TABLE: Final[str] = "inventory"
    S_URL: Final[str] = "YOUR_PROJECT_REF.supabase.co"
    S_KEY: Final[str] = "YOUR_SERVICE_ROLE_KEY"
    # ----------------------------

    migrator = SupabaseMigrator(S_URL, S_KEY)
    records = migrator.fetch_data(DB_FILE, TARGET_TABLE)
    migrator.migrate(TARGET_TABLE, records)