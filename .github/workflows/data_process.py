import duckdb
import os
import psycopg2
from typing import List, Tuple
from datetime import datetime
# requests, json はここでは不要ですが、以前のスクリプトには必要でした。

# 既に定義されているはずの環境変数を使用
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL") 
OUTPUT_SQL_FILE = "data_insert.sql"

# odpt の生データテーブル名と、カスタムスキーマのテーブル名をマッピング
MAPPING_TABLES = {
    "railway": "routes",
    "station": "stops_maas",
    "stops": "stops_maas", # バス停も同じテーブルにマッピング
}

# ===================================================================
# ③ DuckDBでの変換・マッピング処理
# ===================================================================
def transform_to_custom_schema():
    """ 
    Parquetファイルを読み込み、DuckDBでカスタムスキーマに変換し、
    PostgreSQLのINSERT文を生成する
    """
    con = duckdb.connect(database=':memory:', read_only=False)
    
    # ParquetファイルをDuckDBに取り込み
    for odpt_name, custom_name in MAPPING_TABLES.items():
        parquet_path = os.path.join("output", f"{odpt_name}.parquet")
        if os.path.exists(parquet_path):
             # odpt名で一時テーブルを作成
            con.execute(f"CREATE OR REPLACE TABLE raw_{odpt_name} AS SELECT * FROM read_parquet('{parquet_path}')")
            print(f"Loaded raw_{odpt_name} from Parquet.")
        else:
            print(f"Warning: {parquet_path} not found. Skipping {odpt_name}.")


    # --- 路線情報 (routes) の変換 ---
    print("\nTransforming Railway data to routes...")
    con.execute("""
        CREATE OR REPLACE TABLE final_routes AS
        SELECT
            -- odpt.Railway:JR-East.ChuoRapid -> ChuoRapid
            SUBSTRING("owl:sameAs" FROM '[^.]+$') AS id, 
            -- odpt:Operator:JR-East -> OP_JR_EAST (事業者は別途マッピングが必要だが、今回は固定値)
            CASE 
                WHEN "odpt:operator" LIKE '%JR%' THEN 'OP_JR_EAST'
                WHEN "odpt:operator" LIKE '%Toei%' THEN 'OP_TOEI'
                ELSE 'OP_OTHER'
            END AS operator_id, 
            "odpt:lineCode" AS route_short_name,
            "odpt:railwayTitle" AS route_long_name,
            2 AS route_type -- 鉄道はGTFSで 2
        FROM raw_railway;
    """)


    # --- 停車地情報 (stops_maas) の変換・統合 ---
    print("Transforming Station/Busstop data to stops_maas...")
    
    # 鉄道駅 (Station) のマッピング
    station_query = """
        SELECT
            SUBSTRING("owl:sameAs" FROM '[^.]+$') AS id,
            "odpt:stationTitle" AS name,
            "geo:lat" AS latitude,
            "geo:lon" AS longitude,
            1 AS location_type -- 1 = Station
        FROM raw_station
    """
    
    # バス停 (BusstopPole) のマッピング (緯度経度のカラム名が異なる可能性あり)
    # ここでは仮に geo:lat, geo:lon と同じとして UNION
    busstop_query = """
        SELECT
            SUBSTRING("owl:sameAs" FROM '[^.]+$') AS id,
            "odpt:busstopPoleTitle" AS name, -- odpt:BusstopPole のタイトル
            "geo:lat" AS latitude,
            "geo:lon" AS longitude,
            0 AS location_type -- 0 = Stop
        FROM raw_stops
    """

    con.execute(f"""
        CREATE OR REPLACE TABLE final_stops_maas AS
        {station_query}
        UNION ALL
        {busstop_query}
    """)
    
    
    # 4. PostgreSQL向け TRUNCATE & INSERT 文の生成 (最終SQLファイルの作成)
    
    # TRUNCATE + INSERT のSQLを生成
    def generate_insert_sql(custom_table: str):
        # COPY コマンドを使う代わりに、INSERT VALUES(...) を大量に生成します
        # 挿入時の外部キー制約違反を避けるため、TRUNCATE でクリアします
        sql_list = [f'TRUNCATE TABLE public."{custom_table}" RESTART IDENTITY CASCADE;']
        
        # DuckDBの CSVエクスポート機能を使って、INSERT VALUESの形式で出力
        csv_data = con.execute(f"SELECT * FROM final_{custom_table}").fetchall()
        
        for row in csv_data:
            values_str = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in row])
            # 注意: SQLインジェクションを防ぐため、実際には psycopg2 の cursor.execute を使うべきですが、
            # GitHub Actionsでのファイル生成のため、ここでは単純な形式で出力します。
            sql_list.append(f"INSERT INTO {custom_table} VALUES ({values_str});")
        return sql_list

    
    final_sql_statements = []
    # 依存関係を考慮し、依存関係のないテーブルから生成 (stops_maas は routes より先に挿入)
    for table in ['stops_maas', 'routes']:
        final_sql_statements.extend(generate_insert_sql(table))
        print(f"Generated {len(final_sql_statements)} statements for {table}.")


    # 5. SQLファイルへの書き出し
    with open(OUTPUT_SQL_FILE, 'w', encoding='utf-8') as f:
        f.write(f"-- Data generated on: {datetime.now().isoformat()}\n\n")
        f.write("BEGIN;\n\n") # トランザクション開始
        for stmt in final_sql_statements:
            f.write(stmt + "\n")
        f.write("\nCOMMIT;\n") # トランザクション終了
        
        print(f"✅ 処理完了。SQL INSERT文を {OUTPUT_SQL_FILE} に書き出しました。")

    con.close()
    return OUTPUT_SQL_FILE

# 以前の main 関数と統合するための処理 (upload_all_to_supabase を置き換え)
# （GitHub Actionsで実行されることを想定）
def main():
    # 1. odptデータダウンロードとParquet圧縮 (以前の処理)
    # ... (前回のスクリプトの download_data, process_and_compress_data 部分を実行)
    
    # 2. カスタムスキーマへの変換とSQL生成
    transform_to_custom_schema()
    
    # 3. 生成されたSQLファイルをSupabaseで実行
    db_url = os.environ.get("SUPABASE_DB_URL")
    if db_url:
        print(f"\n--- Executing {OUTPUT_SQL_FILE} on Supabase ---")
        try:
            conn = psycopg2.connect(db_url)
            conn.autocommit = True
            cur = conn.cursor()
            with open(OUTPUT_SQL_FILE, 'r', encoding='utf-8') as f:
                cur.execute(f.read())
            print("Successfully uploaded data to Supabase.")
        except Exception as e:
            print(f"Error during Supabase upload: {e}")
            sys.exit(1)
    else:
        print("SUPABASE_DB_URL not found. Skipping direct upload.")


if __name__ == "__main__":
    # この部分が GitHub Actions で実行されるメイン処理
    # ⚠️ 実際には、このスクリプトにダウンロードロジックを統合する必要があります。
    # ここでは簡略化のため、Parquetファイルが既に 'output/' に存在することを前提とします。
    # ダウンロードロジックは以前のスクリプトから統合してください。
    
    # 以下の処理は、odpt_to_postgres_extended.py に置き換わることを想定
    # main() 
    print("This is the custom transformation step. Assuming raw Parquet files are ready.")
    transform_to_custom_schema()
