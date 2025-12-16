import duckdb
import requests
import os
import psycopg2
from typing import List, Tuple
import datetime
import sys

# ===================================================================
# ① 設定
# ===================================================================
STATIC_DATA_ENDPOINTS: List[Tuple[str, str, str | None]] = [
    ("calendar", "https://api.odpt.org/api/v4/odpt:Calendar.json", None),
    ("operator", "https://api.odpt.org/api/v4/odpt:Operator.json", None),
    ("passengersurvey", "https://api.odpt.org/api/v4/odpt:PassengerSurvey.json", None),
    ("railway", "https://api.odpt.org/api/v4/odpt:Railway.json", None),
    ("raildirection", "https://api.odpt.org/api/v4/odpt:RailDirection.json", None),
    ("railwayfare", "https://api.odpt.org/api/v4/odpt:RailwayFare.json", None),
    ("station", "https://api.odpt.org/api/v4/odpt:Station.json", None),
    ("stationtimetable", "https://api.odpt.org/api/v4/odpt:StationTimetable.json", None),
    ("traintimetable", "https://api.odpt.org/api/v4/odpt:TrainTimetable.json", None),
    ("traintype", "https://api.odpt.org/api/v4/odpt:TrainType.json", None),
    ("stops", "https://api.odpt.org/api/v4/odpt:BusstopPole.json", "odpt:BusstopPole"),
    ("busroutepattern", "https://api.odpt.org/api/v4/odpt:BusroutePattern.json", None),
    ("busstoptimetable", "https://api.odpt.org/api/v4/odpt:BusstopPoleTimetable.json", "odpt:BusstopPoleTimetable"),
    ("busfare", "https://api.odpt.org/api/v4/odpt:BusroutePatternFare.json", None),
]

OUTPUT_DIR = "output"
TOEI_OPERATOR = "odpt.Operator:Toei"

# ===================================================================
# ② データダウンロード
# ===================================================================
def download_data(endpoint_name: str, url: str) -> str:
    params = {'acl:consumerKey': os.environ["ODPT_API_KEY"]}
    print(f"Downloading {endpoint_name}...")
    response = requests.get(url, params=params)
    response.raise_for_status()

    file_path = os.path.join(OUTPUT_DIR, f"{endpoint_name}_raw.json")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(response.text)
    print(f"Saved to {file_path}")
    return file_path

# ===================================================================
# ③ DuckDB処理（Toeiフィルタ + Parquet圧縮）
# ===================================================================
def process_and_compress_data(json_path: str, table_name: str):
    con = duckdb.connect(database=':memory:')
    
    # JSON読み込み（配列の場合は自動で1行ずつ）
    con.execute(f"""
        CREATE OR REPLACE TABLE raw AS
        SELECT * FROM read_json('{json_path}', format='array');
    """)

    # Toeiフィルタ共通部分（operatorが配列 or 単一文字列 or NULLの場合に対応）
    filter_clause = f"""
        WHERE COALESCE(
            (SELECT COUNT(1) > 0 FROM UNNEST(raw."odpt:operator") AS t(op) WHERE op LIKE '%{TOEI_OPERATOR}%'),
            raw."odpt:operator" LIKE '%{TOEI_OPERATOR}%',
            TRUE  -- operatorがない場合は許可（例: calendarなど）
        )
    """

    # operatorテーブルはフィルタしない
    if table_name == "operator":
        filter_clause = ""

    con.execute(f"""
        CREATE OR REPLACE TABLE processed AS
        SELECT *, current_timestamp AS loaded_at
        FROM raw
        {filter_clause}
    """)

    parquet_path = os.path.join(OUTPUT_DIR, f"{table_name}.parquet")
    con.execute(f"""
        COPY processed TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
    """)
    print(f"Compressed to {parquet_path}")
    con.close()
    return parquet_path

# ===================================================================
# ④ Supabaseへ全テーブル投入
# ===================================================================
def upload_all_to_supabase():
    db_url = os.environ["SUPABASE_DB_URL"]  # postgres://user:pass@host:5432/postgres 形式を推奨
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    cur = conn.cursor()

    for table_name, _, _ in STATIC_DATA_ENDPOINTS:
        parquet_path = os.path.join(OUTPUT_DIR, f"{table_name}.parquet")
        if not os.path.exists(parquet_path):
            print(f"Skip {table_name} (no parquet)")
            continue

        # DuckDBでParquet → CSV（一時的にメモリ上で）
        duckdb.sql(f"""
            COPY (SELECT * FROM read_parquet('{parquet_path}'))
            TO '/tmp/{table_name}.csv' (HEADER, DELIMITER ',');
        """)

        # Supabase側テーブルをTRUNCATE
        cur.execute(f'TRUNCATE TABLE public."{table_name}" RESTART IDENTITY;')

        # COPY投入
        with open(f'/tmp/{table_name}.csv', 'r', encoding='utf-8') as f:
            cur.copy_expert(f"COPY public.\"{table_name}\" FROM STDIN WITH CSV HEADER", f)

        print(f"Uploaded {table_name} to Supabase")

    cur.close()
    conn.close()

# ===================================================================
# ⑤ メイン
# ===================================================================
def main():
    action = sys.argv[1] if len(sys.argv) > 1 else "all"

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if action in ["all", "download"]:
        if not os.environ.get("ODPT_API_KEY"):
            raise ValueError("ODPT_API_KEY is required")

        for table_name, url, _ in STATIC_DATA_ENDPOINTS:
            start = datetime.datetime.now()
            print(f"\n=== Processing {table_name} ===")
            json_path = download_data(table_name, url)
            process_and_compress_data(json_path, table_name)
            print(f"Completed in {(datetime.datetime.now() - start).total_seconds():.1f}s")

    if action in ["all", "upload"]:
        if not os.environ.get("SUPABASE_DB_URL"):
            raise ValueError("SUPABASE_DB_URL is required")
        upload_all_to_supabase()

if __name__ == "__main__":
    main()
