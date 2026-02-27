
import os
import sys
import time
from typing import Any, Callable

import pandas as pd

# psycopg2 (v2) often fails on newer Python (e.g., 3.13) on Windows with
# "No module named 'psycopg2._psycopg'" due to missing/unsupported binary wheels.
# Prefer psycopg2 when it imports cleanly; otherwise fall back to psycopg (v3).
_pg_connect: Callable[..., Any]
_pg_driver: str
try:
    import psycopg2  # type: ignore

    _pg_connect = psycopg2.connect
    _pg_driver = 'psycopg2'
except Exception:  # noqa: BLE001
    import psycopg  # type: ignore

    _pg_connect = psycopg.connect
    _pg_driver = 'psycopg'


QUERY = '''
SELECT 
    时间::DATE AS error_date,   -- Converts timestamp to YYYY-MM-DD
    printer_id, 
    内容, 
    COUNT(*) AS occurance
FROM 
    public.printer_almlist
GROUP BY 
    时间::DATE,   -- You must group by the converted date
    printer_id, 
    内容
ORDER BY 
    error_date DESC, 
    occurance DESC;
'''


def _get_pg_connection() -> Any:
    host = os.getenv('PGHOST', '10.1.3.102')
    database = os.getenv('PGDATABASE', 'postgres')
    user = os.getenv('PGUSER', 'printer_data')
    password = os.getenv('PGPASSWORD', 'printer_data')
    port = int(os.getenv('PGPORT', '5432'))

    kwargs: dict[str, Any] = {
        'host': host,
        'user': user,
        'password': password,
        'port': port,
    }
    # psycopg (v3) uses libpq keyword 'dbname'; psycopg2 commonly uses 'database'.
    kwargs['dbname' if _pg_driver == 'psycopg' else 'database'] = database
    return _pg_connect(**kwargs)


def fetch_printer_summary_df() -> pd.DataFrame:
    cnx = _get_pg_connection()
    try:
        with cnx.cursor() as cursor:
            cursor.execute("SET client_encoding TO 'UTF8'")
            cursor.execute(QUERY)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        df_log = pd.DataFrame(rows, columns=columns)
        return df_log
    finally:
        cnx.close()


def transform_printer_summary(df_log: pd.DataFrame) -> dict:
    # --- Regex Extraction ---
    # Note: I kept your regex `[:：],\s*`.
    # Ensure your data actually has "Colon, Comma" (e.g., "Error:, 123").
    # If it is just "Error: 123", remove the comma from regex.
    pattern = r'[:：]\s*,?\s*(\d{3})'

    df_log_with_code = df_log.copy()
    df_log_with_code['error_date'] = pd.to_datetime(df_log_with_code['error_date'], errors='coerce')
    def _ensure_text(val: Any) -> Any:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return val
        if isinstance(val, memoryview):
            val = val.tobytes()
        if isinstance(val, (bytes, bytearray)):
            return val.decode('utf-8', errors='replace')
        return val if isinstance(val, str) else str(val)

    df_log_with_code['内容'] = df_log_with_code['内容'].apply(_ensure_text)
    df_log_with_code['error_code'] = df_log_with_code['内容'].str.extract(pattern, expand=False)
    df_log_with_code['error_code'] = pd.to_numeric(
        df_log_with_code['error_code'], errors='coerce'
    ).astype('Int64')

    # Keep original columns and rename error_date -> date
    df = df_log_with_code[['error_date', 'printer_id', 'error_code', '内容', 'occurance']].copy()
    df.rename(columns={'error_date': 'date'}, inplace=True)

    # Remove embedded error_code from 内容 and trim
    df['内容'] = df['内容'].str.replace(pattern, '', regex=True).str.strip()

    # --- FIX 1: Filter using 'df', not 'df_log' to avoid index mismatch ---
    df_11 = df[df['printer_id'].str.startswith('SPT1.1_')].copy()
    df_12 = df[df['printer_id'].str.startswith('SPT1.2_')].copy()

    # --- FIX 2: Find top printers by SUM of occurrences, not count of rows ---
    # This finds the printers that actually had the most errors
    top4_ids_11 = df_11.groupby('printer_id')['occurance'].sum().nlargest(4).index.tolist()
    top4_ids_12 = df_12.groupby('printer_id')['occurance'].sum().nlargest(4).index.tolist()

    # Build dicts for quick access
    dfs_by_printer_11 = {pid: df_11.loc[df_11['printer_id'] == pid].copy() for pid in top4_ids_11}
    dfs_by_printer_12 = {pid: df_12.loc[df_12['printer_id'] == pid].copy() for pid in top4_ids_12}

    # --- FIX 3: Ensure empty fallback uses 'df.columns', not 'df_log.columns' ---
    def _get_df(dfs_dict, ids, idx):
        # Returns the specific DF or an empty one with the CORRECT columns if missing
        return dfs_dict[ids[idx]] if len(ids) > idx else pd.DataFrame(columns=df.columns)

    # Create 4 DataFrames for SPT1.1
    df_spt11_printer_4 = _get_df(dfs_by_printer_11, top4_ids_11, 0)
    df_spt11_printer_2 = _get_df(dfs_by_printer_11, top4_ids_11, 1)
    df_spt11_printer_3 = _get_df(dfs_by_printer_11, top4_ids_11, 2)
    df_spt11_printer_1 = _get_df(dfs_by_printer_11, top4_ids_11, 3)

    # Create 4 DataFrames for SPT1.2
    df_spt12_printer_4 = _get_df(dfs_by_printer_12, top4_ids_12, 0)
    df_spt12_printer_2 = _get_df(dfs_by_printer_12, top4_ids_12, 1)
    df_spt12_printer_3 = _get_df(dfs_by_printer_12, top4_ids_12, 2)
    df_spt12_printer_1 = _get_df(dfs_by_printer_12, top4_ids_12, 3)

    return {
        'df_log': df_log,
        'df': df,
        'df_11': df_11,
        'df_12': df_12,
        'top4_ids_11': top4_ids_11,
        'top4_ids_12': top4_ids_12,
        'df_spt11_printer_1': df_spt11_printer_1,
        'df_spt11_printer_2': df_spt11_printer_2,
        'df_spt11_printer_3': df_spt11_printer_3,
        'df_spt11_printer_4': df_spt11_printer_4,
        'df_spt12_printer_1': df_spt12_printer_1,
        'df_spt12_printer_2': df_spt12_printer_2,
        'df_spt12_printer_3': df_spt12_printer_3,
        'df_spt12_printer_4': df_spt12_printer_4,
    }


def refresh_once(print_status: bool = True) -> dict:
    df_log = fetch_printer_summary_df()
    result = transform_printer_summary(df_log)
    if print_status:
        print(
            f"✅ Refreshed from PostgreSQL: df rows={len(result['df'])}, "
            f"top SPT1.1={result['top4_ids_11']}, top SPT1.2={result['top4_ids_12']}"
        )
    return result


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description='Fetch + transform printer data from PostgreSQL')
    parser.add_argument('--watch', action='store_true', help='Continuously refresh from DB')
    parser.add_argument('--interval', type=float, default=3600.0, help='Refresh interval in seconds (watch mode)')
    args = parser.parse_args(argv)

    if not args.watch:
        # Preserve notebook %run behavior by exporting variables at module scope.
        globals().update(refresh_once(print_status=True))
        return 0

    interval = max(1.0, args.interval)
    while True:
        start = time.time()
        try:
            globals().update(refresh_once(print_status=True))
        except Exception as e:
            print(f"Error refreshing printer data: {e}")

        elapsed = time.time() - start
        time.sleep(max(0.0, interval - elapsed))


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))

