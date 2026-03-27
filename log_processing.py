
import argparse
import os
import sys
import time
from typing import Any

import pandas as pd
import pymysql

DEFAULT_QUERY = """
WITH AggregatedAlarms AS (
    SELECT
        DATE(start_time) AS date,
        alarm_codes AS error_code,
        line_id,
        machine,
        COUNT(*) AS occurance
    FROM tester_alarm
    -- A WHERE clause should go here (see point 2)
    GROUP BY
        DATE(start_time),
        line_id, 
        machine,
        alarm_codes       
       
)
SELECT 
    A.date,
    A.line_id,
    A.machine,
    A.error_code,
    L.alarm_cn AS 内容,
    A.occurance
FROM AggregatedAlarms A
LEFT JOIN tester_alarm_list L 
    ON A.error_code = L.alarm_code 
    AND A.machine = L.machine;
"""


def _get_mariadb_connection() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=os.getenv("MARIADB_HOST", "192.168.3.139"),
        user=os.getenv("MARIADB_USER", "sorterbin"),
        password=os.getenv("MARIADB_PASSWORD", "sorterbin123"),
        database=os.getenv("MARIADB_DATABASE", "machine_status_db"),
        port=int(os.getenv("MARIADB_PORT", "3306")),
        charset="utf8mb4",
    )


def _resolve_column(df: pd.DataFrame, name: str) -> str | None:
    columns_by_lower = {str(col).lower(): col for col in df.columns}
    return columns_by_lower.get(name.lower())


def fetch_printer_summary_df() -> pd.DataFrame:
    query = os.getenv("MARIADB_ALARM_QUERY", DEFAULT_QUERY).strip()
    if not query:
        raise ValueError("MARIADB_ALARM_QUERY is empty; please provide a valid SQL query.")

    cnx = _get_mariadb_connection()
    try:
        with cnx.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    finally:
        cnx.close()


def _ensure_text(val: Any) -> Any:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return val
    if isinstance(val, memoryview):
        val = val.tobytes()
    if isinstance(val, (bytes, bytearray)):
        return val.decode("utf-8", errors="replace")
    return val if isinstance(val, str) else str(val)


def _normalize_alarm_df(df_log: pd.DataFrame) -> pd.DataFrame:
    col_date = os.getenv("ALARM_COL_DATE", "date")
    col_machine = os.getenv("ALARM_COL_MACHINE", "machine")
    col_code = os.getenv("ALARM_COL_CODE", "error_code")
    col_message = os.getenv("ALARM_COL_MESSAGE", "内容")
    col_line = os.getenv("ALARM_COL_LINE", "line_id")
    col_occ = os.getenv("ALARM_COL_OCCURANCE", "occurance")

    resolved = {
        "date": _resolve_column(df_log, col_date),
        "machine_id": _resolve_column(df_log, col_machine),
        "error_code": _resolve_column(df_log, col_code),
        "内容": _resolve_column(df_log, col_message),
        "line_id": _resolve_column(df_log, col_line),
        "occurance": _resolve_column(df_log, col_occ) if col_occ else None,
    }

    missing = [
        key
        for key, value in resolved.items()
        if key not in {"occurance"} and value is None
    ]
    if missing:
        available = ", ".join([str(c) for c in df_log.columns])
        raise ValueError(f"Missing columns {missing}. Available columns: {available}")

    df = df_log.rename(
        columns={
            resolved["date"]: "date",
            resolved["machine_id"]: "machine_id",
            resolved["error_code"]: "error_code",
            resolved["内容"]: "内容",
            resolved["line_id"]: "line_id",
        }
    ).copy()

    if resolved["occurance"] is not None:
        df = df.rename(columns={resolved["occurance"]: "occurance"})
    else:
        df["occurance"] = 0

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["内容"] = df["内容"].apply(_ensure_text)
    df["error_code"] = pd.to_numeric(df["error_code"], errors="coerce").astype("Int64")
    df["machine_id"] = df["machine_id"].astype(str)
    df["line_id"] = df["line_id"].astype(str)

    return df[["date", "line_id", "machine_id", "error_code", "内容", "occurance"]].copy()


def transform_printer_summary(df_log: pd.DataFrame) -> dict:
    df = _normalize_alarm_df(df_log)

    df_11 = df[df["machine_id"].str.startswith("SPT1.1_")].copy()
    df_12 = df[df["machine_id"].str.startswith("SPT1.2_")].copy()

    top4_ids_11 = df_11.groupby("machine_id")["occurance"].sum().nlargest(4).index.tolist()
    top4_ids_12 = df_12.groupby("machine_id")["occurance"].sum().nlargest(4).index.tolist()

    dfs_by_printer_11 = {pid: df_11.loc[df_11["machine_id"] == pid].copy() for pid in top4_ids_11}
    dfs_by_printer_12 = {pid: df_12.loc[df_12["machine_id"] == pid].copy() for pid in top4_ids_12}

    def _get_df(dfs_dict, ids, idx):
        return dfs_dict[ids[idx]] if len(ids) > idx else pd.DataFrame(columns=df.columns)

    df_spt11_printer_4 = _get_df(dfs_by_printer_11, top4_ids_11, 0)
    df_spt11_printer_2 = _get_df(dfs_by_printer_11, top4_ids_11, 1)
    df_spt11_printer_3 = _get_df(dfs_by_printer_11, top4_ids_11, 2)
    df_spt11_printer_1 = _get_df(dfs_by_printer_11, top4_ids_11, 3)

    df_spt12_printer_4 = _get_df(dfs_by_printer_12, top4_ids_12, 0)
    df_spt12_printer_2 = _get_df(dfs_by_printer_12, top4_ids_12, 1)
    df_spt12_printer_3 = _get_df(dfs_by_printer_12, top4_ids_12, 2)
    df_spt12_printer_1 = _get_df(dfs_by_printer_12, top4_ids_12, 3)

    return {
        "df_log": df_log,
        "df": df,
        "df_11": df_11,
        "df_12": df_12,
        "top4_ids_11": top4_ids_11,
        "top4_ids_12": top4_ids_12,
        "df_spt11_printer_1": df_spt11_printer_1,
        "df_spt11_printer_2": df_spt11_printer_2,
        "df_spt11_printer_3": df_spt11_printer_3,
        "df_spt11_printer_4": df_spt11_printer_4,
        "df_spt12_printer_1": df_spt12_printer_1,
        "df_spt12_printer_2": df_spt12_printer_2,
        "df_spt12_printer_3": df_spt12_printer_3,
        "df_spt12_printer_4": df_spt12_printer_4,
    }


def refresh_once(print_status: bool = True) -> dict:
    df_log = fetch_printer_summary_df()
    result = transform_printer_summary(df_log)
    if print_status:
        print(
            f"✅ Refreshed from MariaDB: df rows={len(result['df'])}, "
            f"top SPT1.1={result['top4_ids_11']}, top SPT1.2={result['top4_ids_12']}"
        )
    return result


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description='Fetch + transform printer data from MariaDB')
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

