import os
from datetime import date
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st

from log_processing import refresh_once


st.set_page_config(page_title="Printer Errors Dashboard", layout="wide")

# Ensure Chinese text renders in Matplotlib on Windows.
plt.rcParams["font.sans-serif"] = [
    "Microsoft YaHei",
    "SimHei",
    "PingFang SC",
    "Noto Sans CJK SC",
    "Arial Unicode MS",
]
plt.rcParams["axes.unicode_minus"] = False


@st.cache_data(ttl=600)
def load_data() -> dict:
    # 从PostgreSQL拉取并做清洗（复用你现有逻辑）
    return refresh_once(print_status=False)


def _add_line_column(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # printer_id: SPT1.1_xxx / SPT1.2_xxx
    out["line"] = out["printer_id"].astype(str).str.extract(r"^(SPT\d+\.\d+)", expand=False)
    return out


def _load_rules_csv(uploaded_file, default_path: str = "error_rules.csv") -> pd.DataFrame:
    if uploaded_file is not None:
        rules = pd.read_csv(uploaded_file)
    elif os.path.exists(default_path):
        rules = pd.read_csv(default_path)
    else:
        return pd.DataFrame()
    rules.columns = [str(col).strip().lower() for col in rules.columns]
    if "error_code" not in rules.columns:
        st.warning("CSV缺少error_code列，已忽略该文件。")
        return pd.DataFrame()

    if "content" not in rules.columns:
        rules["content"] = ""
    if "flag" not in rules.columns:
        rules["flag"] = ""

    rules = rules[["error_code", "content", "flag"]].copy()
    rules["content"] = rules["content"].fillna("")
    rules["flag"] = rules["flag"].fillna("")
    return rules


def _apply_csv_rules(df: pd.DataFrame, rules: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    if rules.empty:
        return df, {}

    rules = rules.copy()
    rules["error_code"] = pd.to_numeric(rules["error_code"], errors="coerce")
    rules = rules.dropna(subset=["error_code"]).copy()
    rules["error_code"] = rules["error_code"].astype(int)
    rules["content"] = rules["content"].astype(str).str.strip()
    rules["flag"] = rules["flag"].astype(str).str.lower().str.strip()

    exclude_flags = {"exclude", "x", "0", "false", "no", "drop", "remove"}
    exclude_mask = rules["flag"].isin(exclude_flags)

    exclude_codes = set(
        rules.loc[exclude_mask & (rules["content"] == ""), "error_code"].tolist()
    )
    exclude_pairs = set(
        zip(
            rules.loc[exclude_mask & (rules["content"] != ""), "error_code"].tolist(),
            rules.loc[exclude_mask & (rules["content"] != ""), "content"].tolist(),
        )
    )

    df_out = df.copy()
    if exclude_codes or exclude_pairs:
        drop_mask = df_out["error_code"].isin(exclude_codes)
        if exclude_pairs:
            drop_mask |= df_out.apply(
                lambda row: (row["error_code"], row["内容"]) in exclude_pairs,
                axis=1,
            )
        df_out = df_out.loc[~drop_mask].copy()

    group_mask = (~exclude_mask) & (rules["content"] != "")
    group_map = (
        rules.loc[group_mask]
        .drop_duplicates(subset=["error_code"], keep="last")
        .set_index("error_code")["content"]
    )

    if not group_map.empty:
        df_out["内容分组"] = df_out["error_code"].map(group_map)
        df_out["内容分组"] = df_out["内容分组"].fillna(df_out["内容"])
    else:
        df_out["内容分组"] = df_out["内容"]

    info = {
        "excluded_rows": int(len(df) - len(df_out)),
        "grouped_codes": int(group_map.shape[0]),
    }
    return df_out, info


def main() -> None:
    st.title("Printer Error Dashboard")

    data = load_data()
    df = data["df"].copy()  # columns: date, printer_id, error_code, 内容, occurance
    if df.empty:
        st.warning("没有查询到数据（df为空）。请检查数据库连接/表数据。")
        st.stop()

    df = _add_line_column(df)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # ---- Sidebar filters ----
    with st.sidebar:
        st.header("筛选")

        line_options = ["All"] + sorted([x for x in df["line"].dropna().unique().tolist()])
        selected_line = st.selectbox("产线", options=line_options, index=0)

        df_for_printers = df if selected_line == "All" else df[df["line"] == selected_line]
        printer_options = ["All"] + sorted(df_for_printers["printer_id"].unique().tolist())
        selected_printer = st.selectbox("Printer", options=printer_options, index=0)

        min_date = df_for_printers["date"].min()
        max_date = df_for_printers["date"].max()
        today = date.today()
        default_date = min(max(today, min_date), max_date)
        date_range = st.date_input(
            "日期范围",
            value=(default_date, default_date),
            min_value=min_date,
            max_value=max_date,
        )

        topn = st.number_input("Top N 内容", min_value=3, max_value=20, value=5, step=1)

        rules_file = st.file_uploader(
            "CSV规则（error_code, content, flag）",
            type=["csv"],
            help="flag=exclude表示过滤；content用于按error_code分组显示。",
        )

        refresh = st.button("手动刷新", help="清除缓存并重新从数据库拉取")
        if refresh:
            load_data.clear()
            st.rerun()

    # Normalize date_range return type
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        # fallback if Streamlit returns single date
        start_date = date_range
        end_date = date_range

    dff = df_for_printers.copy()
    if selected_printer != "All":
        dff = dff[dff["printer_id"] == selected_printer]

    dff = dff[(dff["date"] >= start_date) & (dff["date"] <= end_date)]

    rules = _load_rules_csv(rules_file)
    dff, rules_info = _apply_csv_rules(dff, rules)

    if dff.empty:
        st.info("当前筛选条件下没有数据。")
        st.stop()

    # ---- Metrics ----
    total_occ = int(dff["occurance"].sum())
    st.caption(f"数据来源：PostgreSQL（PGHOST={os.getenv('PGHOST', '10.1.3.102')}），当前筛选总occurance={total_occ}")
    if rules_info:
        st.caption(
            f"CSV规则：过滤{rules_info['excluded_rows']}行；分组{rules_info['grouped_codes']}个error_code。"
        )

    # ---- Chart: daily occurrences split by Top N 内容 ----
    st.subheader(f"按天 Top {int(topn)} 内容（occurance）")

    content_col = "内容分组" if "内容分组" in dff.columns else "内容"
    daily_content = (
        dff.groupby(["date", content_col], as_index=False)["occurance"].sum()
    )

    top_contents = (
        daily_content.groupby([content_col], as_index=False)["occurance"].sum()
        .sort_values("occurance", ascending=False)
        .head(int(topn))
    )
    top_content_set = set(top_contents[content_col].tolist())

    daily_top = daily_content[daily_content[content_col].isin(top_content_set)].copy()
    daily_top["date"] = pd.to_datetime(daily_top["date"])
    daily_top = daily_top.sort_values(["date", "occurance"], ascending=[True, False])

    pivot_top = (
        daily_top
        .pivot_table(index=content_col, columns="date", values="occurance", aggfunc="sum", fill_value=0)
        .sort_index()
    )
    pivot_top.columns = pd.to_datetime(pivot_top.columns).strftime("%Y-%m-%d")

    fig, ax = plt.subplots(figsize=(10, 5))
    pivot_top.plot(kind="bar", stacked=False, ax=ax)
    for container in ax.containers:
        ax.bar_label(container, padding=2, fontsize=8)
    ax.set_title("Daily occurrences by Top 内容")
    ax.set_xlabel(content_col)
    ax.set_ylabel("occurance")
    ax.tick_params(axis="x", labelrotation=45)
    ax.legend(title="date", bbox_to_anchor=(1.02, 1), loc="upper left")
    fig.tight_layout()
    st.pyplot(fig, use_container_width=True)

    st.subheader("明细（可导出）")
    st.dataframe(
        dff.sort_values(["date", "occurance"], ascending=[False, False]),
        use_container_width=True,
        hide_index=True,
    )


if __name__ == "__main__":
    main()
