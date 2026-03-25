import os
from datetime import date
import pandas as pd
import plotly.express as px
import streamlit as st

from log_processing import refresh_once


st.set_page_config(page_title="Machine Alarm Dashboard", layout="wide")

@st.cache_data(ttl=600)
def load_data() -> dict:
    # 从MariaDB拉取并做清洗（复用你现有逻辑）
    return refresh_once(print_status=False)


def _add_line_column(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "line_id" in out.columns:
        out["line"] = out["line_id"].astype(str)
    else:
        # machine_id: SPT1.1_xxx / SPT1.2_xxx
        out["line"] = out["machine_id"].astype(str).str.extract(r"^(SPT\d+\.\d+)", expand=False)
    return out


def _load_rules_csv(uploaded_file, selected_path: str | None) -> pd.DataFrame:
    if uploaded_file is not None:
        rules = pd.read_csv(uploaded_file)
    elif selected_path and os.path.exists(selected_path):
        rules = pd.read_csv(selected_path)
    else:
        return pd.DataFrame()
    rules.columns = [str(col).strip().lower() for col in rules.columns]
    if "error_code" not in rules.columns:
        st.warning("CSV缺少error_code列，已忽略该文件。")
        return pd.DataFrame()

    if "keyword" in rules.columns:
        rules["keyword"] = rules["keyword"].fillna("")
        if "group_label" not in rules.columns:
            rules["group_label"] = rules.get("content", "").fillna("")
    else:
        # Legacy CSVs may only have "content" as the display label.
        rules["keyword"] = ""
        if "group_label" not in rules.columns:
            rules["group_label"] = rules.get("content", "").fillna("")

    if "group_label" in rules.columns:
        rules["group_label"] = rules["group_label"].fillna("")
    else:
        rules["group_label"] = ""
    if "flag" not in rules.columns:
        rules["flag"] = ""

    rules = rules[["error_code", "keyword", "group_label", "flag"]].copy()
    rules["keyword"] = rules["keyword"].fillna("")
    rules["group_label"] = rules["group_label"].fillna("")
    rules["flag"] = rules["flag"].fillna("")
    return rules


def _apply_csv_rules(df: pd.DataFrame, rules: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    if rules.empty:
        return df, {}

    def _split_keywords(value: str) -> list[str]:
        return [part.strip() for part in value.split("|") if part.strip()]

    rules = rules.copy()
    rules["error_code"] = pd.to_numeric(rules["error_code"], errors="coerce")
    rules["error_code"] = rules["error_code"].astype("Int64")
    rules["keyword"] = rules["keyword"].astype(str).str.strip()
    rules["group_label"] = rules["group_label"].astype(str).str.strip()
    rules["flag"] = rules["flag"].astype(str).str.lower().str.strip()
    rules["keyword_key"] = rules["keyword"].str.lower()

    exclude_flags = {"exclude", "x", "0", "false", "no", "drop", "remove"}
    exclude_mask = rules["flag"].isin(exclude_flags)

    exclude_codes = set(
        rules.loc[
            exclude_mask & (rules["keyword"] == "") & rules["error_code"].notna(),
            "error_code",
        ].tolist()
    )
    df_out = df.copy()
    if exclude_codes:
        drop_mask = df_out["error_code"].isin(exclude_codes)
        df_out = df_out.loc[~drop_mask].copy()

    exclude_rules = rules.loc[
        exclude_mask & (rules["keyword"] != ""), ["error_code", "keyword_key"]
    ]
    if not exclude_rules.empty:
        content_key = df_out["内容"].astype(str).str.lower()
        drop_mask = pd.Series(False, index=df_out.index)
        for code, keyword in exclude_rules.itertuples(index=False, name=None):
            if not keyword:
                continue
            keywords = _split_keywords(keyword)
            if pd.isna(code):
                for key in keywords:
                    drop_mask |= content_key.str.contains(key, na=False)
            else:
                code_mask = df_out["error_code"] == int(code)
                for key in keywords:
                    drop_mask |= code_mask & content_key.str.contains(key, na=False)
        df_out = df_out.loc[~drop_mask].copy()

    group_mask = (~exclude_mask) & (rules["group_label"] != "")
    group_rules = rules.loc[
        group_mask, ["error_code", "keyword", "group_label", "keyword_key"]
    ].copy()
    group_rules["keyword_len"] = group_rules["keyword_key"].str.len()
    group_rules = group_rules.sort_values(["error_code", "keyword_len"], ascending=[True, False])
    group_rules_by_code: dict[int, list[tuple[list[str], str]]] = {}
    group_rules_any: list[tuple[list[str], str]] = []
    group_labels_by_code: dict[int, str] = {}
    for code, keyword, label, keyword_key, _ in group_rules.itertuples(index=False, name=None):
        if pd.notna(code) and not keyword:
            group_labels_by_code[int(code)] = label
            continue
        if not keyword_key:
            continue
        keywords = _split_keywords(keyword_key)
        if not keywords:
            continue
        if pd.isna(code):
            group_rules_any.append((keywords, label))
        else:
            group_rules_by_code.setdefault(int(code), []).append((keywords, label))

    if group_rules_by_code or group_rules_any or group_labels_by_code:
        def _match_group(row: pd.Series) -> str:
            if row["error_code"] in group_labels_by_code:
                return group_labels_by_code[row["error_code"]]
            rules_for_code = group_rules_by_code.get(row["error_code"])
            if not rules_for_code:
                rules_for_code = []
            text = str(row["内容"]).lower()
            for keywords, label in rules_for_code:
                if any(key in text for key in keywords):
                    return label
            for keywords, label in group_rules_any:
                if any(key in text for key in keywords):
                    return label
            return row["内容"]

        df_out["内容分组"] = df_out.apply(_match_group, axis=1)
    else:
        df_out["内容分组"] = df_out["内容"]

    info = {
        "excluded_rows": int(len(df) - len(df_out)),
        "grouped_codes": int(group_rules.shape[0]),
    }
    return df_out, info


def main() -> None:
    st.title("Machine Alarm Dashboard")

    data = load_data()
    df = data["df"].copy()  # columns: date, line_id, machine_id, error_code, 内容, occurance
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
        machine_options = ["All"] + sorted(df_for_printers["machine_id"].unique().tolist())
        selected_machine = st.selectbox("Machine", options=machine_options, index=0)

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

        xaxis_mode = st.selectbox(
            "图表维度",
            options=["Action on X (Legend=Date)", "Date on X (Legend=Action)"],
            index=0,
        )

        rules_file = st.file_uploader(
            "CSV规则（error_code, keyword, group_label, flag）",
            type=["csv"],
            help="flag=exclude表示过滤；keyword用于匹配内容；group_label用于分组显示。",
        )

        rules_options = ["None", "Uploaded"]
        if os.path.exists("MW_rules.csv"):
            rules_options.append("MW_rules.csv")
        if os.path.exists("SC_rules.csv"):
            rules_options.append("SC_rules.csv")

        mapped_rules = None
        if selected_line in {"SPT1.1", "SPT1.2"} and "SC_rules.csv" in rules_options:
            mapped_rules = "SC_rules.csv"
        elif selected_line in {"SPT2.1", "SPT2.2"} and "MW_rules.csv" in rules_options:
            mapped_rules = "MW_rules.csv"

        default_rules_index = 1 if "Uploaded" in rules_options else 0
        if mapped_rules in rules_options:
            default_rules_index = rules_options.index(mapped_rules)

        selected_rules = st.selectbox(
            "规则文件选择",
            options=rules_options,
            index=default_rules_index,
            help="选择用于过滤/分组的CSV；Uploaded优先使用上面上传的文件。",
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
    if selected_machine != "All":
        dff = dff[dff["machine_id"] == selected_machine]

    dff = dff[(dff["date"] >= start_date) & (dff["date"] <= end_date)]

    if "内容分组" in dff.columns:
        dff = dff.drop(columns=["内容分组"])

    rules_path = None
    if selected_rules not in {"None", "Uploaded"}:
        rules_path = selected_rules
    rules = _load_rules_csv(rules_file if selected_rules == "Uploaded" else None, rules_path)
    dff, rules_info = _apply_csv_rules(dff, rules)

    if dff.empty:
        st.info("当前筛选条件下没有数据。")
        st.stop()

    # ---- Metrics ----
    total_occ = int(dff["occurance"].sum())
    st.caption(
        "数据来源：MariaDB"
        f"（MARIADB_HOST={os.getenv('MARIADB_HOST', '127.0.0.1')}），当前筛选总occurance={total_occ}"
    )
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

    date_range_label = f"{start_date} to {end_date}"
    machine_label = selected_machine
    title_suffix = f" | Machine: {machine_label} | Dates: {date_range_label}"

    if xaxis_mode == "Date on X (Legend=Action)":
        plot_df = daily_top.copy()
        plot_df["date"] = pd.to_datetime(plot_df["date"]).dt.strftime("%Y-%m-%d")
        fig = px.bar(
            plot_df,
            x="date",
            y="occurance",
            color=content_col,
            barmode="group",
            title=f"Occurrences{title_suffix}",
        )
        fig.update_layout(xaxis_title="date", legend_title_text=content_col)
    else:
        plot_df = daily_top.copy()
        plot_df["date"] = pd.to_datetime(plot_df["date"]).dt.strftime("%Y-%m-%d")
        fig = px.bar(
            plot_df,
            x=content_col,
            y="occurance",
            color="date",
            barmode="group",
            title=f"Top {int(topn)} {content_col} by occurance{title_suffix}",
        )
        fig.update_layout(xaxis_title=content_col, legend_title_text="date")

    fig.update_layout(
        yaxis_title="occurance",
        legend=dict(orientation="v", x=1.02, y=1),
        font=dict(family="Microsoft YaHei, SimHei, Arial Unicode MS"),
        margin=dict(r=200),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("明细（可导出）")
    st.dataframe(
        dff.sort_values(["date", "occurance"], ascending=[False, False]),
        use_container_width=True,
        hide_index=True,
    )


if __name__ == "__main__":
    main()
