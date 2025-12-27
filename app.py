import os
import pandas as pd
import plotly.express as px
import streamlit as st

from log_processing import refresh_once


st.set_page_config(page_title="Printer Errors Dashboard", layout="wide")


@st.cache_data(ttl=600)
def load_data() -> dict:
    # 从PostgreSQL拉取并做清洗（复用你现有逻辑）
    return refresh_once(print_status=False)


def _add_line_column(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # printer_id: SPT1.1_xxx / SPT1.2_xxx
    out["line"] = out["printer_id"].astype(str).str.extract(r"^(SPT\d+\.\d+)", expand=False)
    return out


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
        date_range = st.date_input("日期范围", value=(min_date, max_date), min_value=min_date, max_value=max_date)

        topn = st.number_input("Top N error_code", min_value=3, max_value=20, value=5, step=1)

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

    if dff.empty:
        st.info("当前筛选条件下没有数据。")
        st.stop()

    # ---- Metrics ----
    total_occ = int(dff["occurance"].sum())
    st.caption(f"数据来源：PostgreSQL（PGHOST={os.getenv('PGHOST', '10.1.3.102')}），当前筛选总occurance={total_occ}")

    # ---- Charts ----
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("按天统计（occurance）")
        daily = (
            dff.groupby(["date"], as_index=False)["occurance"].sum()
            .sort_values("date")
        )
        fig_daily = px.bar(daily, x="date", y="occurance", title="Daily occurrences")
        fig_daily.update_layout(margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig_daily, use_container_width=True)

    with col2:
        st.subheader(f"Top {int(topn)} 内容")
        top_contents = (
            dff.groupby(["内容"], as_index=False)["occurance"].sum()
            .sort_values("occurance", ascending=False)
            .head(int(topn))
        )
        fig_top = px.bar(top_contents, x="内容", y="occurance", title="Top 内容")
        fig_top.update_layout(margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig_top, use_container_width=True)

    st.subheader("明细（可导出）")
    st.dataframe(
        dff.sort_values(["date", "occurance"], ascending=[False, False]),
        use_container_width=True,
        hide_index=True,
    )


if __name__ == "__main__":
    main()
