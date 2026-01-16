import pandas as pd
import streamlit as st

import db
import generator


def _derive_run_status(ok_records: int, fail_records: int) -> str:
    return "OK" if ok_records >= fail_records else "WARN"


def _load_history():
    runs = db.get_runs(limit=50)
    if not runs:
        return pd.DataFrame()

    history_df = pd.DataFrame(runs)
    history_df["status"] = history_df.apply(
        lambda row: _derive_run_status(row["ok_records"], row["fail_records"]),
        axis=1,
    )
    history_df = history_df.rename(
        columns={
            "created_at": "Timestamp",
            "status": "Status",
            "total_records": "Total records",
            "ok_records": "OK records",
            "warn_records": "WARN records",
            "fail_records": "FAIL records",
        }
    )
    return history_df[
        [
            "Timestamp",
            "Status",
            "Total records",
            "OK records",
            "WARN records",
            "FAIL records",
        ]
    ]


def _load_records(run_id: int):
    records = db.get_records_for_run(run_id)
    if not records:
        return pd.DataFrame()

    records_df = pd.DataFrame(records).rename(
        columns={
            "sensor_id": "source_id",
            "created_at": "timestamp",
        }
    )
    return records_df[
        ["id", "run_id", "source_id", "value", "status", "timestamp"]
    ]


def main() -> None:
    st.set_page_config(page_title="Data Collection & Monitoring – Demo Module", layout="wide")
    st.title("Data Collection & Monitoring – Demo Module")
    st.write(
        "Simulated end-to-end data collection, processing, persistence and visualization."
    )

    db.init_db()

    st.sidebar.header("Simulation parameters")
    min_records = st.sidebar.slider("Min records", 5, 20, 10)
    max_records = st.sidebar.slider("Max records", 10, 50, 30)
    ok_ratio = st.sidebar.slider("OK probability", 0.5, 0.95, 0.75)
    warn_ratio = st.sidebar.slider("WARN probability", 0.0, 0.4, 0.2)

    if ok_ratio + warn_ratio > 0.98:
        st.sidebar.info("Reduce OK/WARN to leave room for FAIL records.")

    run_clicked = st.sidebar.button("Run simulation", type="primary")

    if run_clicked:
        run_summary, records = generator.generate_run_data(
            min_records=min_records,
            max_records=max_records,
            ok_ratio=ok_ratio,
            warn_ratio=warn_ratio,
        )
        run_id = db.create_run(**run_summary)
        db.add_records(run_id, records)
        st.session_state["latest_run_id"] = run_id

    latest_run_id = st.session_state.get("latest_run_id")
    latest_run = db.get_latest_run()
    if latest_run:
        latest_run_id = latest_run["id"]
        st.session_state["latest_run_id"] = latest_run_id

    col1, col2, col3, col4 = st.columns(4)
    if latest_run:
        derived_status = _derive_run_status(
            latest_run["ok_records"], latest_run["fail_records"]
        )
        col1.metric("Status", derived_status)
        col2.metric("Total records", latest_run["total_records"])
        col3.metric("OK records", latest_run["ok_records"])
        col4.metric("WARN records", latest_run["warn_records"])
        st.caption(f'Fail records: {latest_run["fail_records"]}')
    else:
        col1.metric("Status", "-")
        col2.metric("Total records", "0")
        col3.metric("OK records", "0")
        col4.metric("WARN records", "0")
        st.caption("Fail records: 0")

    st.subheader("Run history")
    history_df = _load_history()
    if history_df.empty:
        st.info("No runs recorded yet.")
    else:
        st.dataframe(history_df, use_container_width=True)

    st.subheader("Latest run results")
    if latest_run_id:
        records_df = _load_records(latest_run_id)
        if records_df.empty:
            st.info("No records found for this run.")
        else:
            st.dataframe(records_df, use_container_width=True)
    else:
        st.info("Run a simulation to view results.")


if __name__ == "__main__":
    main()
