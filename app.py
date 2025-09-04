import re
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

# ====== CONFIG ======
PROJECT_ID = "disco-sector-423110-r6"
DATASET_ID = "raw_new"
TABLE_NAME = "authorize_net_transactions"

# ====== AUTHENTICATION (via Streamlit Secrets) ======
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# ====== HELPER FUNCTION: Clean column names ======
def clean_column(col: str) -> str:
    col = col.strip().lower()
    col = col.replace(" ", "_").replace("-", "_").replace("/", "_")
    col = col.replace("#", "number")
    col = col.replace("__", "_")
    return col

# ====== DATE PARSING ======
def parse_datetime_with_tz(series):
    series_clean = series.astype(str).apply(
        lambda x: re.sub(r"\b[A-Z]{2,4}\b", "", x).strip()
    )
    return pd.to_datetime(series_clean, errors="coerce").dt.date  # force DATE not DATETIME

def parse_date(series):
    series_clean = series.astype(str).apply(
        lambda x: re.sub(r"\b[A-Z]{2,4}\b", "", x).strip()
    )
    return pd.to_datetime(series_clean, errors="coerce").dt.date

# ====== STREAMLIT UI ======
st.set_page_config(page_title="BigQuery File Uploader", page_icon="📂")
st.title("📂 Upload .net file")

uploaded_file = st.file_uploader("Drag & Drop a CSV or Excel file", type=["csv", "xlsx"])

if uploaded_file is not None:
    # Detect file type
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file, dtype=str, low_memory=False)
    else:
        df = pd.read_excel(uploaded_file, dtype=str)

    # Clean columns
    df.columns = [clean_column(c) for c in df.columns]

    # Convert numeric fields
    numeric_fields = [
        "settlement_amount", "authorization_amount", "total_amount",
        "reserved15", "reserved16", "reserved17", "reserved18", "reserved19", "reserved20"
    ]
    for col in numeric_fields:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Convert to DATE fields
    for col in ["settlement_date_time", "submit_date_time"]:
        if col in df.columns:
            df[col] = parse_datetime_with_tz(df[col])

    if "business_day" in df.columns:
        df["business_day"] = parse_date(df["business_day"])

    # Reserved columns as INT64
    for col in ["reserved15", "reserved16", "reserved17", "reserved18", "reserved19", "reserved20"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # ✅ Force critical numeric-looking fields to STRING
    string_columns = ["bank_account_number", "routing_number", "card_number", "customer_id", "invoice_number"]
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # ✅ Add load_time column with current timestamp
    df["load_time"] = datetime.utcnow()

    st.write("✅ Preview of Data:", df.head())

    if st.button("🚀 Load to BigQuery"):
        try:
            table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

            # Explicit schema (DATE + INT64 + load_time)
            schema = [
                bigquery.SchemaField("transaction_id", "STRING"),
                bigquery.SchemaField("transaction_status", "STRING"),
                bigquery.SchemaField("settlement_amount", "FLOAT64"),
                bigquery.SchemaField("settlement_currency", "STRING"),
                bigquery.SchemaField("settlement_date_time", "DATE"),
                bigquery.SchemaField("authorization_amount", "FLOAT64"),
                bigquery.SchemaField("authorization_currency", "STRING"),
                bigquery.SchemaField("submit_date_time", "DATE"),
                bigquery.SchemaField("authorization_code", "STRING"),
                bigquery.SchemaField("reference_transaction_id", "STRING"),
                bigquery.SchemaField("transaction_type", "STRING"),
                bigquery.SchemaField("address_verification_status", "STRING"),
                bigquery.SchemaField("card_code_status", "STRING"),
                bigquery.SchemaField("fraudscreen_applied", "STRING"),
                bigquery.SchemaField("recurring_billing_transaction", "STRING"),
                bigquery.SchemaField("partial_capture_status", "STRING"),
                bigquery.SchemaField("card_number", "STRING"),
                bigquery.SchemaField("expiration_date", "STRING"),
                bigquery.SchemaField("bank_account_number", "STRING"),
                bigquery.SchemaField("routing_number", "STRING"),
                bigquery.SchemaField("total_amount", "FLOAT64"),
                bigquery.SchemaField("currency", "STRING"),
                bigquery.SchemaField("invoice_number", "STRING"),
                bigquery.SchemaField("invoice_description", "STRING"),
                bigquery.SchemaField("customer_first_name", "STRING"),
                bigquery.SchemaField("customer_last_name", "STRING"),
                bigquery.SchemaField("company", "STRING"),
                bigquery.SchemaField("address", "STRING"),
                bigquery.SchemaField("city", "STRING"),
                bigquery.SchemaField("state", "STRING"),
                bigquery.SchemaField("zip", "STRING"),
                bigquery.SchemaField("country", "STRING"),
                bigquery.SchemaField("phone", "STRING"),
                bigquery.SchemaField("fax", "STRING"),
                bigquery.SchemaField("email", "STRING"),
                bigquery.SchemaField("customer_id", "STRING"),
                bigquery.SchemaField("ship_to_first_name", "STRING"),
                bigquery.SchemaField("ship_to_last_name", "STRING"),
                bigquery.SchemaField("ship_to_company", "STRING"),
                bigquery.SchemaField("ship_to_address", "STRING"),
                bigquery.SchemaField("ship_to_city", "STRING"),
                bigquery.SchemaField("ship_to_state", "STRING"),
                bigquery.SchemaField("ship_to_zip", "STRING"),
                bigquery.SchemaField("ship_to_country", "STRING"),
                bigquery.SchemaField("l2__tax", "STRING"),
                bigquery.SchemaField("l2__freight", "STRING"),
                bigquery.SchemaField("l2__tax_exempt", "STRING"),
                bigquery.SchemaField("l2__purchase_order_number", "STRING"),
                bigquery.SchemaField("cavv_results_code", "STRING"),
                bigquery.SchemaField("business_day", "DATE"),
                bigquery.SchemaField("order_number", "STRING"),
                bigquery.SchemaField("available_card_balance", "STRING"),
                bigquery.SchemaField("approved_amount", "STRING"),
                bigquery.SchemaField("market_type", "STRING"),
                bigquery.SchemaField("product", "STRING"),
                bigquery.SchemaField("reserved7", "STRING"),
                bigquery.SchemaField("reserved8", "STRING"),
                bigquery.SchemaField("reserved9", "STRING"),
                bigquery.SchemaField("reserved10", "STRING"),
                bigquery.SchemaField("reserved11", "STRING"),
                bigquery.SchemaField("reserved12", "STRING"),
                bigquery.SchemaField("reserved13", "STRING"),
                bigquery.SchemaField("reserved14", "STRING"),
                bigquery.SchemaField("reserved15", "INT64"),
                bigquery.SchemaField("reserved16", "INT64"),
                bigquery.SchemaField("reserved17", "INT64"),
                bigquery.SchemaField("reserved18", "INT64"),
                bigquery.SchemaField("reserved19", "INT64"),
                bigquery.SchemaField("reserved20", "INT64"),
                bigquery.SchemaField("load_time", "DATETIME"),  # ✅ new column
            ]

            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )

            load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            load_job.result()

            st.success(f"✅ Data successfully appended to `{table_ref}` with load_time")
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
            st.write("⚠️ Debugging Data Types:")
            st.write(df.dtypes)
            st.write("⚠️ Sample Data:")
            st.write(df.head())
