from __future__ import annotations

import os
import uuid
from datetime import timezone
from typing import Dict, List

import boto3
import pandas as pd
import psycopg2
import streamlit as st

from rag.answer import answer_query

# ============================================================
# Page config
# ============================================================

st.set_page_config(
    page_title="RAG-S3 Knowledge Hub",
    layout="wide",
)

st.title("RAG-S3 Knowledge Hub")
st.caption("Upload documents and ask questions based on your private knowledge base.")

# ============================================================
# Configuration
# ============================================================

BUCKET_NAME = os.getenv("UPLOADS_BUCKET", "uploads")

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "service_s3")
PG_USER = os.getenv("PG_USER", "jovyan")
PG_PASSWORD = os.getenv("PG_PASSWORD", "jovyan")

SUPPORTED_TYPES = ["pdf", "docx", "txt"]

# ============================================================
# Clients
# ============================================================

@st.cache_resource
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
    )


def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB,
    )


s3 = get_s3_client()

# ============================================================
# Helpers
# ============================================================

def ensure_bucket_exists():
    buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if BUCKET_NAME not in buckets:
        s3.create_bucket(Bucket=BUCKET_NAME)


def list_files() -> List[Dict]:
    response = s3.list_objects_v2(Bucket=BUCKET_NAME)
    if "Contents" not in response:
        return []

    files = []
    for obj in response["Contents"]:
        files.append(
            {
                "name": obj["Key"],
                "size_mb": round(obj["Size"] / (1024 * 1024), 2),
                "last_modified": obj["LastModified"].astimezone(timezone.utc),
            }
        )
    return files


def load_processed_files() -> Dict[str, bool]:
    """
    Returns {filename: True} for already processed documents.
    """
    result: Dict[str, bool] = {}

    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT object_key
            FROM document_ingestion
            WHERE bucket_name = %s
            """,
            (BUCKET_NAME,),
        )
        for (key,) in cur.fetchall():
            result[key] = True

        cur.close()
        conn.close()
    except Exception:
        # Table may not exist yet - UI should still work
        return {}

    return result


def delete_file(filename: str):
    s3.delete_object(Bucket=BUCKET_NAME, Key=filename)


ensure_bucket_exists()

# ============================================================
# Tabs
# ============================================================

tab_ask, tab_upload, tab_files = st.tabs(
    ["Ask questions", "Upload documents", "My documents"]
)

# ============================================================
# TAB 1 - Ask questions
# ============================================================

with tab_ask:
    st.subheader("Ask questions about your documents")

    question = st.text_area(
        "Your question",
        placeholder="For example: What is the main purpose of the EU AI Act?",
        height=100,
    )

    col_a, col_b = st.columns([1, 1])

    with col_a:
        answer_style = st.selectbox(
            "Answer style",
            ["Short answer", "Detailed answer"],
            index=0,
        )

    with col_b:
        sources_count = st.slider(
            "Number of sources to use",
            min_value=2,
            max_value=8,
            value=4,
        )

    ask_button = st.button("Get answer", type="primary")

    if ask_button:
        if not question.strip():
            st.warning("Please enter a question.")
        else:
            with st.spinner("Searching your knowledge base..."):
                if answer_style == "Short answer":
                    final_query = question + "\n\nAnswer briefly."
                else:
                    final_query = question + "\n\nProvide a detailed explanation."

                result = answer_query(
                    query=final_query,
                    top_k=sources_count,
                )

            st.markdown("### Answer")
            st.write(result["answer"])

            st.markdown("### Sources")
            if not result.get("sources"):
                st.info("No relevant sources found.")
            else:
                for i, src in enumerate(result["sources"], start=1):
                    st.markdown(
                        f"**{i}. {src.get('file', 'Unknown document')}**"
                    )
                    st.caption(
                        f"Relevance score: {round(float(src.get('score', 0)), 4)}"
                    )

# ============================================================
# TAB 2 - Upload documents
# ============================================================

with tab_upload:
    st.subheader("Upload documents")

    st.caption("Supported formats: PDF, DOCX, TXT")

    uploaded_files = st.file_uploader(
        "Select files",
        type=SUPPORTED_TYPES,
        accept_multiple_files=True,
    )

    keep_names = st.checkbox(
        "Keep original file names",
        value=True,
    )

    upload_button = st.button(
        "Upload files",
        type="primary",
        disabled=not uploaded_files,
    )

    if upload_button:
        success = 0
        failed = []

        with st.spinner("Uploading files..."):
            for file in uploaded_files:
                try:
                    filename = file.name
                    if not keep_names:
                        suffix = uuid.uuid4().hex[:8]
                        if "." in filename:
                            base, ext = filename.rsplit(".", 1)
                            filename = f"{base}_{suffix}.{ext}"
                        else:
                            filename = f"{filename}_{suffix}"

                    s3.upload_fileobj(
                        Fileobj=file,
                        Bucket=BUCKET_NAME,
                        Key=filename,
                        ExtraArgs={"ContentType": file.type},
                    )
                    success += 1
                except Exception:
                    failed.append(file.name)

        if success:
            st.success(f"Uploaded {success} file(s).")
        if failed:
            st.error(f"Failed to upload: {', '.join(failed)}")

        st.info(
            "Uploaded documents will become searchable after background processing."
        )

# ============================================================
# TAB 3 - My documents
# ============================================================

with tab_files:
    st.subheader("My documents")

    processed_map = load_processed_files()
    files = list_files()

    if not files:
        st.info("No documents uploaded yet.")
    else:
        rows = []
        for f in files:
            rows.append(
                {
                    "File name": f["name"],
                    "Size (MB)": f["size_mb"],
                    "Last modified": f["last_modified"],
                    "Status": (
                        "Ready"
                        if processed_map.get(f["name"])
                        else "Processing"
                    ),
                }
            )

        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.divider()

        selected_file = st.selectbox(
            "Select a file to delete",
            options=[""] + [f["name"] for f in files],
            index=0,
        )

        if st.button(
            "Delete selected file",
            disabled=not selected_file,
        ):
            try:
                delete_file(selected_file)
                st.success("File deleted.")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to delete file: {e}")
