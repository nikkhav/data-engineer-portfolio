"""
Vectorisation task for RAG ingestion pipeline.

Responsibilities:
- download documents from S3 (MinIO)
- extract text from PDF / DOCX / TXT
- split text into chunks
- generate embeddings
- store embeddings with metadata in ChromaDB
"""

from __future__ import annotations

import logging
import io
import os

import boto3
import chromadb
from pypdf import PdfReader
from docx import Document
from sentence_transformers import SentenceTransformer


# -----------------------------
# Configuration
# -----------------------------

CHUNK_SIZE = 3000
CHUNK_OVERLAP = 500
MAX_CHUNKS = 1000
EMBEDDING_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
CHROMA_PERSIST_DIR = "/opt/chroma"
os.environ["ANONYMIZED_TELEMETRY"] = "False"

# -----------------------------
# Embedding model loader
# -----------------------------

_embedding_model: SentenceTransformer | None = None


def _get_embedding_model() -> SentenceTransformer:
    global _embedding_model
    if _embedding_model is None:
        _embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    return _embedding_model


# -----------------------------
# Text extraction helpers
# -----------------------------

def _extract_text_from_pdf(file_bytes: bytes) -> str:
    reader = PdfReader(io.BytesIO(file_bytes))
    texts = []
    for page in reader.pages:
        text = page.extract_text()
        if text:
            texts.append(text)
    return "\n".join(texts)


def _extract_text_from_docx(file_bytes: bytes) -> str:
    doc = Document(io.BytesIO(file_bytes))
    return "\n".join(p.text for p in doc.paragraphs if p.text)


def _extract_text_from_txt(file_bytes: bytes) -> str:
    return file_bytes.decode("utf-8", errors="ignore")


# -----------------------------
# Chunking
# -----------------------------

def _chunk_text(
    text: str,
    chunk_size: int = CHUNK_SIZE,
    overlap: int = CHUNK_OVERLAP,
) -> list[str]:
    chunks = []
    start = 0
    text_length = len(text)

    while start < text_length:
        end = start + chunk_size
        chunks.append(text[start:end])
        start = end - overlap

    return chunks


# -----------------------------
# ChromaDB persistence
# -----------------------------

def _store_chunks_in_chroma(chunks: list[str], metadata: dict):
    client = chromadb.PersistentClient(
        path=CHROMA_PERSIST_DIR,
    )

    collection = client.get_or_create_collection(
        name="documents"
    )

    model = SentenceTransformer(
        "sentence-transformers/all-MiniLM-L6-v2"
    )

    embeddings = model.encode(chunks).tolist()

    ids = [
        f"{metadata['key']}::{metadata['etag']}::{i}"
        for i in range(len(chunks))
    ]

    metadatas = [
        {**metadata, "chunk_index": i}
        for i in range(len(chunks))
    ]

    collection.add(
        ids=ids,
        documents=chunks,
        embeddings=embeddings,
        metadatas=metadatas,
    )

# -----------------------------
# Main task logic
# -----------------------------

def vectorise_documents(files: list[dict]) -> list[dict]:
    """
    Ingest documents into vector storage.

    Input:
    - list of dicts with keys: bucket, key, etag

    Output:
    - list of successfully processed files
    """

    if not files:
        logging.info("No files to vectorise")
        return []

    # Use env variables in production
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )

    processed_files: list[dict] = []

    for f in files:
        bucket = f["bucket"]
        key = f["key"]
        etag = f["etag"]

        logging.info(f"Processing file: {key}")

        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            file_bytes = response["Body"].read()

            if key.lower().endswith(".pdf"):
                text = _extract_text_from_pdf(file_bytes)
            elif key.lower().endswith(".docx"):
                text = _extract_text_from_docx(file_bytes)
            elif key.lower().endswith(".txt"):
                text = _extract_text_from_txt(file_bytes)
            else:
                logging.warning(f"Unsupported file type: {key}")
                continue

            text_length = len(text.strip())
            if text_length == 0:
                logging.warning(f"No text extracted from {key}")
                continue

            chunks = _chunk_text(text)

            if len(chunks) > MAX_CHUNKS:
                logging.warning(
                    f"Too many chunks for {key}, truncating to {MAX_CHUNKS}"
                )
                chunks = chunks[:MAX_CHUNKS]

            _store_chunks_in_chroma(
                chunks=chunks,
                metadata={
                    "bucket": bucket,
                    "key": key,
                    "etag": etag,
                    "embedding_model": EMBEDDING_MODEL_NAME,
                    "chunk_size": CHUNK_SIZE,
                    "chunk_overlap": CHUNK_OVERLAP,
                },
            )

            logging.info(
                "Vectorisation completed",
                extra={
                    "file": key,
                    "etag": etag,
                    "chunks": len(chunks),
                },
            )

            processed_files.append(f)

        except Exception as e:
            logging.exception(f"Failed to process file {key}: {e}")
            raise

    return processed_files
