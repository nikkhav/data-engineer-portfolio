"""
Vector search module for RAG pipeline.

This module provides a single responsibility:
- take a user query
- embed it using the same embedding model as ingestion
- retrieve top-k relevant document chunks from ChromaDB
"""

from __future__ import annotations

import chromadb
from sentence_transformers import SentenceTransformer
from typing import List, Dict

# -----------------------------
# Configuration
# -----------------------------

CHROMA_PERSIST_DIR = "/opt/chroma"
COLLECTION_NAME = "documents"
EMBEDDING_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

# -----------------------------
# Lazy-loaded embedding model
# -----------------------------

_embedding_model: SentenceTransformer | None = None


def _get_embedding_model() -> SentenceTransformer:
    global _embedding_model

    if _embedding_model is None:
        _embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME)

    return _embedding_model


# -----------------------------
# Chroma client
# -----------------------------

def _get_chroma_collection():
    client = chromadb.PersistentClient(
        path=CHROMA_PERSIST_DIR
    )

    return client.get_or_create_collection(
        name=COLLECTION_NAME
    )


# -----------------------------
# Public API
# -----------------------------

def search_documents(
    query: str,
    top_k: int = 5,
) -> List[Dict]:
    """
    Search for relevant document chunks in ChromaDB.

    Args:
        query: User natural language query
        top_k: Number of top results to return

    Returns:
        List of dictionaries with:
        - text
        - score (distance)
        - metadata (file, chunk index, etc.)
    """

    if not query.strip():
        return []

    model = _get_embedding_model()
    collection = _get_chroma_collection()

    query_embedding = model.encode(query).tolist()

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=top_k,
        include=["documents", "metadatas", "distances"],
    )

    documents = results.get("documents", [[]])[0]
    metadatas = results.get("metadatas", [[]])[0]
    distances = results.get("distances", [[]])[0]

    response: List[Dict] = []

    for doc, meta, dist in zip(documents, metadatas, distances):
        response.append(
            {
                "text": doc,
                "score": float(dist),
                "metadata": meta,
            }
        )

    return response
