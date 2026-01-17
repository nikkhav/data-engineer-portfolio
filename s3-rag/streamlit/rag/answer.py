"""
Answer generation layer for RAG pipeline.

Responsibilities:
- retrieve relevant chunks from vector store
- build grounded prompt
- call LLM (GigaChat)
- return answer with sources
"""

from __future__ import annotations

from typing import Dict, List

from rag.search import search_documents
from rag.llm.gigachat_client import generate


# -----------------------------
# Prompt builder
# -----------------------------

def _build_prompt(query: str, contexts: List[Dict]) -> str:
    """
    Build a strict RAG prompt to avoid hallucinations.
    """

    context_blocks = []

    for i, ctx in enumerate(contexts, start=1):
        meta = ctx.get("metadata", {})
        source = meta.get("key", "unknown")
        chunk_idx = meta.get("chunk_index", "?")

        context_blocks.append(
            f"[Source {i} | {source} | chunk {chunk_idx}]\n"
            f"{ctx['text']}"
        )

    joined_context = "\n\n".join(context_blocks)

    prompt = f"""
You are an assistant answering questions ONLY using the provided sources.

Rules:
- Use ONLY the information explicitly stated in the sources.
- DO NOT infer, summarize beyond the text, or use external knowledge.
- If the answer is not explicitly present, say: "The provided documents do not contain this information."
- Cite facts only if they are directly supported by the sources.

Sources:
{joined_context}

Question:
{query}

Answer:
"""

    return prompt.strip()


# -----------------------------
# Public API
# -----------------------------

def answer_query(
    query: str,
    top_k: int = 5,
) -> Dict:
    """
    Full RAG pipeline: retrieve → generate answer.

    Returns:
    {
        "answer": str,
        "sources": [
            {
                "file": str,
                "chunk_index": int,
                "score": float
            }
        ]
    }
    """

    if not query.strip():
        return {
            "answer": "Empty query.",
            "sources": [],
        }

    results = search_documents(
        query=query,
        top_k=top_k,
    )

    if not results:
        return {
            "answer": "No relevant information found in the documents.",
            "sources": [],
        }

    prompt = _build_prompt(query, results)
    answer = generate(prompt)

    sources = [
        {
            "file": r["metadata"].get("key"),
            "chunk_index": r["metadata"].get("chunk_index"),
            "score": r["score"],
        }
        for r in results
    ]

    return {
        "answer": answer,
        "sources": sources,
    }
