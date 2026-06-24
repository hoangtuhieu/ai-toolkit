# Embedding Stack — Technical Discoveries
**Date:** 2026-06-23/24 — Block 10 session
**Scope:** Findings from investigation of embedding routing through LiteLLM, MAGE, and direct HTTP

---

## Root Cause: OpenAI SDK 2.x Injects `encoding_format=null`

The fundamental issue affecting all embedding routing through LiteLLM is in the **OpenAI Python SDK itself**, not in LiteLLM or any specific version.

OpenAI Python SDK 2.x injects `encoding_format: null` into embedding request bodies. llama-server rejects this with:
```
[json.exception.type_error.302] type must be string, but is null
```

This manifests as `'list' object has no attribute 'model_dump'` in LiteLLM's response parsing.

**Versions confirmed affected:**
- LiteLLM 1.81.9 (Memgraph MAGE's bundled version)
- LiteLLM 1.82.6
- LiteLLM 1.87.1 (our external proxy)
- OpenAI Python SDK 2.41.1 (inside Memgraph container)
- OpenAI Python SDK 2.43.0 (in unstructured2graph venv)

**This is not fixable via LiteLLM configuration.** The bug is upstream in the OpenAI SDK. No provider prefix (`openai/`, `llamafile/`, `hosted_vllm/`) avoids it.

**Workaround:** Call llama-embedding directly via `requests`:
```python
resp = requests.post(
    "http://192.168.10.103:8089/v1/embeddings",
    headers={"Content-Type": "application/json"},
    json={"model": "embed::active", "input": text},
    timeout=60,
)
embedding = resp.json()["data"][0]["embedding"]
```
This bypasses the OpenAI SDK entirely and works correctly.

---

## LiteLLM Embedding Routing — What Was Tested

| Approach | Result | Reason |
|---|---|---|
| `openai/embed::active` via LiteLLM proxy | ❌ Fails | OpenAI SDK injects `encoding_format=null` |
| `llamafile/embed::active` via LiteLLM proxy | ❌ Fails | Same root cause |
| `hosted_vllm/embed::active` via LiteLLM proxy | ❌ Fails | Same root cause |
| Direct `requests` to llama-embedding:8089 | ✅ Works | No OpenAI SDK involved |
| Memgraph MAGE internal LiteLLM (1.81.9) | ❌ Fails | Same OpenAI SDK version used internally |

---

## Memgraph MAGE Embedding Module

### Version 3.9.0 (old)
- `embeddings.node_sentence()` uses **local sentence-transformers only**
- Default model: `all-MiniLM-L6-v2` (384 dims, 256 token max)
- No remote API support at all in this version
- No `litellm` package accessible at Python level for this module

### Version 3.11.0 (current)
- Remote embedding support added via LiteLLM
- Routing controlled by `model_name` parameter: bare name = local, `provider/model` prefix = remote
- Remote path configured via `OPENAI_API_KEY` and `OPENAI_BASE_URL` in container environment
- **But:** hits the same OpenAI SDK 2.x bug — `embedding: 'STRING'` stored instead of `LIST`
- Local path still works with sentence-transformers models

### Routing mechanism in 3.11.0
```python
# In /usr/lib/memgraph/query_modules/embeddings.py
def resolve_remote_model(model_name):
    model, provider, default_api_base = litellm.get_llm_provider(model_name)
    # Returns provider if model_name has a recognized prefix
```
A bare HuggingFace model name → local. A LiteLLM-style prefix (`openai/`, `ollama/`, etc.) → remote via LiteLLM. But the remote path hits the OpenAI SDK bug.

---

## LightRAG Environment Variable

LightRAG reads `OPENAI_API_BASE` for its base URL, **not** `OPENAI_BASE_URL`.

The OpenAI Python SDK v2 reads `OPENAI_BASE_URL`. These are different environment variable names.

Both must be set in `.env` when using LightRAG alongside direct OpenAI client calls:
```
OPENAI_BASE_URL=http://192.168.10.101:4000/v1    # for OpenAI client in query_graph()
OPENAI_API_BASE=http://192.168.10.101:4000/v1    # for LightRAG's internal client
```

If only `OPENAI_BASE_URL` is set, LightRAG silently falls back to OpenAI cloud and fails with 401.

---

## Embedding Model Comparison

| Model | Dims | Max Tokens | Size | Vietnamese | Notes |
|---|---|---|---|---|---|
| all-MiniLM-L6-v2 (MAGE default) | 384 | 256 | ~22MB | Poor | Good for English retrieval, built into MAGE |
| Qwen3-Embedding-0.6B | 1024 | 32768 | ~639MB | Strong | Our primary choice, served via llama-embedding |
| EmbeddingGemma-300M | 768 | 2048 | ~329MB | Good | Secondary option, served via llama-embedding |

---

## llama-embedding Service (ai-main)

- Port: 8089
- Mode: CPU-only (RTX 3090 fully occupied by `primary`)
- Default model: Qwen3-Embedding-0.6B (`embed::active` alias)
- Secondary model: EmbeddingGemma-300M
- Switch script: `/etc/llama-embedding/switch-embedding-model.sh`
- Env files: `/etc/llama-embedding/{qwen3-embedding-0.6b,embeddinggemma-300m,active}.env`

**Verified working via direct HTTP from:**
- MacBook Air → ai-main:8089 ✅
- docker-services (memgraph-u2g container) → ai-main:8089 ✅

---

## unstructured2graph Embedding Path

`unstructured2graph`'s `compute_embeddings()` calls `embeddings.node_sentence()` MAGE procedure internally. This procedure does **not** go through Python or LiteLLM in our `ingest.py` — we bypass it entirely.

Our `compute_embeddings_direct()` in `ingest.py`:
1. Queries Memgraph for all `:Chunk` nodes missing embeddings
2. Calls `llama-embedding` directly via `requests` for each chunk's text
3. Stores result as `n.embedding` list property (proper float list, not string)
4. Adds `n.modified_at` timestamp

This produces embeddings stored as `LIST` type, which the vector index requires. Storing via `export_util.cypher_all` would convert these to `STRING` (a different bug in export_util's property formatting) — another reason to use `CREATE SNAPSHOT` instead of Cypher export for backups.

---

## Summary: What Works, What Doesn't, What to Use

| Task | Don't Use | Use Instead |
|---|---|---|
| Compute embeddings in ingest pipeline | LiteLLM `/v1/embeddings`, MAGE `embeddings.node_sentence()` | Direct `requests` to llama-embedding:8089 |
| Backup memgraph-u2g | `export_util.cypher_all` | `CREATE SNAPSHOT;` + `docker cp` |
| LightRAG LLM routing | Hardcoded `gpt-4o-mini` | `OPENAI_API_BASE` + custom `litellm_complete()` function |
| Cross-version migration | Snapshot files (version-tied) | `DUMP DATABASE;` → CYPHERL |
