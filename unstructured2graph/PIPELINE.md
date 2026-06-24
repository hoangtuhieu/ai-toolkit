# unstructured2graph Pipeline — Usage Guide
**Date:** 2026-06-24 — Block 10 session (updated from initial draft)
**Machine:** MacBook Air (pipeline runs here)
**Target database:** memgraph-u2g (docker-services:7688)

---

## Architecture

```
Document
  → unstructured (chunking via chunk_by_title())
  → LightRAG (entity/relationship extraction)
       → LiteLLM (port 4000) → primary (ai-main:8080)
       → writes :base entity nodes + DIRECTED edges to memgraph-u2g
  → llama-embedding (ai-main:8089, direct requests, no LiteLLM)
       → writes n.embedding (1024-dim LIST) + n.modified_at to :Chunk nodes
  → vector index vs_name on :Chunk(embedding), dim=1024

Query
  → llama-embedding (embed the question)
  → vector_search.search('vs_name', N, embedding) in memgraph-u2g
  → BFS graph traversal for context
  → LiteLLM → primary (summarization)
```

**Why embeddings bypass LiteLLM:** OpenAI SDK 2.x injects encoding_format=null
which llama-server rejects. All LiteLLM provider prefixes are affected. Direct
requests to llama-embedding works correctly. See embedding-discoveries.md.

---

## Setup (one-time)

```bash
cd ~/projects/dev/ai-toolkit/unstructured2graph
source .venv/bin/activate
cat .env  # verify all 13 variables present
```

---

## Environment Variables (.env)

```
MEMGRAPH_URL=bolt://192.168.10.101:7688
OPENAI_BASE_URL=http://192.168.10.101:4000/v1
OPENAI_API_KEY=dummy
OPENAI_API_BASE=http://192.168.10.101:4000/v1
EMBEDDING_URL=http://192.168.10.103:8089/v1/embeddings
EMBEDDING_MODEL=embed::active
EMBEDDING_DIM=1024
LIGHTRAG_MODEL=primary
ROLLBACK_LOCAL_RETENTION_HOURS=32
ROLLBACK_REMOTE_RETENTION_DAYS=7
BACKUP_HOST=192.168.10.101
BACKUP_PATH=/home/hieu/docker/memgraph-u2g-backups
TZ=Asia/Ho_Chi_Minh
```

Note: OPENAI_BASE_URL (OpenAI SDK) and OPENAI_API_BASE (LightRAG) are different
variables. Both must be set or either extraction or retrieval will fail.

---

## Workflows

### Workflow 1 — Quick test (chunks + embeddings, no entity extraction)
```bash
python ingest.py --source /path/to/document.md --skip-lightrag
```

### Workflow 2 — Full ingestion
```bash
python ingest.py --source /path/to/document.md
```

### Workflow 3 — Folder ingestion (atomic batch)
```bash
python ingest.py --source-dir /path/to/folder/
```

### Workflow 4 — Query only
```bash
python ingest.py --query-only --query "What is this document about?"
```

### Workflow 5 — Inspect failure without auto-rollback
```bash
python ingest.py --source doc.md --no-auto-rollback
# Manual restore: python ingest.py --restore rollback/rollback-{ts}.jsonl
```

### Workflow 6 — Manual restore from rollback file
```bash
python ingest.py --restore rollback/rollback-2026-06-23T15-45-28+0700.jsonl
```

---

## Viewing in Memgraph Lab

Open: http://192.168.10.101:3001
Reconfigure connection: Host=memgraph-u2g, Port=7687 (internal Docker port)

```cypher
MATCH (n) RETURN labels(n) AS type, count(n) AS count ORDER BY count DESC;
MATCH (a:Chunk)-[r:NEXT]->(b:Chunk) RETURN a, r, b;
MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 100;
MATCH (n:base) RETURN n.entity_id, n.entity_type, n.description LIMIT 20;
SHOW INDEX INFO;
MATCH (n:Chunk) RETURN valueType(n.embedding) AS type LIMIT 1;
```

---

## Clearing the Graph

```bash
echo "MATCH (n) DETACH DELETE n;" | docker exec -i memgraph-u2g mgconsole
echo "DROP VECTOR INDEX vs_name;" | docker exec -i memgraph-u2g mgconsole
```

---

## Daily Backup and Restore

Cron: 2:00 AM Ho Chi Minh time on docker-services
Manual: ~/docker/memgraph-u2g-backups/backup_graph.sh
Snapshots: ~/docker/memgraph-u2g-backups/snapshots/

RESTORE — see memgraph-snapshot-reference.md for the complete procedure.
The WAL clear step is CRITICAL and must not be skipped for rollback scenarios.

---

## Known Limitations

- LiteLLM embedding routing broken (OpenAI SDK 2.x bug) — direct requests workaround in ingest.py
- LightRAG extraction failures are swallowed (not Python exceptions) — graceful degradation, not integrity threat
- Entity extraction is non-deterministic — different runs produce different counts (normal)
- Hard interrupt may leave incomplete rollback file — use daily snapshot as safety net
- Mac Mini replication not yet done — MBA only

---

## File Locations

| File | Location | Machine |
|---|---|---|
| ingest.py | ~/projects/dev/ai-toolkit/unstructured2graph/ingest.py | MBA |
| .env | ~/projects/dev/ai-toolkit/unstructured2graph/.env | MBA |
| rollback/ | ~/projects/dev/ai-toolkit/unstructured2graph/rollback/ | MBA |
| backup_graph.sh | ~/docker/memgraph-u2g-backups/backup_graph.sh | docker-services |
| Snapshots | ~/docker/memgraph-u2g-backups/snapshots/ | docker-services |
| Remote rollback | ~/docker/memgraph-u2g-backups/rollback/ | docker-services |
| llama-embedding envs | /etc/llama-embedding/ | ai-main |
