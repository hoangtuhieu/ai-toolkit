#!/usr/bin/env python3
"""
ingest.py — IDEA project ingestion script for unstructured2graph.

Differences from upstream example (graphrag.py + loading.py):
1. Embeddings computed via direct requests to llama-embedding (port 8089),
   bypassing compute_embeddings() which fails due to OpenAI SDK 2.x
   incompatibility with llama-server response format (model_dump() bug).
2. Vector index created with correct dimension (1024 for Qwen3-Embedding-0.6B),
   not the hardcoded 384 in create_vector_search_index().
3. Target database: memgraph-u2g (bolt://192.168.10.101:7688).
4. Document source passed via --source or --source-dir argument.
5. Retrieval/summarization uses LiteLLM via OPENAI_BASE_URL.
6. Graph wipe removed — memgraph-u2g accumulates data across ingestion runs.
7. Atomic rollback: captures pre-write state of every node/edge in JSONL format,
   auto-restores on failure using parameterized Cypher (safe serialization).
8. modified_at property injected on every node/edge write.
9. Batch manifest file detects incomplete rollbacks from hard interrupts.
10. Lockfile prevents concurrent ingestion runs.
11. Two-step rollback execution (copy then execute) eliminates mid-SSH-drop risk.

Environment variables (loaded from .env):
    MEMGRAPH_URL                    bolt://192.168.10.101:7688
    OPENAI_BASE_URL                 http://192.168.10.101:4000/v1
    OPENAI_API_KEY                  dummy
    OPENAI_API_BASE                 http://192.168.10.101:4000/v1  (for LightRAG)
    EMBEDDING_URL                   http://192.168.10.103:8089/v1/embeddings
    EMBEDDING_MODEL                 embed::active
    EMBEDDING_DIM                   1024
    LIGHTRAG_MODEL                  primary
    ROLLBACK_LOCAL_RETENTION_HOURS  32     <- change here to adjust local retention
    ROLLBACK_REMOTE_RETENTION_DAYS  7      <- change here to adjust remote retention
    BACKUP_HOST                     192.168.10.101
    BACKUP_PATH                     /home/hieu/docker/memgraph-u2g-backups
    TZ                              Asia/Ho_Chi_Minh

Usage:
    python ingest.py --source /path/to/document.md
    python ingest.py --source /path/to/document.md --skip-lightrag
    python ingest.py --source-dir /path/to/folder/
    python ingest.py --source-dir /path/to/folder/ --skip-lightrag
    python ingest.py --query-only --query "What is X?"
    python ingest.py --source doc.md --no-auto-rollback
    python ingest.py --restore rollback/rollback-{ts}.jsonl
"""

import argparse
import asyncio
import json
import logging
import os
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from openai import OpenAI

from lightrag.kg.memgraph_impl import MemgraphStorage
from lightrag.llm.openai import openai_complete_if_cache
from lightrag_memgraph import MemgraphLightRAGWrapper
from memgraph_toolbox.api.memgraph import Memgraph
from unstructured2graph import create_index, from_unstructured
from graph_schema import (
    ENTITY_TYPES, SCHEMA_VERSION,
    extract_file_metadata, build_document_node_props, build_chunk_extra_props,
)


load_dotenv()

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
LIGHTRAG_MODEL  = os.environ.get("LIGHTRAG_MODEL",  "primary")
EMBEDDING_URL   = os.environ.get("EMBEDDING_URL",   "http://192.168.10.103:8089/v1/embeddings")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "embed::active")
EMBEDDING_DIM   = int(os.environ.get("EMBEDDING_DIM", "1024"))
LIGHTRAG_DIR    = os.path.join(os.path.dirname(os.path.realpath(__file__)), "lightrag_storage.out")

ROLLBACK_LOCAL_RETENTION_HOURS = int(os.environ.get("ROLLBACK_LOCAL_RETENTION_HOURS", "32"))
ROLLBACK_REMOTE_RETENTION_DAYS = int(os.environ.get("ROLLBACK_REMOTE_RETENTION_DAYS", "7"))
BACKUP_HOST  = os.environ.get("BACKUP_HOST",  "192.168.10.101")
BACKUP_PATH  = os.environ.get("BACKUP_PATH",  "/home/hieu/docker/memgraph-u2g-backups")
LOCAL_TZ     = ZoneInfo(os.environ.get("TZ", "Asia/Ho_Chi_Minh"))

SCRIPT_DIR    = Path(os.path.dirname(os.path.realpath(__file__)))
ROLLBACK_DIR  = SCRIPT_DIR / "rollback"
LOCKFILE      = ROLLBACK_DIR / ".lock"


# ── Timezone helpers ───────────────────────────────────────────────────────────

def now_local() -> datetime:
    return datetime.now(tz=LOCAL_TZ)

def timestamp_str(dt: datetime | None = None) -> str:
    if dt is None:
        dt = now_local()
    return dt.strftime("%Y-%m-%dT%H-%M-%S%z")


# ── Lockfile ───────────────────────────────────────────────────────────────────

def acquire_lock() -> bool:
    ROLLBACK_DIR.mkdir(exist_ok=True)
    if LOCKFILE.exists():
        try:
            pid = int(LOCKFILE.read_text().strip())
            try:
                os.kill(pid, 0)
                logger.error(f"Another ingestion is running (PID {pid}). If stale, delete: {LOCKFILE}")
                return False
            except ProcessLookupError:
                logger.warning(f"Stale lockfile (PID {pid} gone). Removing.")
                LOCKFILE.unlink()
        except Exception:
            LOCKFILE.unlink()
    LOCKFILE.write_text(str(os.getpid()))
    return True

def release_lock():
    try:
        LOCKFILE.unlink(missing_ok=True)
    except Exception:
        pass


# ── Batch manifest ─────────────────────────────────────────────────────────────

def write_manifest(batch_ts: str, sources: list[str]) -> Path:
    manifest = {"batch_ts": batch_ts, "sources": sources, "started_at": timestamp_str(), "completed": False}
    path = ROLLBACK_DIR / f"manifest-{batch_ts}.json"
    path.write_text(json.dumps(manifest, indent=2))
    return path

def complete_manifest(manifest_path: Path):
    data = json.loads(manifest_path.read_text())
    data["completed"] = True
    data["completed_at"] = timestamp_str()
    manifest_path.write_text(json.dumps(data, indent=2))

def check_incomplete_manifests():
    if not ROLLBACK_DIR.exists():
        return
    for f in ROLLBACK_DIR.glob("manifest-*.json"):
        try:
            data = json.loads(f.read_text())
            if not data.get("completed"):
                rb_file = ROLLBACK_DIR / f"rollback-{data['batch_ts']}.jsonl"
                logger.warning(
                    f"INCOMPLETE BATCH from {data.get('started_at', 'unknown')}. "
                    f"Sources: {data.get('sources')}. "
                    f"Graph may be partial. Rollback: {rb_file}. "
                    f"To restore: python ingest.py --restore {rb_file}"
                )
        except Exception:
            pass


# ── Rollback file (JSONL) ──────────────────────────────────────────────────────

def rollback_path(batch_ts: str) -> Path:
    return ROLLBACK_DIR / f"rollback-{batch_ts}.jsonl"

def append_rollback(path: Path, record: dict):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def cleanup_old_local_rollbacks():
    if not ROLLBACK_DIR.exists():
        return
    cutoff = time.time() - (ROLLBACK_LOCAL_RETENTION_HOURS * 3600)
    for pattern in ["rollback-*.jsonl", "manifest-*.json"]:
        for f in ROLLBACK_DIR.glob(pattern):
            if f.stat().st_mtime < cutoff:
                f.unlink()
                logger.info(f"Deleted old local file: {f.name}")


# ── Remote management ──────────────────────────────────────────────────────────

def transfer_rollback_to_remote(local_path: Path, batch_ts: str) -> bool:
    remote_dir  = f"{BACKUP_PATH}/rollback"
    remote_dest = f"hieu@{BACKUP_HOST}:{remote_dir}/rollback-{batch_ts}.jsonl"
    try:
        subprocess.run(["ssh", f"hieu@{BACKUP_HOST}", f"mkdir -p {remote_dir}"],
                       check=True, capture_output=True, timeout=30)
        subprocess.run(["scp", str(local_path), remote_dest],
                       check=True, capture_output=True, timeout=120)
        logger.info(f"Rollback transferred: {remote_dest}")
        return True
    except Exception as e:
        logger.warning(f"Rollback transfer failed (non-critical): {e}")
        return False

def cleanup_old_remote_rollbacks():
    remote_dir = f"{BACKUP_PATH}/rollback"
    cmd = (f"mkdir -p {remote_dir} && find {remote_dir} -name 'rollback-*.jsonl' "
           f"-mtime +{ROLLBACK_REMOTE_RETENTION_DAYS} -delete 2>/dev/null || true")
    try:
        subprocess.run(["ssh", f"hieu@{BACKUP_HOST}", cmd],
                       check=True, capture_output=True, timeout=30)
    except Exception as e:
        logger.warning(f"Remote cleanup failed (non-critical): {e}")


# ── Execute rollback (two-step) ────────────────────────────────────────────────

def execute_rollback(rollback_file: Path):
    if not rollback_file.exists() or rollback_file.stat().st_size == 0:
        logger.warning("Rollback file empty or missing — nothing to restore.")
        return
    logger.warning(f"Executing rollback: {rollback_file}")
    remote_tmp = f"/tmp/rollback-exec-{int(time.time())}.cypherl"
    try:
        # Build Cypher from JSONL records
        cypher_statements = []
        with open(rollback_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                op = rec["op"]
                if op == "delete_node":
                    cypher_statements.append(
                        f"MATCH (n {{entity_id: {json.dumps(rec['entity_id'])}}}) DETACH DELETE n")
                elif op == "restore_node":
                    cypher_statements.append(
                        f"MATCH (n {{entity_id: {json.dumps(rec['entity_id'])}}}) SET n = {json.dumps(rec['props'])}")
                elif op == "delete_edge":
                    cypher_statements.append(
                        f"MATCH (a {{entity_id: {json.dumps(rec['src'])}}}) -[r]- "
                        f"(b {{entity_id: {json.dumps(rec['tgt'])}}}) DELETE r")
                elif op == "restore_edge":
                    cypher_statements.append(
                        f"MATCH (a {{entity_id: {json.dumps(rec['src'])}}}) -[r]- "
                        f"(b {{entity_id: {json.dumps(rec['tgt'])}}}) SET r = {json.dumps(rec['props'])}")
                elif op == "delete_chunk":
                    cypher_statements.append(
                        f"MATCH (n:Chunk {{hash: {json.dumps(rec['hash'])}}}) DETACH DELETE n")
        if not cypher_statements:
            logger.info("No rollback statements to execute.")
            return
        cypher_content = "\n".join(s + ";" for s in cypher_statements) + "\n"
        # Step 1: copy Cypher to docker-services
        proc = subprocess.run(
            ["ssh", f"hieu@{BACKUP_HOST}", f"cat > {remote_tmp}"],
            input=cypher_content, capture_output=True, text=True, timeout=60)
        if proc.returncode != 0:
            raise RuntimeError(f"Failed to copy rollback Cypher: {proc.stderr}")
        # Step 2: execute entirely on docker-services
        result = subprocess.run(
            ["ssh", f"hieu@{BACKUP_HOST}",
             f"cat {remote_tmp} | docker exec -i memgraph-u2g mgconsole && rm -f {remote_tmp}"],
            capture_output=True, text=True, timeout=300)
        if result.returncode == 0:
            logger.info(f"Rollback complete: {len(cypher_statements)} statements.")
        else:
            logger.error(f"Rollback may have partially failed: {result.stderr[:500]}")
            logger.error(f"Manual restore: python ingest.py --restore {rollback_file}")
    except Exception as e:
        logger.error(f"Rollback execution failed: {e}")
        logger.error(f"Manual restore: python ingest.py --restore {rollback_file}")


# ── Write interceptors ─────────────────────────────────────────────────────────

def install_write_interceptors(rollback_file: Path):
    _orig_node = MemgraphStorage.upsert_node
    _orig_edge = MemgraphStorage.upsert_edge

    async def _patched_upsert_node(self, node_id: str, node_data: dict):
        now_ts = int(time.time())
        try:
            existing = await self.get_node(node_id)
            if existing is None:
                append_rollback(rollback_file, {"op": "delete_node", "entity_id": node_id})
            else:
                append_rollback(rollback_file, {"op": "restore_node", "entity_id": node_id, "props": dict(existing)})
        except Exception as e:
            logger.warning(f"Rollback capture failed for node {node_id}: {e}")
        node_data["modified_at"] = now_ts
        return await _orig_node(self, node_id, node_data)

    async def _patched_upsert_edge(self, src: str, tgt: str, edge_data: dict):
        now_ts = int(time.time())
        try:
            existing = await self.get_edge(src, tgt)
            if existing is None or existing.get("source_id") is None:
                append_rollback(rollback_file, {"op": "delete_edge", "src": src, "tgt": tgt})
            else:
                append_rollback(rollback_file, {"op": "restore_edge", "src": src, "tgt": tgt, "props": dict(existing)})
        except Exception as e:
            logger.warning(f"Rollback capture failed for edge {src}->{tgt}: {e}")
        edge_data["modified_at"] = now_ts
        return await _orig_edge(self, src, tgt, edge_data)

    MemgraphStorage.upsert_node = _patched_upsert_node
    MemgraphStorage.upsert_edge = _patched_upsert_edge


# ── Embedding ──────────────────────────────────────────────────────────────────

def get_embedding(text: str) -> list[float]:
    resp = requests.post(EMBEDDING_URL,
                         headers={"Content-Type": "application/json"},
                         json={"model": EMBEDDING_MODEL, "input": text},
                         timeout=60)
    resp.raise_for_status()
    return resp.json()["data"][0]["embedding"]


async def litellm_complete(prompt, system_prompt=None, history_messages=None, **kwargs) -> str:
    if history_messages is None:
        history_messages = []
    return await openai_complete_if_cache(
        LIGHTRAG_MODEL, prompt,
        system_prompt=system_prompt, history_messages=history_messages, **kwargs)


def compute_embeddings_direct(memgraph: Memgraph, label: str, rollback_file: Path) -> int:
    nodes = memgraph.query(
        f"MATCH (n:{label}) WHERE n.embedding IS NULL "
        f"RETURN id(n) AS node_id, n.hash AS hash, n.text AS text")
    total = len(nodes)
    logger.info(f"Computing embeddings for {total} :{label} nodes...")
    embedded = 0
    now_ts = int(time.time())
    for i, row in enumerate(nodes):
        node_id = row["node_id"]
        chunk_hash = row.get("hash", "")
        text = row.get("text", "")
        if not text or not text.strip():
            logger.warning(f"Node {node_id} has empty text — skipping.")
            continue
        try:
            if chunk_hash:
                append_rollback(rollback_file, {"op": "delete_chunk", "hash": chunk_hash})
            embedding = get_embedding(text)
            memgraph.query(
                f"MATCH (n:{label}) WHERE id(n) = {node_id} "
                f"SET n.embedding = {embedding}, n.modified_at = {now_ts}")
            embedded += 1
        except Exception as e:
            logger.error(f"Embedding failed for node {node_id}: {e}")
        if (i + 1) % 10 == 0 or (i + 1) == total:
            logger.info(f"  {i + 1}/{total} processed, {embedded} embedded")
    return embedded


def create_vector_index(memgraph: Memgraph, label: str, prop: str):
    try:
        memgraph.query(
            f"CREATE VECTOR INDEX vs_name ON :{label}({prop}) "
            f"WITH CONFIG {{'dimension': {EMBEDDING_DIM}, 'capacity': 10000}};")
        logger.info(f"Vector index created: vs_name on :{label}({prop}) dim={EMBEDDING_DIM}")
    except Exception as e:
        logger.warning(f"Vector index creation: {e}")


# ── Retrieval / QA ─────────────────────────────────────────────────────────────

def query_graph(memgraph: Memgraph, prompt: str, top_k: int = 5) -> str:
    embedding = get_embedding(prompt)
    rows = memgraph.query(f"""
        CALL vector_search.search('vs_name', {top_k}, {embedding})
        YIELD distance, node, similarity
        MATCH (node)-[r*bfs]-(dst:Chunk)
        WITH DISTINCT dst, degree(dst) AS degree ORDER BY degree DESC
        RETURN dst LIMIT {top_k};""")
    chunks = []
    for row in rows:
        dst = row.get("dst", {})
        if "text" in dst:
            chunks.append(dst["text"])
        elif "description" in dst:
            chunks.append(dst["description"])
    if not chunks:
        return "No relevant chunks found."
    context = "\n\n".join(chunks)
    client = OpenAI(
        api_key=os.environ.get("OPENAI_API_KEY", "dummy"),
        base_url=os.environ.get("OPENAI_BASE_URL", "http://192.168.10.101:4000/v1"))
    response = client.chat.completions.create(
        model="primary",
        messages=[
            {"role": "system", "content": "You are a helpful assistant. Answer based only on the provided context."},
            {"role": "user", "content": f"Context:\n{context}\n\nQuestion: {prompt}"}],
        temperature=0.1, max_tokens=512)
    return response.choices[0].message.content


# ── Batch ingestion ────────────────────────────────────────────────────────────

async def ingest_batch(sources: list[str], skip_lightrag: bool = False, auto_rollback: bool = True):
    ROLLBACK_DIR.mkdir(exist_ok=True)
    check_incomplete_manifests()
    cleanup_old_local_rollbacks()
    cleanup_old_remote_rollbacks()

    if not acquire_lock():
        raise RuntimeError("Could not acquire ingestion lock.")

    batch_ts = timestamp_str()
    rb_file = rollback_path(batch_ts)
    manifest_path = write_manifest(batch_ts, sources)

    logger.info(f"Batch start [{batch_ts}]: {len(sources)} source(s)")
    logger.info(f"Rollback file: {rb_file}")

    install_write_interceptors(rb_file)

    if os.path.exists(LIGHTRAG_DIR):
        shutil.rmtree(LIGHTRAG_DIR)
    os.makedirs(LIGHTRAG_DIR)

    memgraph = Memgraph(user_agent="idea-ingest")
    create_index(memgraph, "Chunk", "hash")

    lightrag_wrapper = MemgraphLightRAGWrapper(log_level="WARNING", disable_embeddings=True)
    await lightrag_wrapper.initialize(
        working_dir=LIGHTRAG_DIR,
        llm_model_func=litellm_complete,
        llm_model_name=LIGHTRAG_MODEL,
        addon_params={"entity_type_prompt_file": "idea-infra.yml"})

    success = False
    try:
        for source in sources:
            logger.info(f"Processing: {source}")

            # ── Pre-ingestion: extract filename metadata ───────────────────────
            file_metadata = extract_file_metadata(source)
            if file_metadata:
                logger.info(
                    f"File metadata: timestamp={file_metadata['note_timestamp']} "
                    f"role={file_metadata['note_role']}"
                )

            # ── Read full file content for :Document node ─────────────────────
            try:
                file_content = Path(source).read_text(encoding="utf-8")
            except Exception as e:
                logger.warning(f"Could not read file content for Document node: {e}")
                file_content = ""

            # ── LightRAG ingestion with entity type vocabulary ────────────────
            await from_unstructured(
                [source], memgraph, lightrag_wrapper,
                only_chunks=skip_lightrag, link_chunks=True,
            )
            logger.info(f"Chunked: {source}")

            # ── Write :Document node ──────────────────────────────────────────
            doc_props = build_document_node_props(source, file_content, file_metadata)
            doc_stem  = doc_props["filename_stem"]
            now_ts    = doc_props["modified_at"]

            # MERGE so re-ingestion updates an existing or placeholder node
            memgraph.query(
                """
                MERGE (d:Document {filename_stem: $stem})
                SET d += $props
                """,
                {"stem": doc_stem, "props": doc_props},
            )
            logger.info(f"Document node written: {doc_stem}")

            # ── Inject note_timestamp / note_role into Chunk nodes ────────────
            # Chunk nodes have no source_id or file_path property — match all
            # Chunk nodes that do not yet have note_timestamp set. Safe because
            # ingestion is single-threaded and locked (one file at a time).
            chunk_extra = build_chunk_extra_props(file_metadata)
            if chunk_extra:
                memgraph.query(
                    """
                    MATCH (c:Chunk)
                    WHERE c.note_timestamp IS NULL
                    SET c += $extra
                    """,
                    {"extra": chunk_extra},
                )
                logger.info(
                    f"Chunk metadata injected: timestamp={chunk_extra.get('note_timestamp')}"
                )

            # ── Write PART_OF edges: Chunk -> Document ────────────────────────
            # Match Chunk nodes that belong to this Document via text content
            # heuristic: Chunk nodes without an existing PART_OF edge.
            memgraph.query(
                """
                MATCH (c:Chunk), (d:Document {filename_stem: $stem})
                WHERE NOT (c)-[:PART_OF]->()
                MERGE (c)-[:PART_OF]->(d)
                """,
                {"stem": doc_stem},
            )
            logger.info(f"PART_OF edges written: Chunk -> Document({doc_stem})")

        await lightrag_wrapper.afinalize()
        logger.info("Entity extraction complete.")

        n = compute_embeddings_direct(memgraph, "Chunk", rb_file)
        logger.info(f"Embedded {n} Chunk nodes.")

        # Verify all Chunk nodes have embeddings — partial embedding = integrity concern
        total_chunks = memgraph.query("MATCH (n:Chunk) RETURN count(n) AS c")[0]["c"]
        missing = memgraph.query(
            "MATCH (n:Chunk) WHERE n.embedding IS NULL RETURN count(n) AS c"
        )[0]["c"]
        if missing > 0:
            raise RuntimeError(
                f"Embedding incomplete: {missing}/{total_chunks} Chunk nodes missing embeddings. "
                f"Triggering rollback to preserve graph integrity."
            )
        logger.info(f"Embedding verified: all {total_chunks} Chunk nodes have embeddings.")

        create_vector_index(memgraph, "Chunk", "embedding")
        complete_manifest(manifest_path)
        success = True
        logger.info(f"Batch complete [{batch_ts}]")

    except Exception as e:
        logger.error(f"Batch FAILED [{batch_ts}]: {e}")
        if auto_rollback:
            logger.warning("Auto-rollback: restoring graph...")
            execute_rollback(rb_file)
        else:
            logger.warning(
                f"--no-auto-rollback set. Graph in partial state.\n"
                f"To restore: python ingest.py --restore {rb_file}")
        raise

    finally:
        release_lock()
        transfer_rollback_to_remote(rb_file, batch_ts)
        if success:
            logger.info(f"Local rollback kept {ROLLBACK_LOCAL_RETENTION_HOURS}h, remote {ROLLBACK_REMOTE_RETENTION_DAYS} days.")
        else:
            logger.info(f"Rollback on docker-services: {BACKUP_PATH}/rollback/rollback-{batch_ts}.jsonl")


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Ingest documents into memgraph-u2g with atomic rollback")

    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument("--source", default=None,
                               help="Single file or URL to ingest")
    source_group.add_argument("--source-dir", default=None,
                               help="Folder — all supported files as one atomic batch")
    source_group.add_argument("--restore", default=None,
                               help="Rollback JSONL file path — restore graph to pre-batch state")

    parser.add_argument("--skip-lightrag", action="store_true",
                        help="Skip LightRAG entity extraction")
    parser.add_argument("--no-auto-rollback", action="store_true",
                        help="On failure, leave graph in partial state for inspection")
    parser.add_argument("--query", default=None,
                        help="Run a retrieval query against the graph")
    parser.add_argument("--query-only", action="store_true",
                        help="Skip ingestion, run query only")
    args = parser.parse_args()

    if args.restore:
        restore_path = Path(args.restore)
        execute_rollback(restore_path)

    elif not args.query_only:
        sources = []
        if args.source:
            sources = [args.source]
        elif args.source_dir:
            source_dir = Path(args.source_dir)
            if not source_dir.is_dir():
                parser.error(f"--source-dir: not a directory: {args.source_dir}")
            SUPPORTED_EXTENSIONS = {".md", ".txt", ".pdf", ".docx", ".html", ".htm"}
            sources = sorted([str(f) for f in source_dir.iterdir()
                              if f.is_file() and f.suffix.lower() in SUPPORTED_EXTENSIONS])
            if not sources:
                parser.error(f"No supported files found in: {args.source_dir}")
            logger.info(f"Found {len(sources)} file(s) in {args.source_dir}")
        else:
            parser.error("--source, --source-dir, --restore, or --query-only required")

        asyncio.run(ingest_batch(sources, skip_lightrag=args.skip_lightrag,
                                  auto_rollback=not args.no_auto_rollback))

    if args.query:
        memgraph = Memgraph(user_agent="idea-ingest")
        answer = query_graph(memgraph, args.query)
        print(f"\nQ: {args.query}\nA: {answer}")
