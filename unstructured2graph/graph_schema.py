"""
graph_schema.py — LightRAG customization and graph schema logic
===============================================================
Part of: IDEA / unstructured2graph pipeline
Location: ~/projects/dev/ai-toolkit/unstructured2graph/graph_schema.py

This file is the single place to change how the ingest process
creates nodes and edges in the graph. ingest.py imports from here.
query_graph.py reads schema_config.json directly.

Contents:
    - Schema config loader
    - Exported constants: ENTITY_TYPES, RELATIONSHIP_TYPES
    - Filename metadata extraction (note_timestamp, note_role)
    - Document node construction
    - Chunk property injection
    - Obsidian link parsing and edge creation
    - QAChain Cypher prompt builder
"""

import json
import re
import time
from calendar import timegm
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ── Paths ────────────────────────────────────────────────────────────────────

_HERE = Path(__file__).parent
SCHEMA_CONFIG_PATH = _HERE / "schema_config.json"


# ── Schema config loader ─────────────────────────────────────────────────────

def load_schema_config() -> dict:
    """Load and return schema_config.json. Raises if file is missing or invalid."""
    if not SCHEMA_CONFIG_PATH.exists():
        raise FileNotFoundError(f"schema_config.json not found at {SCHEMA_CONFIG_PATH}")
    with open(SCHEMA_CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
    required_keys = {"schema_version", "entity_types", "relationship_types",
                     "property_hints", "note_roles"}
    missing = required_keys - set(config.keys())
    if missing:
        raise ValueError(f"schema_config.json missing required keys: {missing}")
    return config


# ── Exported constants ────────────────────────────────────────────────────────
# These are read once at import time. ingest.py uses them directly.

_CONFIG = load_schema_config()

SCHEMA_VERSION: str = _CONFIG["schema_version"]
ENTITY_TYPES: list[str] = _CONFIG["entity_types"]
RELATIONSHIP_TYPES: list[str] = _CONFIG["relationship_types"]
NOTE_ROLES: dict[str, str] = _CONFIG["note_roles"]


# ── Filename metadata extraction ──────────────────────────────────────────────

# Pattern: YYMMDD-hhmm-x at the start of the filename stem
# Examples:
#   "260619-2132-0 system-snapshot-by-Hieu.md"  → match
#   "260616-1450-1 some-note.md"                 → match
#   "README.md"                                  → no match
_FILENAME_PATTERN = re.compile(
    r'^(?P<date>\d{6})-(?P<time>\d{4})-(?P<role_code>[01])'
)


def extract_file_metadata(filepath: str | Path) -> Optional[dict]:
    """
    Parse YYMMDD-hhmm-x metadata from a filename stem.

    Returns a dict with:
        note_timestamp       str  e.g. "260619-2132"
        note_timestamp_epoch int  Unix epoch (seconds, UTC, midnight of the date + hhmm)
        note_role            str  e.g. "hieu" or "claude.ai"

    Returns None if the filename does not match the expected pattern.
    """
    stem = Path(filepath).stem  # filename without extension
    m = _FILENAME_PATTERN.match(stem)
    if not m:
        return None

    date_str = m.group("date")   # e.g. "260619"
    time_str = m.group("time")   # e.g. "2132"
    role_code = m.group("role_code")  # "0" or "1"

    # Build human-readable timestamp
    note_timestamp = f"{date_str}-{time_str}"  # e.g. "260619-2132"

    # Parse to epoch — interpret YYMMDD as 20YY-MM-DD
    try:
        yy = int(date_str[0:2])
        mm = int(date_str[2:4])
        dd = int(date_str[4:6])
        hh = int(time_str[0:2])
        mn = int(time_str[2:4])
        year = 2000 + yy
        dt = datetime(year, mm, dd, hh, mn, 0, tzinfo=timezone.utc)
        note_timestamp_epoch = int(dt.timestamp())
    except (ValueError, OverflowError):
        # Malformed date components — skip epoch, keep string
        note_timestamp_epoch = 0

    # Resolve role code
    note_role = NOTE_ROLES.get(role_code, f"unknown-{role_code}")

    return {
        "note_timestamp": note_timestamp,
        "note_timestamp_epoch": note_timestamp_epoch,
        "note_role": note_role,
    }


# ── Document node construction ────────────────────────────────────────────────

def build_document_node_props(
    filepath: str | Path,
    content: str,
    file_metadata: Optional[dict] = None,
) -> dict:
    """
    Build the property dict for a :Document node.

    Args:
        filepath:       Full path to the source file.
        content:        Full text content of the file.
        file_metadata:  Output of extract_file_metadata(), or None.

    Returns a dict of properties ready to write to Memgraph.
    """
    path = Path(filepath)
    now = int(time.time())

    props = {
        "filename":      path.name,          # e.g. "260619-2132-0 system-snapshot-by-Hieu.md"
        "filename_stem": path.stem,          # e.g. "260619-2132-0 system-snapshot-by-Hieu"
        "file_path":     str(path.resolve()),
        "content":       content,
        "status":        "ingested",
        "resolved":      True,
        "schema_version": SCHEMA_VERSION,
        "created_at":    now,
        "modified_at":   now,
    }

    if file_metadata:
        props["note_timestamp"]       = file_metadata["note_timestamp"]
        props["note_timestamp_epoch"] = file_metadata["note_timestamp_epoch"]
        props["note_role"]            = file_metadata["note_role"]

    return props


def build_chunk_extra_props(file_metadata: Optional[dict]) -> dict:
    """
    Build extra properties to inject into :Chunk nodes for this file.
    These are added on top of whatever LightRAG writes.

    Args:
        file_metadata:  Output of extract_file_metadata(), or None.

    Returns a dict of extra properties (may be empty).
    """
    if not file_metadata:
        return {}
    return {
        "note_timestamp":       file_metadata["note_timestamp"],
        "note_timestamp_epoch": file_metadata["note_timestamp_epoch"],
        "note_role":            file_metadata["note_role"],
    }


# ── Obsidian link parsing and edge creation ───────────────────────────────────

# Format 1: [[stem]]  or  [[stem|display text]]
_WIKI_LINK = re.compile(r'\[\[([^\]|]+)(?:\|[^\]]+)?\]\]')

# Format 2: [display text](<path/to/file.md>)  or  [display text](path/to/file.md)
_MD_LINK = re.compile(r'\[([^\]]+)\]\(<([^>]+\.md)>\)')
_MD_LINK_PLAIN = re.compile(r'\[([^\]]+)\]\(([^)]+\.md)\)')


def extract_obsidian_links(content: str) -> list[dict]:
    """
    Parse Obsidian-style links from document content.

    Returns a list of dicts:
        target_stem   str   filename stem of the target (without .md)
        link_text     str   display text or raw wiki link content
        link_format   str   "wiki" or "markdown"
    """
    links = []
    seen = set()  # deduplicate on target_stem only — one edge per source->target pair

    # Format 1: [[stem]] or [[stem|display]]
    for m in _WIKI_LINK.finditer(content):
        raw = m.group(1).strip()
        # Strip .md if someone wrote [[file.md]]
        stem = raw[:-3] if raw.endswith(".md") else raw
        if stem not in seen:
            seen.add(stem)
            links.append({
                "target_stem": stem,
                "link_text":   raw,
                "link_format": "wiki",
            })

    # Format 2: [text](<path.md>) and [text](path.md)
    for pattern in (_MD_LINK, _MD_LINK_PLAIN):
        for m in pattern.finditer(content):
            link_text = m.group(1).strip()
            path_str  = m.group(2).strip()
            stem = Path(path_str).stem
            if stem not in seen:
                seen.add(stem)
                links.append({
                    "target_stem": stem,
                    "link_text":   link_text,
                    "link_format": "markdown",
                })

    return links


def resolve_obsidian_links(driver, verbose: bool = True) -> dict:
    """
    Scan all ingested :Document nodes in the graph, parse their Obsidian links,
    and create LINKS_TO edges to target :Document nodes.

    If a target :Document node does not exist, a placeholder is created:
        (:Document {filename_stem: "...", status: "placeholder", resolved: false})

    When a real :Document node is later ingested with the same filename_stem,
    ingest.py upgrades the placeholder (status → "ingested", resolved → true).

    This function is IDEMPOTENT — safe to re-run. It uses MERGE for both
    placeholder nodes and edges, so re-running does not create duplicates.

    Args:
        driver:   A neo4j.GraphDatabase.driver instance connected to memgraph-u2g.
        verbose:  Print progress to stdout.

    Returns a summary dict:
        total_documents    int
        total_links_found  int
        edges_created      int
        placeholders_created int
    """
    summary = {
        "total_documents":      0,
        "total_links_found":    0,
        "edges_created":        0,
        "placeholders_created": 0,
    }

    # Fetch all ingested Document nodes with content
    docs = driver.query(
        "MATCH (d:Document {status: 'ingested'}) "
        "RETURN d.filename_stem AS stem, d.content AS content"
    )

    summary["total_documents"] = len(docs)
    if verbose:
        print(f"[resolve_obsidian_links] Scanning {len(docs)} ingested Document nodes...")

    for doc in docs:
        source_stem = doc["stem"]
        doc_content = doc.get("content") or ""

        links = extract_obsidian_links(doc_content)
        summary["total_links_found"] += len(links)

        for link in links:
            target_stem  = link["target_stem"]
            link_text    = link["link_text"]
            link_format  = link["link_format"]

            # Check if target Document node exists
            result = driver.query(
                "MATCH (d:Document {filename_stem: $stem}) RETURN d.status AS status",
                {"stem": target_stem}
            )

            if not result:
                # Create placeholder
                now = int(time.time())
                driver.query(
                    """
                    MERGE (d:Document {filename_stem: $stem})
                    ON CREATE SET
                        d.status      = 'placeholder',
                        d.resolved    = false,
                        d.created_at  = $now,
                        d.modified_at = $now
                    """,
                    {"stem": target_stem, "now": now}
                )
                summary["placeholders_created"] += 1
                if verbose:
                    print(f"  [placeholder] {target_stem}")

            # Create LINKS_TO edge (MERGE = idempotent)
            driver.query(
                """
                MATCH (src:Document {filename_stem: $src_stem})
                MATCH (tgt:Document {filename_stem: $tgt_stem})
                MERGE (src)-[r:LINKS_TO {link_text: $link_text, link_format: $link_format}]->(tgt)
                """,
                {"src_stem": source_stem, "tgt_stem": target_stem,
                 "link_text": link_text, "link_format": link_format}
            )
            summary["edges_created"] += 1

    if verbose:
        print(f"[resolve_obsidian_links] Done.")
        print(f"  Documents scanned:    {summary['total_documents']}")
        print(f"  Links found:          {summary['total_links_found']}")
        print(f"  Edges created/merged: {summary['edges_created']}")
        print(f"  Placeholders created: {summary['placeholders_created']}")

    return summary


# ── QAChain Cypher prompt builder ─────────────────────────────────────────────

def build_cypher_system_prompt(config: Optional[dict] = None) -> str:
    """
    Build the system prompt for MemgraphQAChain Cypher generation.
    Injects vocabulary and property hints so the LLM generates correct Cypher.

    Args:
        config:  Output of load_schema_config(), or None to load automatically.

    Returns a prompt string with {schema} and {question} placeholders,
    suitable for use with langchain_core.prompts.PromptTemplate.
    """
    if config is None:
        config = load_schema_config()

    entity_types     = config["entity_types"]
    relationship_types = config["relationship_types"]
    property_hints   = config["property_hints"]
    schema_version   = config["schema_version"]

    entity_list  = "\n".join(f'  - "{t}"' for t in entity_types)
    rel_list     = "\n".join(f'  - {r}' for r in relationship_types)
    hint_list    = "\n".join(
        f'  - {prop}: {hint}' for prop, hint in property_hints.items()
    )

    prompt = f"""You are an expert at generating Cypher queries for Memgraph (schema version {schema_version}).
Use the schema below to understand the structure of the graph.

Graph schema:
{{schema}}

Entity type vocabulary — the `entity_type` property on :base nodes uses ONLY these exact values:
{entity_list}

When the user asks about a concept, map it to the closest entity_type:
  - "services", "applications", "software", "tools running" → use entity_type = 'service' or 'tool'
  - "machines", "servers", "computers", "hosts" → use entity_type = 'machine'
  - "settings", "configuration", "configs" → use entity_type = 'config'
  - "networks", "ports", "IPs", "connections" → use entity_type = 'network'
  - "people", "users", "authors" → use entity_type = 'person'

Relationship types available:
{rel_list}

Property hints:
{hint_list}

Rules:
1. Generate a single Cypher query only. No explanation, no markdown, no code fences.
2. Use MATCH, RETURN, WHERE, ORDER BY, LIMIT as appropriate.
3. For :Document nodes, filter by status = 'ingested' unless explicitly asked about placeholders.
4. Use note_timestamp_epoch (integer) for time comparisons, note_timestamp (string) for display.
5. If the question cannot be answered from the graph schema, return: RETURN "Question cannot be answered from available graph data" AS answer

Question: {{question}}"""

    return prompt
