#!/usr/bin/env python3
"""
resolve_links.py — Run resolve_obsidian_links() on memgraph-u2g.

Scans all ingested :Document nodes for [[stem]] and [text](<path.md>) links,
creates LINKS_TO edges, and creates placeholder :Document nodes for unresolved targets.

Safe to re-run — all writes use MERGE (idempotent).

Usage:
    cd unstructured2graph
    source .venv/bin/activate
    python3 resolve_links.py [--verbose]
"""

import sys
import os
from dotenv import load_dotenv
from memgraph_toolbox.api.memgraph import Memgraph
from graph_schema import resolve_obsidian_links

load_dotenv()

verbose = "--verbose" in sys.argv or "-v" in sys.argv

print(f"Connecting to memgraph at {os.getenv('MEMGRAPH_URL')} ...")
driver = Memgraph(user_agent="idea-resolve-links")

print("Running resolve_obsidian_links() ...")
resolve_obsidian_links(driver, verbose=verbose)
print("Done.")
