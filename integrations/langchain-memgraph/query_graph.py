#!/usr/bin/env python3
"""
Block 10 Part D — langchain-memgraph exploration
MemgraphQAChain: NL → Cypher → answer
MemgraphToolkit: LangGraph agent with graph tools

Target: memgraph-u2g (port 7688) — 83 entities, 7 chunks, 184 edges
LLM: LiteLLM proxy → primary model
"""

import json
import os
from dotenv import load_dotenv
from langchain_memgraph.graphs.memgraph import MemgraphLangChain
from langchain_memgraph.chains.graph_qa import MemgraphQAChain
from langchain_memgraph import MemgraphToolkit
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

load_dotenv()

MEMGRAPH_URL = os.getenv("MEMGRAPH_URL", "bolt://localhost:7687")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "dummy")

# ── Schema config loader (reads schema_config.json from unstructured2graph) ──
SCHEMA_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "../../unstructured2graph/schema_config.json"
)

def load_schema_config() -> dict:
    with open(SCHEMA_CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def build_cypher_system_prompt(config: dict) -> str:
    entity_types   = config["entity_types"]
    rel_types      = config["relationship_types"]
    property_hints = config["property_hints"]
    schema_version = config["schema_version"]

    entity_list = "\n".join(f'  - "{t}"' for t in entity_types)
    rel_list    = "\n".join(f'  - {r}' for r in rel_types)
    hint_list   = "\n".join(
        f'  - {prop}: {hint}' for prop, hint in property_hints.items()
    )

    return f"""You are an expert at generating Cypher queries for Memgraph (schema version {schema_version}).
Use the schema below to understand the structure of the graph.

Graph schema:
{{schema}}

Entity type vocabulary — the entity_type property on :base nodes uses ONLY these exact values:
{entity_list}

When the user asks about a concept, map it to the closest entity_type:
  - "services", "applications", "software", "running processes" -> entity_type = 'service'
  - "machines", "servers", "computers", "hosts" -> entity_type = 'machine'
  - "settings", "configuration files", "configs" -> entity_type = 'config'
  - "networks", "ports", "IPs", "addresses" -> entity_type = 'network'
  - "tools", "utilities", "CLIs" -> entity_type = 'tool'
  - "people", "users", "authors" -> entity_type = 'person'

Relationship types available:
{rel_list}

Property hints:
{hint_list}

Rules:
1. Generate a single Cypher query only. No explanation, no markdown, no code fences.
2. Use MATCH, RETURN, WHERE, ORDER BY, LIMIT as appropriate.
3. For :Document nodes, filter by status = 'ingested' unless asked about placeholders.
4. Use note_timestamp_epoch (integer) for time comparisons, note_timestamp (string) for display.
5. If unanswerable from graph data, return: RETURN "Question cannot be answered from available graph data" AS answer

Cypher examples (study these patterns — always use MATCH, never FROM):
  Q: What entity types exist?
  A: MATCH (n:base) RETURN DISTINCT n.entity_type AS entity_type ORDER BY entity_type

  Q: What services are mentioned?
  A: MATCH (n:base) WHERE n.entity_type = 'service' RETURN n.entity_id, n.description LIMIT 20

  Q: What machines are in the graph?
  A: MATCH (n:base) WHERE n.entity_type = 'machine' RETURN n.entity_id, n.description LIMIT 20

  Q: What chunks reference a specific entity?
  A: MATCH (n:base)-[:MENTIONED_IN]->(c:Chunk) WHERE n.entity_id = 'ai-main' RETURN c.text LIMIT 5

Question: {{question}}"""

# ── 1. Connect to memgraph-u2g and inspect schema ───────────────────────────
print("=== Connecting to memgraph-u2g ===")
graph = MemgraphLangChain(
    url=MEMGRAPH_URL,
    username="",
    password="",
    refresh_schema=True,
)

print("=== Schema ===")
print(graph.schema)
print()

# ── 2. LLM via LiteLLM proxy ─────────────────────────────────────────────────
llm = ChatOpenAI(
    model="primary",
    base_url=OPENAI_BASE_URL,
    api_key=OPENAI_API_KEY,
    temperature=0,
)

# ── 3. QAChain — NL → Cypher → answer ───────────────────────────────────────
schema_config = load_schema_config()
cypher_prompt_str = build_cypher_system_prompt(schema_config)

from langchain_core.prompts import PromptTemplate
cypher_prompt = PromptTemplate(
    input_variables=["schema", "question"],
    template=cypher_prompt_str,
)

chain = MemgraphQAChain.from_llm(
    llm,
    graph=graph,
    allow_dangerous_requests=True,
    verbose=True,
    cypher_prompt=cypher_prompt,
)

questions = [
    "What are all the entity types in the graph?",
    "What services are mentioned in the graph?",
    "What machines are part of the infrastructure?",
]

print("=== QAChain Results ===")
for q in questions:
    print(f"\nQ: {q}")
    response = chain.invoke(q)
    print(f"A: {response['result']}")

# ── 4. MemgraphToolkit + LangGraph agent ─────────────────────────────────────
print("\n=== MemgraphToolkit Agent ===")
toolkit = MemgraphToolkit(db=graph, llm=llm)
tools = toolkit.get_tools()
print(f"Available tools: {[t.name for t in tools]}")

agent = create_react_agent(llm, tools)
result = agent.invoke({
    "messages": [("user", "What node labels exist in the database? Show me the schema.")]
})
print(f"\nAgent response:\n{result['messages'][-1].content}")
