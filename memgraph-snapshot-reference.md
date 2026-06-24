# Memgraph Snapshot + WAL Reference
**Date:** 2026-06-24 — Block 10 session
**Scope:** memgraph-u2g on docker-services (applies equally to memgraph/Hermes instance)

---

## How Memgraph Persistence Works

Memgraph uses two complementary mechanisms:

**Snapshots** — a full copy of the graph at a specific point in time, written to
`/var/lib/memgraph/snapshots/`. Created by `CREATE SNAPSHOT;` or periodically
by Memgraph automatically (every 5 minutes by default, per `storage_snapshot_interval_sec: 300`).

**WAL (Write-Ahead Log)** — a continuous log of every write operation since the last
snapshot, stored in `/var/lib/memgraph/wal/`. Always being written as long as Memgraph
is running and data changes are made.

**At every startup**, Memgraph automatically:
1. Loads the most recent snapshot → establishes base state
2. Finds WAL files with timestamps newer than that snapshot → replays all writes
3. Result: the exact state the database was in when it last stopped

This means **normal stop/start never loses data** — the WAL brings you back to the
precise moment you stopped, regardless of when the last snapshot was taken.

---

## Three Scenarios — Each Requires a Different Procedure

### Scenario 1: Normal Stop and Start (no data change intended)

You stop Memgraph for maintenance, a config change, or container upgrade.
You want to resume exactly where you left off.

```bash
docker stop memgraph-u2g mcp-memgraph-u2g
# ... do whatever is needed ...
docker start memgraph-u2g mcp-memgraph-u2g
```

**Do NOT touch snapshots or WAL.** Memgraph loads latest snapshot + replays WAL
automatically. You resume at the exact state you left.

---

### Scenario 2: Rollback to a Specific Snapshot (intentional data loss)

Something went wrong — a failed ingest corrupted the graph, or you want to undo
recent changes. You want to restore to the exact state at snapshot time, discarding
everything that happened after.

```bash
# Step 1: Stop both containers
docker stop memgraph-u2g mcp-memgraph-u2g

# Step 2: Copy your chosen snapshot into the snapshots directory
docker cp ~/docker/memgraph-u2g-backups/snapshots/{snapshot_filename} \
    memgraph-u2g:/var/lib/memgraph/snapshots/{snapshot_filename}

# Step 3: *** CRITICAL *** Clear the WAL
# Without this, Memgraph replays all writes after the snapshot on startup —
# including the very changes you are trying to undo. The restore silently fails.
docker run --rm -v services_memgraph-u2g-data:/data \
    alpine sh -c "rm -f /data/wal/*"

# Step 4: Start — Memgraph loads snapshot only, no WAL replay
docker start memgraph-u2g mcp-memgraph-u2g
```

**Step 3 is the critical difference.** Without it, the restore silently fails.

---

### Scenario 3: Restore to Snapshot AND Keep Subsequent Valid Writes

You want to go back to a snapshot but replay only selected later writes.
Example: ingested documents A and B successfully, then C failed. You want
to restore to after B (the snapshot), not before A.

```bash
# Steps 1-4 as Scenario 2 (restore snapshot + clear WAL)

# Then replay only the good batch's rollback file:
python ingest.py --restore rollback/rollback-{good_batch_timestamp}.jsonl
```

JSONL rollback files are kept locally on MBA for 32 hours and on docker-services
for 7 days under `~/docker/memgraph-u2g-backups/rollback/`.

---

## Creating a Snapshot

### Manual (before a risky operation)
```bash
echo "CREATE SNAPSHOT;" | docker exec -i memgraph-u2g mgconsole
```
Returns the path of the created snapshot file inside the container.
No downtime required — hot operation while database continues running.

### Daily automated backup
```bash
~/docker/memgraph-u2g-backups/backup_graph.sh
```
Cron: `0 2 * * *` (2:00 AM Ho Chi Minh time)
Copies snapshot to: `~/docker/memgraph-u2g-backups/snapshots/`
Log: `~/docker/memgraph-u2g-backups/backup.log`

---

## Why Our First Restore Test Worked Without Clearing the WAL

In that test, we took a snapshot immediately before stopping the container.
There were no writes between the snapshot and the stop — the WAL contained
nothing newer than the snapshot. Memgraph loaded the snapshot, found no newer
WAL entries, and stopped there. The result looked correct but for the wrong reason.

In a real failure scenario with writes between the snapshot and the failure,
skipping the WAL clear would silently replay the corrupted writes.

**Always clear the WAL when the intent is rollback, not just restart.**

---

## Snapshot Compatibility

| Scenario | Method | Notes |
|---|---|---|
| Same container, same version | Snapshot file + docker cp | WAL must be cleared for rollback |
| Different container, same version | Copy snapshot to new container's snapshots dir | Fully compatible |
| Different version (migration) | `DUMP DATABASE;` → CYPHERL | Snapshot format is version-tied; CYPHERL is portable |

### Cross-version migration with DUMP DATABASE
```bash
# On the source instance
echo "DUMP DATABASE;" | docker exec -i memgraph-u2g mgconsole \
    --output-format=cypherl > migration.cypherl

# On the target instance (after starting fresh)
cat migration.cypherl | docker exec -i {target_container} mgconsole

# Recreate vector index (not included in DUMP DATABASE output)
echo "CREATE VECTOR INDEX vs_name ON :Chunk(embedding) \
    WITH CONFIG {\"dimension\": 1024, \"capacity\": 10000};" \
    | docker exec -i {target_container} mgconsole
```

---

## File Locations

| Path | Location | Purpose |
|---|---|---|
| Snapshots (in container) | `/var/lib/memgraph/snapshots/` | Memgraph's internal snapshots |
| WAL files (in container) | `/var/lib/memgraph/wal/` | Write-ahead log files |
| Named volume (host) | `/var/lib/docker/volumes/services_memgraph-u2g-data/_data/` | Underlying volume data (don't edit directly) |
| Backup snapshots (host) | `~/docker/memgraph-u2g-backups/snapshots/` | Exported snapshot copies |
| Rollback files (host) | `~/docker/memgraph-u2g-backups/rollback/` | ingest.py JSONL rollback files |
| Backup script | `~/docker/memgraph-u2g-backups/backup_graph.sh` | Daily backup script |
| Backup log | `~/docker/memgraph-u2g-backups/backup.log` | Cron job output |

---

## Quick Reference Commands

```bash
# Create snapshot (hot, no downtime)
echo "CREATE SNAPSHOT;" | docker exec -i memgraph-u2g mgconsole

# List snapshots inside container
docker exec memgraph-u2g ls /var/lib/memgraph/snapshots/

# Copy snapshot out of container
docker cp memgraph-u2g:/var/lib/memgraph/snapshots/{filename} \
    ~/docker/memgraph-u2g-backups/snapshots/{timestamp}_{filename}

# Copy snapshot into container (for restore)
docker cp ~/docker/memgraph-u2g-backups/snapshots/{file} \
    memgraph-u2g:/var/lib/memgraph/snapshots/{file}

# Clear WAL (ONLY when rolling back — destroys post-snapshot writes)
docker run --rm -v services_memgraph-u2g-data:/data \
    alpine sh -c "rm -f /data/wal/*"

# Verify graph state after restore
echo "MATCH (n) RETURN labels(n) AS type, count(n) AS count ORDER BY count DESC;" \
    | docker exec -i memgraph-u2g mgconsole
echo "MATCH (n:Chunk) RETURN count(n.embedding) AS with_embeddings, valueType(n.embedding) AS type LIMIT 1;" \
    | docker exec -i memgraph-u2g mgconsole
echo "SHOW INDEX INFO;" | docker exec -i memgraph-u2g mgconsole
```
