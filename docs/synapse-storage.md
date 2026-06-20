# Synapse Storage Design

Synapse is the CMN federated indexer and resilience layer, responsible for aggregating, verifying, and storing spore and mycelium data from sovereign domains.

## 1. Storage Backends

Synapse supports two storage backends, each gated behind a Cargo feature flag:

| Backend | Use Case | Features | Cargo Feature |
|---------|----------|----------|---------------|
| PostgreSQL | Production | ACID, GIN indexes, recursive CTEs | `postgres` |
| Redb | Development | Embedded, zero-config, file-based | `redb` |

Both features are enabled by default. To compile only the backend you need:

```bash
# Redb only (faster compile, no libpq dependency)
cargo build -p synapse --no-default-features --features redb

# PostgreSQL only
cargo build -p synapse --no-default-features --features postgres
```

The backend is selected at runtime via `config.yml`. If a backend is requested but was not compiled in, Synapse exits with a clear error message.

## 2. PostgreSQL Schema

```sql
-- Spores table: store spore content
-- Primary key: (domain, hash) - a spore belongs to a specific domain
CREATE TABLE spores (
    domain TEXT NOT NULL,
    hash TEXT NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (domain, hash)
);
CREATE INDEX idx_spores_hash ON spores(hash);
CREATE INDEX idx_spores_data ON spores USING GIN (data);

-- Mycelia table: store mycelium content and sync status
CREATE TABLE mycelia (
    domain TEXT PRIMARY KEY,
    data JSONB,
    public_key TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    retry_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    last_attempt_at TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    synced_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_mycelia_retry ON mycelia(status, next_retry_at);
CREATE INDEX idx_mycelia_data ON mycelia USING GIN (data);

-- Spore bonds table: lineage tracking (with relation types)
CREATE TABLE spore_bonds (
    spore_hash TEXT NOT NULL,
    bond_domain TEXT NOT NULL,
    bond_hash TEXT NOT NULL,
    bond_type TEXT NOT NULL,
    PRIMARY KEY (spore_hash, bond_domain, bond_hash)
);
CREATE INDEX idx_spore_bonds_hash ON spore_bonds(bond_hash);

-- Settings table: key-value store (Nostr keys, etc.)
CREATE TABLE settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- TLS certificates table
CREATE TABLE tls_certs (
    domains TEXT PRIMARY KEY,
    cert_pem TEXT NOT NULL,
    key_pem TEXT NOT NULL,
    expires_at TIMESTAMPTZ,
    self_signed BOOLEAN
);
```

## 3. Redb Table Structure

```
SPORES:      "domain:hash" (str) -> data (bytes)
HASH_INDEX:  hash (str) -> ["domain1:hash", "domain2:hash", ...] (vec<str>)
MYCELIA:     domain (str) -> record_json (bytes)
DOMAINS:     domain (str) -> [hash1, hash2, ...] (vec<str>)
INBOUND:     hash (str) -> [child_hash1, child_hash2, ...] (vec<str>)
SPORE_BONDS: hash (str) -> bonds_json (bytes)   # [{uri, relation}]
SETTINGS:    key (str) -> value (str)            # Nostr keys, etc.
TLS_CERTS:   domains (str) -> cert_json (bytes)
```

**Key Design:**
- **SPORES**: Uses "domain:hash" as composite key
- **HASH_INDEX**: Maps hash to all domain:hash keys for content-addressable queries
- **INBOUND**: Uses hash as key for O(log N) lineage lookup
- **SPORE_BONDS**: Stores typed bonds (spawned_from, absorbed_from, depends_on)
- **SETTINGS**: Generic key-value store for application state (e.g. Nostr nsec)

## 4. Data Model

### Spore

```
┌───────────────────────────────────────────┐
│              Spore (Capsule)              │
├───────────────────────────────────────────┤
│ $schema: ".../spore.json"                 │
│ capsule:                                  │
│   ├─ uri: "cmn://domain/b3.hash"         │
│   ├─ core:                               │
│   │   ├─ domain: String                  │
│   │   ├─ name: String                    │
│   │   ├─ synopsis: String                │
│   │   ├─ intent: [String]               │
│   │   ├─ mutations: [String]              │
│   │   ├─ license: String                │
│   │   └─ bonds: [Bond]                  │
│   ├─ core_signature: "ed25519...."       │
│   └─ dist: {platform: url}              │
│ capsule_signature: "ed25519...."          │
└───────────────────────────────────────────┘
```

### Mycelium

```
┌───────────────────────────────────────────┐
│            Mycelium (Capsule)             │
├───────────────────────────────────────────┤
│ $schema: ".../mycelium.json"              │
│ capsule:                                  │
│   ├─ uri: "cmn://domain"                 │
│   ├─ core:                               │
│   │   ├─ domain: String                  │
│   │   ├─ name: String                    │
│   │   ├─ synopsis: String                │
│   │   ├─ updated_at_epoch_ms: u64        │
│   │   └─ spores: [{id, hash, name, …}]  │
│   └─ core_signature: "ed25519...."       │
│ capsule_signature: "ed25519...."          │
└───────────────────────────────────────────┘
```

### Bond Graph

Spores form a Directed Acyclic Graph (DAG) through bonds:

```
     ┌──────────┐
     │ spore-A  │
     └────┬─────┘
          │ fork
    ┌─────┴─────┐
    ▼           ▼
┌──────┐    ┌──────┐
│ B    │    │ C    │
└──┬───┘    └──┬───┘
   │ extends   │ fork
   ▼           ▼
┌──────┐    ┌──────┐
│ D    │    │ E    │
└──────┘    └──────┘
```

## 5. Search & Graph Layer

When the `ruvector` feature is enabled, Synapse adds two optional in-memory layers on top of storage:

### Vector Search (VectorDB)

Spore metadata (name, description, domain, license, keywords) is embedded via an external API (Ollama or OpenAI) and indexed in a ruvector-core VectorDB with cosine distance. The VectorDB supports metadata filtering (domain, license) applied as a post-filter on search results.

```
save_spore() ──→ Storage (Redb/Postgres)   # source of truth
             ──→ VectorDB                   # search index
                   embed(text) → vector
                   insert(hash, vector, metadata)
```

The VectorDB index is persisted to disk at `search.index_path`.

### Evolution Graph (HypergraphIndex)

Spore relationships are modeled as temporal hyperedges in a ruvector-core HypergraphIndex:

| Relation | Hyperedge | Metadata |
|----------|-----------|----------|
| `spawned_from` | `[child, parent]` | `relation`, `child`, `parent` |
| `absorbed_from` | `[child, parent1, ...]` | `relation`, `child` |
| `depends_on` | `[child, dep]` | `relation` |
| `follows` | `[child, strain]` | `relation` |
| `extends` | `[child, parent]` | `relation` |

The graph is purely in-memory. On startup (when `rebuild_on_startup: true`), it iterates all domains and spores from storage to rebuild the full graph.

**Directed traversal**: HypergraphIndex's `k_hop_neighbors()` is undirected. To support directed lineage (descendants vs ancestors), SporeGraph maintains separate adjacency maps:

```
forward_edges: HashMap<parent_hash, HashSet<child_hash>>   # for descendants
reverse_edges: HashMap<child_hash, Vec<(parent_uri, relation)>>  # for ancestors
```

BFS on these maps provides O(V+E) directed traversal with zero I/O.

**HypergraphIndex features used**:
- `add_temporal_hyperedge()` — store relationships with timestamps
- `search_hyperedges()` — semantic search over edge embeddings (when `embed_relationships: true`)
- `query_temporal_range()` — find edges created within a time window
- `stats()` — node/edge counts

## 6. Algorithms

### Pulse Verification Flow

```
1. Validate $schema → determine manifest type (spore/mycelium/taste)
2. Extract domain → from capsule.core.domain
3. Fetch cmn.json → public key
4. Verify core_signature → Ed25519 over JCS-canonical capsule.core
5. Verify capsule_signature → Ed25519 over JCS-canonical capsule
6. Store to database
```

### Dual-Layer Signature Verification

```rust
fn verify_pulse_signatures(manifest, public_key):
    // 1. Parse algorithm-prefixed public key (e.g. "ed25519.5XmkQ9vZP8nL3x...")
    (algorithm, b58) = public_key.split_once('.')
    assert algorithm == "ed25519"
    pk_bytes = base58_decode(b58)
    verifying_key = Ed25519::from_bytes(pk_bytes)

    // 2. Verify core_signature over JCS-canonical capsule.core
    core = manifest.capsule.core
    core_canonical = jcs_serialize(core)
    core_sig = base58_decode(manifest.capsule.core_signature.strip_prefix("ed25519."))
    verifying_key.verify(core_canonical.as_bytes(), Ed25519Signature::from(core_sig))

    // 3. Verify capsule_signature over JCS-canonical capsule
    capsule = manifest.capsule
    capsule_canonical = jcs_serialize(capsule)
    capsule_sig = base58_decode(manifest.capsule_signature.strip_prefix("ed25519."))
    verifying_key.verify(capsule_canonical.as_bytes(), Ed25519Signature::from(capsule_sig))
```

### Lineage Tracking (PostgreSQL)

```sql
WITH RECURSIVE lineage AS (
    -- Base: find direct children
    SELECT spore_hash, 1 as depth
    FROM spore_bonds
    WHERE bond_hash = $1

    UNION

    -- Recursive: find grandchildren
    SELECT r.spore_hash, l.depth + 1
    FROM spore_bonds r
    INNER JOIN lineage l ON r.bond_hash = l.spore_hash
    WHERE l.depth < $2  -- max_depth limit
)
SELECT DISTINCT spore_hash FROM lineage;
```

### Lineage Tracking (Redb BFS)

```rust
fn get_lineage(hash, max_depth):
    result = []
    queue = [(hash, 0)]
    visited = Set()

    while !queue.is_empty():
        (current, depth) = queue.pop()
        if current in visited or depth >= max_depth:
            continue
        visited.add(current)

        children = inbound_table.get(current)
        for child in children:
            result.push(child)
            queue.push((child, depth + 1))

    return result
```

### Mycelium Sync Retry

```
┌──────────┐
│ pending  │  initial state
└────┬─────┘
     │ start sync
     ▼
┌──────────┐   success   ┌──────────┐
│ syncing  │ ──────────▶ │  synced  │
└────┬─────┘             └──────────┘
     │ failure
     ▼
┌──────────┐
│  failed  │ ◀───────┐
│ (waiting) │         │
└────┬─────┘         │
     │ next_retry_at │ failure
     │ reached       │
     ▼               │
┌──────────┐         │
│ syncing  │ ────────┘
└──────────┘
```

**Retry strategy (exponential backoff):**
- retry_count=1: 1 minute
- retry_count=2: 5 minutes
- retry_count=3: 15 minutes
- retry_count=4: 1 hour
- retry_count=5: 6 hours
- retry_count>=6: 24 hours (permanent daily retry)

## 7. Time Complexity

| Operation | PostgreSQL | Redb |
|-----------|------------|------|
| Store spore | O(1) + O(R) | O(1) + O(R) |
| Get spore | O(1) | O(log N) |
| Store mycelium | O(1) | O(1) |
| Get mycelium | O(1) | O(log N) |
| Domain spore list | O(K) | O(K) |
| Lineage tracking | O(V + E) | O(V + E) |
| Save/get setting | O(1) | O(log N) |

Where:
- R = number of bonds
- N = total records
- K = number of results
- V = number of involved spores
- E = number of bond edges

## 8. Nostr Event Storage

When the `nostr` feature is enabled, Synapse stores Nostr events in-memory for relay serving:

- Events are stored in a shared `Arc<RwLock<Vec<Event>>>` inside `NostrBridge`
- Deduplication by event ID prevents storing the same event twice
- Events arrive from three sources:
  1. HTTP pulse forwarding (`forward_pulse`)
  2. External relay subscription (`subscribe`)
  3. Direct WebSocket relay submissions (`/nostr` endpoint)

**Nostr Identity Key:**

The Nostr secp256k1 keypair is persisted via the `settings` table:

| key_store value | Storage | Key |
|-----------------|---------|-----|
| `"storage"` | Database `settings` table | `nostr_nsec` |
| `"file:<path>"` | Plain file (0600 permissions) | nsec bech32 string |

The key is auto-generated on first run if it does not exist.

**CMN Event Verification:**

All CMN content from Nostr events undergoes the same verification as HTTP pulses:

```
1. Parse event content as CMN pulse JSON
2. Validate protocol = "cmn/1"
3. Extract domain from URI
4. Fetch cmn.json, verify Ed25519 signature
5. Store spore/mycelium in database
```

## 9. Background Tasks

**Mycelium Retry Task:**
- Runs every 60 seconds
- Retries failed syncs at `next_retry_at`
- Never abandons failed domains

**Nostr Subscription Listener** (when `nostr.subscribe: true`):
- Long-running task subscribing to external relays
- Filters: `kind:30078 + #t:cmn-spore`
- Processes incoming events into CMN storage
- Deduplicates before storing

**Certificate Renewal Check:**
- Runs every 12 hours
- Logs warning if cert expires within `https.acme.renewal_before_days` (default 30 days)

**Graceful Shutdown:**
- `Ctrl+C` triggers graceful shutdown
- 5-second timeout for completion
