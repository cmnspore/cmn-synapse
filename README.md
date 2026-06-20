# Synapse

The reference CMN indexer — ingests signed manifests, indexes spores and myceliums, serves discovery queries, and syncs across instances via Nostr.

Synapse is the federated indexer and resilience layer for CMN — the network's "Search Engine" and "Backup Layer". It crawls sovereign domains, aggregates metadata, and serves as content fallback when origin servers are offline.

> **API Contract**: The Synapse API is defined by CMN strains — see [strain-synapse](https://cmn.dev/cmn/spore/), [strain-synapse-search](https://cmn.dev/cmn/spore/), and [strain-synapse-nostr](https://cmn.dev/cmn/spore/) conventions for the full specification.

## Installation

```bash
# All features (default: postgres + redb + nostr)
cargo build --release

# With semantic search + graph (requires external embedding service)
cargo build --release --features ruvector

# Without Nostr
cargo build --release --no-default-features --features postgres,redb

# Redb only, no Nostr
cargo build --release --no-default-features --features redb
```

```bash
cp target/release/synapse /usr/local/bin/
```

## Quick Start

### Development (Redb)

```bash
cargo run
# Server starts at http://localhost:3000
```

### Production (PostgreSQL + HTTPS)

Create `config.yml`:

```yaml
storage:
  type: postgres
  url: "postgresql://user:pass@localhost/synapse"

http:
  enabled: true
  port: 80

https:
  enabled: true
  port: 443
  domains:
    - synapse.cmn.dev
  self_signed_validity_days: 365
  acme:
    enabled: true
    email: admin@cmn.dev
    renewal_before_days: 30

cache:
  cmn_ttl_s: 300     # cmn.json cache TTL in seconds (default: 5 minutes)

pulse:
  max_clock_skew_s: 300  # Maximum clock skew in seconds (default: 5 minutes)
```

Run:

```bash
cargo run
# Connects to PostgreSQL
# Obtains TLS certificate via Let's Encrypt
# Starts HTTP (80) and HTTPS (443)
```

## API Overview

Base URL: `https://synapse.cmn.dev/v1`

```bash
# Spore API (content-addressable)
GET /v1/spore/:hash                    # Get spore by hash
GET /v1/spore/:hash/lineage            # Find descendants
GET /v1/spore/:hash/rhizome            # Complete evolution network

# Mycelium API (domain-scoped fallback)
GET /v1/myceliums                      # List all cached myceliums
GET /v1/mycelium/:domain               # Get cached mycelium
GET /v1/mycelium/:domain/spore/:hash   # Get spore by domain+hash
GET /v1/mycelium/:domain/spores        # List domain's spores

# Pulse API (receive signed content)
POST /v1/pulse                         # Receive signed spore/mycelium

# Search API (requires ruvector feature + embedding service)
GET /v1/search?q=...&domain=...&limit= # Semantic spore search

# Graph API (requires ruvector feature + graph.enabled)
GET /v1/graph/search?q=...&limit=      # Semantic relationship search
GET /v1/graph/temporal?start=...&end=.. # Temporal evolution query
GET /v1/graph/stats                    # Graph statistics

# Nostr Relay (WebSocket, NIP-01)
GET /nostr                             # WebSocket relay endpoint
```

### Example Requests

```bash
# Get spore by hash
curl https://synapse.cmn.dev/v1/spore/blake3_8ccf403b0447d5e23d54fd56f7a288bab8e65c7324e90da8e769ad28049433c4

# Find all forks/evolutions
curl https://synapse.cmn.dev/v1/spore/blake3_8ccf403b0447d5e23d54fd56f7a288bab8e65c7324e90da8e769ad28049433c4/lineage

# Get complete ancestry + descendants
curl https://synapse.cmn.dev/v1/spore/blake3_8ccf403b0447d5e23d54fd56f7a288bab8e65c7324e90da8e769ad28049433c4/rhizome?max_depth=5

# Get mycelium fallback
curl https://synapse.cmn.dev/v1/mycelium/cmn.dev

# Semantic search (requires ruvector feature)
curl "https://synapse.cmn.dev/v1/search?q=HTTP+client+library&limit=10"

# Relationship search (requires graph.enabled)
curl "https://synapse.cmn.dev/v1/graph/search?q=forked+for+security&limit=5"

# Temporal evolution query
curl "https://synapse.cmn.dev/v1/graph/temporal?start=1700000000&end=1710000000"

# Graph stats
curl https://synapse.cmn.dev/v1/graph/stats
```

## Integration with Hypha

Notify Synapse after releasing:

```bash
# Release spore
hypha release --domain cmn.dev

# Notify synapse
hypha mycelium pulse --domain cmn.dev --synapse https://synapse.cmn.dev
```

Query evolution via Hypha:

```bash
# Trace ancestors
hypha ancestors cmn://cmn.dev/spore/blake3_8ccf403b0447d5e23d54fd56f7a288bab8e65c7324e90da8e769ad28049433c4 --synapse https://synapse.cmn.dev

# Find descendants
hypha lineage cmn://cmn.dev/spore/blake3_8ccf403b0447d5e23d54fd56f7a288bab8e65c7324e90da8e769ad28049433c4 --synapse https://synapse.cmn.dev

# Auto-discover for absorb
hypha absorb --lineage --synapse https://synapse.cmn.dev
```

## Nostr Integration

Synapse optionally integrates with the [Nostr](https://github.com/nostr-protocol/nostr) protocol for decentralized event distribution. When enabled, Synapse acts as both a **Nostr client** and a **CMN-only Nostr relay**.

- **Relay**: Accepts WebSocket connections at `/nostr`, serves CMN events (kind 30078, NIP-01)
- **Subscriber**: Connects to external Nostr relays, listens for CMN spore/mycelium events
- **Forwarder**: Publishes HTTP pulse events to configured Nostr relays

Any Synapse instance can connect to another's `/nostr` endpoint and sync all historical CMN data.

### Configuration

Add to `config.yml`:

```yaml
nostr:
  enabled: true
  relays:
    - "wss://relay.damus.io"
  key_store: "storage"        # "storage" (database, default) or "file:nostr.key"
  subscribe: true             # Listen for CMN events on external relays
  forward_pulse: false        # Forward HTTP pulses to Nostr relays
```

The Nostr identity key (secp256k1) is auto-generated on first run. By default it is stored in the database (`key_store: "storage"`). Use `key_store: "file:path/to/key"` to store it as a plain file instead.

## Search & Graph

Synapse optionally provides **semantic search** over spores and an **in-memory evolution graph** for fast lineage queries. Both require the `ruvector` feature and an external embedding service (Ollama or OpenAI).

### Search

Vector similarity search over spore metadata (name, description, keywords). Embeddings are generated via an external API and stored in a [ruvector-core](https://crates.io/crates/ruvector-core) VectorDB index.

### Graph

The evolution graph uses ruvector-core's HypergraphIndex to model spore relationships (spawned_from, absorbed_from, depends_on, follows, extends) as hyperedges. It serves as an **in-memory acceleration layer** — storage (Redb/Postgres) remains the source of truth.

When enabled, lineage and rhizome queries are served from the in-memory graph (O(V+E) with no I/O), falling back to storage BFS if the graph has no data for the requested hash.

Additional graph-only capabilities:
- **Relationship search**: semantic search over edge descriptions (e.g., "forked for security fix")
- **Temporal queries**: find evolution events within a time range
- **Graph stats**: node/edge counts and average degree

### Configuration

Add to `config.yml`:

```yaml
search:
  enabled: true
  index_path: "synapse-search"
  embedding:
    provider: "ollama"          # "ollama" or "openai"
    url: "http://localhost:11434"
    model: "bge-large"
    dimensions: 1024
  graph:
    enabled: true
    rebuild_on_startup: true    # Rebuild graph from storage on boot
    embed_relationships: false  # Embed edge descriptions (costs API calls)
    rebuild_batch_size: 100
```

Build with `--features ruvector`.

## Features

| Feature | Dependency | Default |
|---------|------------|---------|
| `postgres` | `sqlx` | Yes |
| `redb` | `redb` | Yes |
| `nostr` | `nostr-sdk` | Yes |
| `ruvector` | `ruvector-core` | No |

Default features: `postgres`, `redb`, `nostr`. Use `--no-default-features --features <feature,...>` to compile only what you need. Search is opt-in via `--features ruvector`.

## Development

```bash
# Build (default: all features)
cargo build

# Build with redb only (faster compile, no libpq needed)
cargo build --no-default-features --features redb

# Build without Nostr (smaller binary, no nostr-sdk dependency)
cargo build --no-default-features --features redb

# Test
cargo test

# Run with debug logging
RUST_LOG=debug cargo run

# Run with plain text logs (development)
# Add to config.yml: log_format: "plain"
```

## Documentation

| Document | Description |
|----------|-------------|
| [Deployment Guide](docs/synapse-deployment.md) | Self-host a Synapse instance — configuration, services, operations |
| [Storage Internals](docs/synapse-storage.md) | Database schema, indexing, and storage backends |
