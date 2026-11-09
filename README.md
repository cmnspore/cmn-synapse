# Synapse

The reference CMN indexer — ingests signed manifests, indexes spores and myceliums, serves discovery queries, and syncs across instances via Nostr.

Synapse is the federated indexer and resilience layer for CMN — the network's "Search Engine" and "Backup Layer". It crawls sovereign domains, aggregates metadata, and serves as content fallback when origin servers are offline.

The Synapse API follows the `strain-synapse`, `strain-synapse-search`, and
`strain-synapse-nostr` conventions. The routes implemented by this server are
listed below.

## Installation

```bash
# All features (default: postgres + redb + nostr + ruvector)
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
debug: false
log_format: json

storage:
  backend: postgres
  postgres_url_secret: "postgresql://user:password@localhost/synapse"

http:
  enabled: true
  address: "0.0.0.0:80"

https:
  enabled: true
  address: "0.0.0.0:443"
  domains:
    - synapse.cmn.dev
  self_signed_validity_days: 365
  acme:
    contact: admin@cmn.dev
    mode: production
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

`synapse --help` returns a strict `cli-help-v2` AFDATA event. Use
`synapse --docs` for the complete generated CLI reference. Structural CLI
errors use stderr and exit 2; service/domain failures use a strict JSON error
event on stderr and exit 1.

HTTP JSON responses are strict AFDATA protocol-v1 result/error envelopes.
Service and access logs use the configured `log_format` (`json`, `yaml`, or
`plain`) and redact `_secret`, `_url`, legacy embedding `url`, and legacy Nostr
`relays` values before serialization.

## API Overview

Base URL: `https://synapse.cmn.dev`

```bash
# Spore API (content-addressable)
GET /synapse/spore/:hash              # Get spore by hash
GET /synapse/spore/:hash/bonds?direction=inbound  # Descendants
GET /synapse/spore/:hash/bonds?direction=outbound # Ancestors
GET /synapse/spore/:hash/tastes       # Taste reports about a spore

# Domain, mycelium, and taste fallback
GET /synapse/cmn/:domain              # Get cached cmn.json
GET /synapse/cmn/:domain/tastes       # Taste reports about a domain
GET /synapse/mycelium/:hash           # Get mycelium by hash
GET /synapse/taste/:hash              # Get taste report by hash

# Pulse API (receive signed content)
POST /synapse/pulse                   # Receive signed CMN content

# Search API (requires ruvector feature + embedding service)
GET /synapse/search?q=...&domain=...&limit=10

# Nostr Relay (WebSocket, NIP-01)
GET /synapse/nostr                    # WebSocket relay endpoint

GET /health                          # Service health
GET /synapse/admin/status             # Administrative status
```

### Example Requests

```bash
# Get spore by hash
curl https://synapse.cmn.dev/synapse/spore/b3.3yMR7vZQ9hL2xKJdFtN8wPcB6sY1mXgU4eH5pTa2

# Find all forks/evolutions
curl 'https://synapse.cmn.dev/synapse/spore/b3.3yMR7vZQ9hL2xKJdFtN8wPcB6sY1mXgU4eH5pTa2/bonds?direction=inbound&max_depth=5'

# Find ancestors
curl 'https://synapse.cmn.dev/synapse/spore/b3.3yMR7vZQ9hL2xKJdFtN8wPcB6sY1mXgU4eH5pTa2/bonds?direction=outbound&max_depth=5'

# Get domain entry fallback
curl https://synapse.cmn.dev/synapse/cmn/cmn.dev

# Semantic search (requires ruvector feature)
curl "https://synapse.cmn.dev/synapse/search?q=HTTP+client+library&limit=10"
```

## Integration with Hypha

Notify Synapse after releasing:

```bash
# Release spore
hypha release --domain cmn.dev

# Notify synapse using the generated domain entry
hypha mycelium pulse --file ~/.cmn/mycelium/cmn.dev/public/.well-known/cmn.json --synapse https://synapse.cmn.dev
```

Query evolution via Hypha:

```bash
# Trace ancestors
hypha lineage cmn://cmn.dev/b3.3yMR7vZQ9hL2xKJdFtN8wPcB6sY1mXgU4eH5pTa2 --direction out --synapse https://synapse.cmn.dev

# Find descendants
hypha lineage cmn://cmn.dev/b3.3yMR7vZQ9hL2xKJdFtN8wPcB6sY1mXgU4eH5pTa2 --synapse https://synapse.cmn.dev

# Auto-discover for absorb
hypha absorb --discover --synapse https://synapse.cmn.dev
```

## Nostr Integration

Synapse optionally integrates with the [Nostr](https://github.com/nostr-protocol/nostr) protocol for decentralized event distribution. When enabled, Synapse acts as both a **Nostr client** and a **CMN-only Nostr relay**.

- **Relay**: Accepts WebSocket connections at `/synapse/nostr`, serves CMN events (kind 30078, NIP-01)
- **Subscriber**: Connects to external Nostr relays, listens for CMN spore/mycelium events
- **Forwarder**: Publishes HTTP pulse events to configured Nostr relays

Any Synapse instance can connect to another's `/synapse/nostr` endpoint and receive its retained CMN events.

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

Incoming events must pass CMN schema, signature, and fresh domain identity verification before entering relay history. Publishers receive NIP-01 `OK` acknowledgements; an unavailable domain is rejected with a retry reason and can be resubmitted. Valid duplicates and older myceliums are accepted without replacing newer CMN data.

`nostr.max_events` bounds both memory and persistent history and must be greater than zero. Startup trims an older oversized window; evictions delete the corresponding database events. Each connection has a 64-message send queue, at most 32 subscriptions, and a default history limit of 256 events (maximum 1,024). Slow or lagged connections close and must resubscribe.

For an instance upgraded from a version that cached unverified events, stop the service and run the explicit one-time cleanup:

```bash
synapse --prune-nostr --config config.yml
```

The cleanup retains only event content exactly represented in the CMN tables. It also removes legitimate relay-only history, including older manifests no longer in those tables; review this retention policy before running it. It does not run on every startup.

## Search & Graph

Synapse optionally provides **semantic search** over spores and an **in-memory evolution graph** for fast lineage queries. Both require the `ruvector` feature and an external embedding service (Ollama or OpenAI).

### Search

Vector similarity search over spore metadata (name, description, keywords). Embeddings are generated via an external API and stored in a [ruvector-core](https://crates.io/crates/ruvector-core) VectorDB index.

### Graph

The evolution graph uses ruvector-core's HypergraphIndex to model spore relationships (spawned_from, absorbed_from, depends_on, follows, extends) as hyperedges. It serves as an **in-memory acceleration layer** — storage (Redb/Postgres) remains the source of truth.

When enabled, inbound and outbound bond queries use the in-memory graph,
falling back to storage BFS if the graph has no data for the requested hash.
Relationship search, temporal queries, and statistics are available internally
in the graph implementation; this server exposes bond traversal over HTTP.

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
| `ruvector` | `ruvector-core` | Yes |

Default features: `postgres`, `redb`, `nostr`, and `ruvector`. Use
`--no-default-features --features <feature,...>` to compile only what you need.

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

# Real PostgreSQL regressions, using a disposable Docker PostgreSQL 18 fixture
scripts/test-postgres.sh

# Run with debug logging
RUST_LOG=debug cargo run

# Run with plain text logs (development)
# Add to config.yml: log_format: "plain"
```

The PostgreSQL entry fails if Docker is unavailable. Alternatively, provide
`SYNAPSE_TEST_POSTGRES_URL_SECRET` through the environment; tests create and
remove isolated schemas and require permission to create schemas. Default tests
use redb and local HTTP mocks. Live embedding quality checks remain separate.

## Documentation

| Document | Description |
|----------|-------------|
| [CLI Reference](docs/cli.md) | Generated closed-world CLI shapes and output contracts |
| [Deployment Guide](docs/synapse-deployment.md) | Self-host a Synapse instance — configuration, services, operations |
| [Storage Internals](docs/synapse-storage.md) | Database schema, indexing, and storage backends |
