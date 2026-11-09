# Deploying Synapse

Self-host a Synapse instance to index CMN spores, serve discovery queries, and provide resilience for your network.

## Prerequisites

- **Rust toolchain** — Install via [rustup](https://rustup.rs/)
- **A domain** — e.g., `synapse.example.com` with DNS A/AAAA record pointing to your server
- **Embedding service** (optional) — [Ollama](https://ollama.ai/) or OpenAI API key for semantic search

## 1. Build

Clone the synapse spore and build:

```bash
git clone https://cmn.dev/cmn/repos/synapse.git
cd synapse
cargo build --release
```

All features (PostgreSQL, redb, Nostr, semantic search) are included by default. To build with only specific features:

```bash
# Embedded storage only, no search or Nostr
cargo build --release --no-default-features --features redb
```

The binary is at `target/release/synapse`.

## 2. Configure

Synapse reads `config.yml` from the working directory. Start from the included example:

```bash
cp config.yml my-config.yml
```

**Minimal config (development):**

```yaml
debug: true
storage:
  backend: "redb"
  redb_path: "synapse.db"
http:
  enabled: true
  address: "0.0.0.0:3000"
https:
  enabled: false
```

**Production with reverse proxy (Caddy, nginx):**

```yaml
debug: false
log_format: "json"
storage:
  backend: "postgres"
  postgres_url_secret: "postgres://synapse:password@localhost/synapse"
http:
  enabled: true
  address: "127.0.0.1:3000"   # Only listen on localhost
https:
  enabled: false               # Reverse proxy handles TLS
```

**Production standalone (built-in ACME):**

```yaml
debug: false
log_format: "json"
storage:
  backend: "redb"
  redb_path: "/var/lib/synapse/synapse.db"
http:
  enabled: true
  address: "[::]:80"
https:
  enabled: true
  address: "[::]:443"
  domains:
    - "synapse.example.com"
  acme:
    contact: "admin@example.com"
    mode: "production"         # Let's Encrypt production certs
    renewal_before_days: 30
```

See the configuration fields above for all options.

## 3. Run

```bash
# Direct
./synapse

# With systemd
sudo cp synapse /usr/local/bin/
sudo systemctl enable --now synapse

# With supervisord
[program:synapse]
command=/home/ubuntu/synapse/bin/synapse
directory=/home/ubuntu/synapse/bin
autostart=true
autorestart=true
stderr_logfile=/home/ubuntu/synapse/bin/log/err.log
stdout_logfile=/home/ubuntu/synapse/bin/log/log.log
```

Runtime Agent-First Data log/protocol events are emitted on stdout; keep `stdout_logfile` as your primary operational log sink.

To bind to ports 80/443 without root:

```bash
sudo setcap 'cap_net_bind_service=+ep' /usr/local/bin/synapse
```

## 4. Verify

```bash
# Health check — should return a valid JSON body
curl http://localhost:3000/synapse/myceliums

# Send a test pulse from a publisher
hypha mycelium pulse --synapse https://synapse.example.com \
  --file path/to/spore.json

# Verify the spore was indexed
curl https://synapse.example.com/synapse/spore/<hash>
```

## 5. Enable Search (Optional)

Semantic search requires an embedding service. The simplest option is Ollama running locally:

```bash
# Install and start Ollama
ollama pull nomic-embed-text
```

Add to `config.yml`:

```yaml
search:
  enabled: true
  index_path: "synapse-search"
  embedding:
    provider: "ollama"
    url: "http://localhost:11434"
    model: "nomic-embed-text"
    dimensions: 768
  graph:
    enabled: true
    rebuild_on_startup: true
```

Restart synapse. To rebuild the search index from existing data:

```bash
synapse --rebuild-search
```

The command logs `indexed_count` and `failed_count`. Any storage enumeration,
read, JSON, or embedding failure produces exit 1 with `search_rebuild_failed`;
successfully indexed items remain available. Fix the reported service or data
failure and rerun. Only a complete rebuild exits 0.

## 6. Enable Nostr (Optional)

Nostr relay enables cross-instance synchronization:

```yaml
nostr:
  enabled: true
  relays:
    - "wss://relay.damus.io"
    - "wss://nos.lol"
  key_store: "storage"
  subscribe: true          # Receive CMN events from other instances
  forward_pulse: true      # Broadcast HTTP pulses to Nostr network
```

This activates the `/nostr` WebSocket endpoint and connects to external relays. A secp256k1 identity key is auto-generated on first run.

Incoming events are verified before relay history is updated. A valid duplicate
or older mycelium receives `OK=true`; a temporarily unavailable domain receives
`OK=false` with a retry reason and is not cached. Publishers must retry it.

`nostr.max_events` (default 10,000) must be positive and bounds the database as
well as memory. Startup trims older oversized history, and runtime evictions
delete database rows. A storage initialization/read failure prevents startup.
PostgreSQL retains its newest insertions, with event IDs breaking ties; redb
trims by event ID order. Each backend is deterministic independently.

Connections allow 32 subscriptions, a 64-message send queue, and 256 historical
events when REQ omits a limit (maximum 1,024). A slow sender or lost broadcast
closes the connection; clients must reconnect and resubscribe with an appropriate
bounded query.

For legacy history, stop the service, back up the database, then run this
explicit one-time operation before restarting:

```bash
synapse --prune-nostr --config config.yml
```

It retains only events whose exact content exists in the current CMN tables,
including the matching host capsule for spores. It removes unverified history
and legitimate relay-only events that are no longer represented there. No
network verification or automatic cleanup runs on subsequent startups.

## 7. Data Sources & Bootstrapping

A Synapse instance receives data through four channels:

| Source | Mechanism | Data Flow |
|--------|-----------|-----------|
| **HTTP Pulse** | `POST /synapse/pulse` | Domains push signed manifests directly to your instance |
| **Nostr Subscription** | Subscribe to external relays | Pull historical events on connect, then stream new events in real-time |
| **Nostr Relay** | `/nostr` WebSocket endpoint | Other instances or clients push CMN events to your relay |
| **Crawler** | Background retry loop (60s interval) | Re-fetches `cmn.json → mycelium → spores` from known domains that previously failed |

### Bootstrapping a New Instance

A fresh Synapse starts with an empty index. Enable Nostr subscription to pull historical data and listen for new events from the network:

```yaml
nostr:
  enabled: true
  relays:
    # Public Nostr relays — CMN events are stored here by other instances
    - "wss://relay.damus.io"
    - "wss://nos.lol"
    # Other Synapse instances also expose /nostr as a relay endpoint
    # - "wss://synapse.cmn.dev/nostr"
  key_store: "storage"
  subscribe: true          # Pull historical + real-time events
  forward_pulse: true      # Broadcast your pulses to the network
```

On startup, the Nostr subscription connects to each relay, sends a `REQ` for all CMN events (kind 30078, tagged `cmn-spore` or `cmn-mycelium`), receives all stored events, then continues listening for new ones. Each event is verified (CMN signature via cmn.json public key) before being indexed.

**Relay sources** (use any combination):

| Relay Type | Example | Notes |
|------------|---------|-------|
| Public Nostr relays | `wss://relay.damus.io` | CMN events forwarded here by instances with `forward_pulse: true` |
| Another Synapse's `/nostr` | `wss://synapse.example.com/nostr` | Every Synapse with Nostr enabled exposes a NIP-01 relay |

Public Nostr relays are the most resilient option — they operate independently of any CMN infrastructure. As long as at least one Synapse instance has been forwarding pulses to these relays, historical data is available for bootstrapping.

### How Domains Enter the Index

Domains become known to your instance through:

1. **Pulse** — A domain sends `hypha mycelium pulse --synapse https://your-instance/` after releasing
2. **Nostr** — Events from subscribed relays contain domain information, which is verified and indexed
3. **Crawler retry** — Network errors, timeouts, and 5xx responses enter persisted retry state. The background loop checks every 60 seconds with backoff; 404, invalid signatures, and mismatched content hashes are logged and skipped. Unchanged or older remote manifests still complete missing spores in the last verified local inventory.

There is no seed list or manual domain configuration — all domains are discovered dynamically through pulses and Nostr events.

### Cross-Instance Synchronization

When `forward_pulse: true`, every HTTP pulse received by your instance is also published to your configured Nostr relays. When `subscribe: true`, your instance listens for CMN events on those same relays. Public Nostr relays act as the shared transport layer between instances:

```
Instance A                 Public Relays              Instance B
    │                          │                          │
    │  forward_pulse ────────→ │                          │
    │                          │ ────────→ subscribe      │
    │                          │                          │
    │      subscribe ←──────── │                          │
    │                          │ ←──────── forward_pulse  │
```

Instances can also subscribe directly to each other's `/nostr` WebSocket endpoints for lower latency, but public relays provide resilience — if any instance goes offline, the data remains available on the relay network.

Each instance independently verifies all data via CMN signatures and cmn.json public keys — no trust is placed in the relay or the sending instance.

## Storage Backends

| Backend | Use Case | Config |
|---------|----------|--------|
| **redb** | Development, single-node, low traffic | `backend: "redb"` — zero-config, single-file |
| **PostgreSQL** | Production, high traffic, multiple queries | `backend: "postgres"` — requires PostgreSQL 14+ |

Both backends are compiled in by default. See [Synapse Storage](/tools/synapse/synapse-storage/) for schema details.

## Updating

Pull the latest source and rebuild:

```bash
cd cmn-tools
git pull
cargo build --release --package synapse
# Replace binary and restart
cp target/release/synapse /usr/local/bin/synapse
sudo systemctl restart synapse
```

## Operations

### Monitoring

The `/health` endpoint returns service status and component versions:

```bash
curl -s http://localhost:3000/health | jq .
```

Key metrics to monitor:

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| Indexed spore count | `GET /synapse/myceliums` | Sudden drop |
| Pulse processing latency | Structured logs (`duration_ms`) | > 5s |
| Storage size | PostgreSQL: `pg_database_size()` / Redb: file size | Disk > 80% |
| Certificate expiry | HTTPS config `renewal_before_days` | < 7 days |
| Failed domain retries | Structured logs (`retry_count`) | Stale > 24h |

Synapse emits structured JSON logs (when `log_format: "json"`) compatible with ELK, Loki, or CloudWatch. Filter by `"level":"ERROR"` for actionable alerts.

### Backup & Restore

**PostgreSQL:**

```bash
# Backup
pg_dump --format=custom synapse > synapse-backup.dump

# Restore
pg_restore --dbname=synapse synapse-backup.dump
```

**Redb:**

```bash
# Backup (stop synapse first, or use filesystem snapshot)
systemctl stop synapse
cp synapse.db synapse.db.bak
systemctl start synapse
```

Nostr events and search indexes are reconstructable from storage data. After restoring a backup, run `synapse --rebuild-search` to regenerate the search index.

### Certificate Renewal

When using built-in ACME (`https.acme.mode: "production"`):

- Certificates auto-renew `renewal_before_days` before expiry (default: 30 days)
- Renewed certs are stored in the database and persist across restarts
- Monitor logs for `"certificate renewed"` or `"certificate renewal failed"` messages
- If renewal fails, check DNS resolution and firewall rules for ACME HTTP-01 challenge (port 80)

When using a reverse proxy (Caddy/nginx), certificate management is handled by the proxy.

### Troubleshooting

| Symptom | Likely Cause | Resolution |
|---------|-------------|------------|
| Pulse returns 401 | Domain rotated its key; cached key is stale | Wait for cache TTL expiry (`cache.cmn_ttl_s`, default 5 min) |
| Pulse returns 502 | Domain's cmn.json unreachable | Check DNS/firewall; domain may be temporarily down |
| Search returns empty | Embedding service down or index stale | Check Ollama/OpenAI connectivity; run `synapse --rebuild-search` |
| Graph degraded to BFS | Graph init failed on startup | Check logs for "Failed to initialize"; restart with working embedding service |
| Nostr events not syncing | Relay connection failed | Check relay URLs in config; verify WebSocket connectivity |
| High memory usage | Too many Nostr events in memory | Lower `nostr.max_events` (default: 10,000) |
| Slow lineage queries | Deep BFS traversal | Lower `lineage.max_depth` (default: 50) |
