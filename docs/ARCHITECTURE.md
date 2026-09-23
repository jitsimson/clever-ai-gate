# Clever AI Gate — Architecture Document

> **High-performance AI Router & Orchestration Core** — A blazing-fast Go gateway that bridges applications to 1600+ AI models across any provider, with sub-millisecond routing overhead.

---

## Table of Contents

1. [Overview](#1-overview)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Request Lifecycle](#3-request-lifecycle)
4. [Core Subsystems](#4-core-subsystems)
   - 4.1 [Entry Point & Bootstrap](#41-entry-point--bootstrap)
   - 4.2 [Configuration](#42-configuration)
   - 4.3 [HTTP Router & Middleware](#43-http-router--middleware)
   - 4.4 [Proxy Engine (Hot-Path)](#44-proxy-engine-hot-path)
   - 4.5 [Credential Management & Load Balancing](#45-credential-management--load-balancing)
   - 4.6 [Stream Transmuxing](#46-stream-transmuxing)
   - 4.7 [Caching Layer](#47-caching-layer)
   - 4.8 [Database & Migrations](#48-database--migrations)
   - 4.9 [Telemetry Pipeline](#49-telemetry-pipeline)
   - 4.10 [Cluster Coordination](#410-cluster-coordination)
   - 4.11 [Health Checks](#411-health-checks)
   - 4.12 [Admin API](#412-admin-api)
   - 4.13 [Playground UI](#413-playground-ui)
5. [Data Model](#5-data-model)
6. [Security](#6-security)
7. [Performance Design Principles](#7-performance-design-principles)
8. [Deployment](#8-deployment)
9. [Project Structure](#9-project-structure)

---

## 1. Overview

Clever AI Gate is an **OpenAI-compatible API gateway** written in Go (Gin framework). It accepts requests in OpenAI's API format and intelligently routes them to any supported AI provider (OpenAI, Anthropic, Google Gemini, DeepSeek, Groq, Azure, AWS Bedrock, Cohere, and many more). The gateway abstracts away provider differences — translating request formats, rewriting URLs/headers, normalizing streaming responses, and managing credentials with automatic failover.

### Key Characteristics

| Metric | Target |
|--------|--------|
| Internal routing overhead | **< 1ms** p99 |
| Memory per request (hot-path) | **0** heap allocations |
| Credential selection | Lock-free, **< 100ns** |
| Cache lookup | **< 200ns** (Ristretto TinyLFU) |
| Stream TTFT overhead | **< 5ms** added |
| Binary size | **~8MB** (stripped) |

---

## 2. High-Level Architecture

```
                          ┌─────────────────────────────────┐
                          │      Client / IDE Extension     │
                          │   (OpenAI-compatible API calls)  │
                          └──────────────┬──────────────────┘
                                         │
                          Authorization: Bearer cag_<tenant_key>
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        Clever AI Gate (Gin Engine)                    │
│                                                                      │
│  ┌─────────┐  ┌────────────┐  ┌───────────────────────────────────┐  │
│  │  Auth   │→ │ Rate Limit │→ │         Proxy Handler             │  │
│  │Middleware│  │ Middleware │  │  (Zero-alloc hot-path)            │  │
│  └────┬────┘  └─────┬──────┘  │                                   │  │
│        │             │         │  1. Extract model from JSON body   │  │
│        ▼             ▼         │  2. Lookup credential pool (cache) │  │
│  ┌──────────┐  ┌──────────┐   │  3. Acquire credential (atomic)    │  │
│  │ L1 Cache │  │ Sliding  │   │  4. Rewrite URL + headers          │  │
│  │ Ristretto│  │ Window   │   │  5. Forward to upstream provider   │  │
│  │ + L2     │  │ (Redis / │   │  6. Transmux stream → OpenAI SSE   │  │
│  │  Redis   │  │  atomic) │   │  7. Emit telemetry (async)         │  │
│  └──────────┘  └──────────┘   └───────────┬───────────────────────┘  │
│                                            │                          │
│              ┌─────────────────────────────┼──────────────────────┐   │
│              │                             │                      │   │
│              ▼                             ▼                      ▼   │
│  ┌───────────────┐           ┌──────────────────┐    ┌──────────────┐│
│  │ Credential    │           │  Transmux Engine  │    │  Telemetry   ││
│  │ Pool Manager  │           │  (Anthropic→OAI,  │    │  Pipeline    ││
│  │ (Lock-free    │           │   Gemini→OAI,     │    │  (Async batch││
│  │  round-robin) │           │   Bedrock→OAI...) │    │   writes)    ││
│  └───────┬───────┘           └──────────────────┘    └──────┬───────┘│
│          │                                                  │        │
│          │          ┌──────────────────────┐                │        │
│          └─────────→│  Cluster Broadcaster │                │        │
│                     │  (Redis Pub/Sub for  │                │        │
│                     │   cross-node cooldown)│                │        │
│                     └──────────────────────┘                │        │
└─────────────────────────────────────────────────────────────┼────────┘
                                                              │
                    ┌─────────────────────────────────────────┼─────┐
                    │              Data Layer                  │     │
                    │                                         │     │
                    │   ┌────────────┐    ┌────────────────┐  │     │
                    │   │ PostgreSQL │    │     Redis      │  │     │
                    │   │ (Config,   │    │ (L2 Cache,     │  │     │
                    │   │  Logs,     │    │  Rate Limit,   │  │     │
                    │   │  Vectors)  │    │  Pub/Sub,      │  │     │
                    │   │            │    │  Telemetry Q)  │  │     │
                    │   └────────────┘    └────────────────┘  │     │
                    └─────────────────────────────────────────┘─────┘
                                         │
                    ┌────────────────────┴────────────────────┐
                    │          Upstream AI Providers           │
                    │  OpenAI · Anthropic · Google Gemini     │
                    │  DeepSeek · Groq · Together · Mistral   │
                    │  Azure · Bedrock · Cohere · NVIDIA      │
                    │  Ollama · Cloudflare · xAI · 1min.ai    │
                    │  Sarvam · Puter · OpenRouter · + more   │
                    └─────────────────────────────────────────┘
```

---

## 3. Request Lifecycle

A typical proxied request flows through these stages:

```
Client Request
    │
    ▼
┌──────────────────────────────────────────────────────────┐
│ 1. AUTH MIDDLEWARE                                        │
│    Extract Bearer token → L1 Ristretto lookup (~200ns)   │
│    → L2 Redis on miss (~1ms) → never hits DB on hot-path │
│    Attach tenant_id, rate_limit to context               │
├──────────────────────────────────────────────────────────┤
│ 2. RATE LIMIT MIDDLEWARE                                  │
│    Redis Lua sliding-window (if Redis available)          │
│    OR atomic in-memory counter per tenant                 │
│    429 + Retry-After header if exceeded                   │
├──────────────────────────────────────────────────────────┤
│ 3. PROXY HANDLER (Hot-Path)                               │
│    a. Read body into pooled buffer (sync.Pool, zero-alloc)│
│    b. Extract "model" field via buger/jsonparser (~0 alloc)│
│    c. Lookup BalancedChannelPool from Ristretto cache     │
│    d. AcquireActiveToken() — atomic round-robin           │
│    e. Rewriter.RewriteURL() — provider-specific path      │
│    f. Rewriter.RewriteHeaders() — provider auth format    │
│    g. Build upstream http.Request                         │
├──────────────────────────────────────────────────────────┤
│ 4. UPSTREAM REQUEST (with retry/failover)                 │
│    Forward via optimized HTTP client (TCP_NODELAY, HTTP/2)│
│    If 429/500/503 → PenalizeToken → retry next credential │
│    Retry up to maxAttempts with cascading fallback pools  │
├──────────────────────────────────────────────────────────┤
│ 5. RESPONSE HANDLING                                      │
│    Non-streaming → copy body to client                    │
│    Streaming → StreamProxy.ProxyStream():                 │
│      Read SSE lines → transmux to OpenAI format → flush   │
│      Accumulate response text + token estimate            │
├──────────────────────────────────────────────────────────┤
│ 6. TELEMETRY (Async, Non-Blocking)                        │
│    AcquireEntry() from pool → fill → Emit() to channel    │
│    Background worker: marshal → Redis LPUSH → DB bulk     │
│    insert (including pgvector embedding for semantic search)│
└──────────────────────────────────────────────────────────┘
    │
    ▼
Client Response (OpenAI-compatible format)
```

---

## 4. Core Subsystems

### 4.1 Entry Point & Bootstrap

**File:** `cmd/server/main.go`

The `main()` function orchestrates a strict 12-step initialization sequence:

1. **Load config** from environment variables (`.env` for local dev)
2. **Initialize LogHub** — dual-write structured logs to stdout + rotating log files
3. **Initialize Zap logger** — production JSON encoder, dual-core (stdout + LogHub)
4. **Database pool** — pgx connection pool (max 20 conns, DB never on hot-path)
5. **Run migrations** — embedded SQL migration files executed in order
6. **Redis client** (optional) — gracefully degrades if unavailable
7. **Encryption vault** — AES-256-GCM for provider API key decryption
8. **Ristretto cache** — TinyLFU in-memory cache store
9. **Tenant L2 cache** — two-layer Ristretto + Redis tenant lookup
10. **SyncManager** — loads routing pools from DB → cache, starts LISTEN/NOTIFY watcher
11. **Cluster broadcaster** (optional) — Redis Pub/Sub for cross-node cooldown sync
12. **Telemetry pipeline** — async write-behind queue with Redis + PostgreSQL
13. **HTTP transport & proxy handler** — optimized for streaming
14. **Gin engine** — route registration with tiered middleware
15. **Graceful shutdown** — SIGINT/SIGTERM → drain in-flight → close resources

### 4.2 Configuration

**File:** `internal/config/config.go`

All configuration is loaded from environment variables at startup using typed helper functions (zero reflection). Required variables (`DATABASE_URL`, `MASTER_ENCRYPTION_KEY`, `ADMIN_API_KEY`) cause a panic if missing. The config struct includes:

- **Server:** `PORT`, `GIN_MODE`
- **Database:** `DATABASE_URL`, `REDIS_URL`
- **Redis pool tuning:** pool size, min idle, timeouts
- **Security:** `MASTER_ENCRYPTION_KEY` (64 hex chars = 32 bytes), `ADMIN_API_KEY`
- **Cache:** `CACHE_MAX_SIZE_MB`, `CACHE_NUM_COUNTERS`
- **Telemetry:** flush interval, batch size, queue size
- **HTTP transport:** max idle conns, dial/keepalive timeouts
- **Rate limiting:** `DEFAULT_RATE_LIMIT_RPM`
- **Playground auth:** basic auth credentials

A `validate()` method enforces invariants (e.g., encryption key must be exactly 64 hex characters).

### 4.3 HTTP Router & Middleware

**File:** `internal/router/engine.go`

The Gin engine is created with `gin.New()` (no default middleware) and routes are organized into three tiers with different middleware stacks:

| Route Group | Path Prefix | Middleware | Purpose |
|-------------|-------------|------------|---------|
| **Health** | `/health`, `/ready` | None | Always responds, even during shutdown |
| **Proxy** | `/v1/*` | Auth + Rate Limit (minimal) | Maximum throughput hot-path |
| **Admin** | `/api/v1/admin/*` | CORS + Recovery + RequestID + AdminAuth | Full CRUD management API |
| **Swagger** | `/swagger/*` | None | Public API documentation |
| **Playground** | `/playground/*` | HTTP Basic Auth | Embedded Svelte admin UI |

**Key design decision:** The proxy group uses the absolute minimum middleware (auth + rate limiting only) to eliminate any unnecessary overhead on the hot-path. No request logging, no recovery middleware, no CORS — those are admin-only concerns.

#### Middleware Layers

| Middleware | File | Description |
|-----------|------|-------------|
| `ProxyAuth` / `ProxyAuthWithRedis` | `middleware/auth.go` | Validates tenant API key via L1 Ristretto (± L2 Redis) cache — zero DB calls |
| `AdminAuth` | `middleware/auth.go` | Constant-time comparison of admin API key |
| `RateLimiter` | `middleware/ratelimit.go` | In-memory atomic sliding-window counter per tenant |
| `RedisRateLimiter` | `middleware/redis_ratelimit.go` | Redis Lua-script sliding window (distributed) |
| `PlaygroundBasicAuth` | `middleware/basic_auth.go` | HTTP Basic Auth for the embedded playground UI |
| `CORS` | `middleware/recovery.go` | Cross-origin for admin API |
| `Recovery` | `middleware/recovery.go` | Panic recovery for admin routes (proxy has its own) |

### 4.4 Proxy Engine (Hot-Path)

**Files:** `internal/proxy/handler.go`, `transport.go`, `stream.go`, `rewriter.go`

The proxy handler is the performance-critical core. Its design principles:

#### Zero-Allocation Body Processing
- Request body is read into a `sync.Pool` buffer — no heap allocation per request
- Model name extraction uses `buger/jsonparser` (byte-level, no `json.Unmarshal`)
- Stream flag detection uses `strings.Contains` — no JSON parsing

#### URL & Header Rewriting (`rewriter.go`)
The `Rewriter` transforms OpenAI-compatible paths and headers to each provider's native format:

| Provider | Path Transformation | Auth Header |
|----------|-------------------|-------------|
| OpenAI, DeepSeek, Groq, etc. | Passthrough (`/v1/chat/completions`) | `Authorization: Bearer` |
| Anthropic | `/v1/chat/completions` → `/v1/messages` | `x-api-key` + `anthropic-version` |
| Google Gemini | → `/v1beta/models/{model}:streamGenerateContent` | `Authorization: Bearer` |
| Azure OpenAI | → `/openai/deployments/{model}/chat/completions` | `api-key` header |
| AWS Bedrock | → `/model/{model}/invoke` | `Authorization: Bearer` (SigV4) |
| Cohere | → `/v2/chat` | `Authorization: Bearer` |
| 1min.ai | → `/api/chat-with-ai` or `/api/features` | `API-KEY` header |
| Cloudflare | → `/accounts/{id}/ai/v1/chat/completions` | `Authorization: Bearer` |
| Ollama (Cloud) | → `/api/chat` | Passthrough |
| Sarvam | Passthrough | `api-subscription-key` + `Bearer` |

#### Optimized HTTP Transport (`transport.go`)
- **TCP_NODELAY** enabled via raw syscall — disables Nagle's algorithm so SSE token chunks flush

#### Stream Resilience & Mid-Stream Failure Rescue (`stream.go`, `continuation.go`)

Long generations (minutes long, thinking-heavy models) die mid-stream behind reseller
load balancers: `unexpected EOF`, `incomplete chunked read`, silent stalls, and in-band
error objects injected as SSE data chunks. Historically the first mid-flight failure
ended the response as a 502 — the client lost everything generated so far. The
stream subsystem now treats streaming as a resilient, repairable pipeline:

- **In-band error detection** (`stream.go`): a top-level `"error"` JSON key inside a
  `data:` chunk (the way OpenAI-compatible resellers abort mid-generation) is caught
  *before* forwarding the garbage to the client leg, and surfaced as `StreamResult.Err`.
- **Completion detection**: SSE/NDJSON streams are complete only on an explicit
  marker (`data: [DONE]` or a non-null `finish_reason`). A markerless EOF — exactly how
  resellers truncate generations around ~109s — is treated as an interruption. Gemini's
  JSON-array streams treat clean EOF as completion.
- **Lazy client-leg commit**: the 200/SSE headers are flushed only with the first
  byte of content. An upstream that dies before producing anything leaves the client
  leg pristine, so `executeWithRetry` can still rotate to another credential with a
  normal 502 (no double penalty — the retry loop owns cooldowns for that path).
- **Heartbeats** (`STREAM_HEARTBEAT_INTERVAL`, default 15s): `: keep-alive` SSE
  comments are emitted during silent upstream pauses (reasoning/thinking), keeping
  browsers, nginx instances and cloud routers from killing the connection as idle. A
  dedicated reader goroutine multiplexes upstream lines against heartbeat ticks and
  the client context, so beats fire even while the upstream `Read` blocks.
- **Idle watchdog** (`STREAM_IDLE_TIMEOUT`, default 5m): a stalled upstream body is
  force-closed and the stall converted into a readable error — silent hangs become
  recoverable interruptions instead of hung clients.
- **Seamless continuation** (`MAX_STREAM_CONTINUATIONS`, default 2): when a stream
  dies after content reached the client, the gateway re-requests the same upstream
  with the partial assistant output spliced into `messages` via `jsonparser`
  (in-place, reflection-free) plus a continue-instruction user turn, and pipes the
  new SSE leg into the *same client response*. The IDE never notices — unless every
  rescue leg fails, in which case the response is closed with a synthetic
  `finish_reason:"length"` chunk + `[DONE]` so clients render a visibly-cut answer
  instead of hanging forever. Mid-tool-call interruptions are not resumed (merged
  arguments JSON would corrupt the call) and instead finish with
  `finish_reason:"tool_calls"`.
- Interruption causes are logged at `Error` level ("upstream stream interrupted
  mid-flight — attempting seamless continuation" / "rotating to next credential")
  so gateway logs finally surface the failure classes that previously only appeared
  in client UIs.