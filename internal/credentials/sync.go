package credentials

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/skadraneshghn/clever-ai-gate/internal/cache"
	"github.com/skadraneshghn/clever-ai-gate/internal/database"
	"go.uber.org/zap"
)

// ActiveModel holds the routing key and detected capabilities for a model.
// Stored in the Ristretto cache under "system:active_models" as []ActiveModel.
// The Capabilities map is decoded from the JSONB column at cache load time —
// zero extra DB round-trips at request time.
type ActiveModel struct {
	Pattern      string          `json:"id"`
	Capabilities map[string]bool `json:"capabilities,omitempty"`
}

// SyncManager handles real-time configuration synchronization between
// PostgreSQL and the in-memory routing cache using LISTEN/NOTIFY.
//
// When an admin updates a config via the management API, PostgreSQL triggers
// fire a notification. This manager receives it and atomically swaps the
// affected routing pool in cache — zero downtime, zero lock contention.
// The manager also writes to Redis L2 after every Ristretto L1 update so that
// all gateway replicas share a common fast-path cache via RedisCacheManager.
type SyncManager struct {
	pool           *pgxpool.Pool
	cache          *cache.Store
	vault          *Vault
	logger         *zap.Logger
	redisCacheMgr  *cache.RedisCacheManager // nil when Redis not configured; all methods nil-safe
	pools          atomic.Pointer[map[string]*BalancedChannelPool] // Atomic pointer for lock-free reads
	ctx            context.Context
	cancel         context.CancelFunc
	reloadChan     chan string // Queues notifications for debounced reload execution
}

// NewSyncManager creates a new configuration sync manager.
func NewSyncManager(pool *pgxpool.Pool, cacheStore *cache.Store, vault *Vault, logger *zap.Logger) *SyncManager {
	ctx, cancel := context.WithCancel(context.Background())
	sm := &SyncManager{
		pool:       pool,
		cache:      cacheStore,
		vault:      vault,
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
		reloadChan: make(chan string, 100),
	}
	// Initialize with empty map
	emptyMap := make(map[string]*BalancedChannelPool)
	sm.pools.Store(&emptyMap)
	return sm
}

// SetRedisCacheManager attaches an optional Redis cache manager.
// Called from main.go after both SyncManager and RedisCacheManager are created.
// Must be called before LoadInitialState to ensure Redis pre-warming works.
func (sm *SyncManager) SetRedisCacheManager(mgr *cache.RedisCacheManager) {
	sm.redisCacheMgr = mgr
}

// LoadInitialState loads all routing pools from the database into cache.
// Called once at startup before the server starts accepting requests.
func (sm *SyncManager) LoadInitialState(ctx context.Context) error {
	poolRows, credsByPool, err := database.LoadAllPoolsWithCredentials(ctx, sm.pool)
	if err != nil {
		return fmt.Errorf("failed to load initial state: %w", err)
	}

	poolMap := make(map[string]*BalancedChannelPool)

	for _, pr := range poolRows {
		creds := credsByPool[pr.ID]
		runtimeCreds := make([]*RuntimeCredential, 0, len(creds))

		for _, cr := range creds {
			// Decrypt the API key — is_healthy is not checked here.
			// All credentials are loaded into the routing cache.
			// is_healthy is exclusively controlled by admin action via the API.
			decryptedKey, err := sm.vault.Decrypt(cr.EncryptedKey)
			if err != nil {
				sm.logger.Error("failed to decrypt credential",
					zap.Int("credential_id", cr.ID),
					zap.Error(err),
				)
				continue
			}

			runtimeCreds = append(runtimeCreds, &RuntimeCredential{
				ID:       cr.ID,
				Provider: cr.Provider,
				APIKey:   decryptedKey,
				BaseURL:  cr.BaseURL,
				Weight:   cr.Weight,
				Prefix:   cr.Prefix,
			})
		}

		if len(runtimeCreds) == 0 {
			sm.logger.Warn("pool has no credentials",
				zap.String("model", pr.ModelPattern),
			)
			continue
		}

		pool := NewBalancedPool(pr.ModelPattern, pr.Strategy, runtimeCreds, nil)
		poolMap[pr.ModelPattern] = pool

		// Store in cache for fast lookup
		sm.cache.Set(cache.PoolKey(pr.ModelPattern), pool, int64(len(runtimeCreds)*100))
	}

	// Wire up fallback pools (second pass after all pools are created)
	for _, pr := range poolRows {
		if pr.FallbackPoolID != nil {
			// Find the fallback pool
			for _, fbRow := range poolRows {
				if fbRow.ID == *pr.FallbackPoolID {
					if primaryPool, ok := poolMap[pr.ModelPattern]; ok {
						if fbPool, ok := poolMap[fbRow.ModelPattern]; ok {
							primaryPool.FallbackPool = fbPool
						}
					}
					break
				}
			}
		}
	}

	// Cache active models list for /v1/models endpoint.
	// Store as []ActiveModel (rich objects) so ListModels can return
	// capabilities without additional DB queries.
	var activeModels []ActiveModel
	for _, pr := range poolRows {
		am := ActiveModel{Pattern: pr.ModelPattern}
		if len(pr.Capabilities) > 0 && string(pr.Capabilities) != "{}" {
			if err := json.Unmarshal(pr.Capabilities, &am.Capabilities); err != nil {
				// Non-fatal: log and continue without capabilities
				sm.logger.Warn("failed to decode model capabilities",
					zap.String("model", pr.ModelPattern),
					zap.Error(err),
				)
			}
		}
		activeModels = append(activeModels, am)
	}
	sm.cache.Set("system:active_models", activeModels, 1000)

	sm.pools.Store(&poolMap)
	sm.cache.Wait() // Ensure all cache writes are visible

	sm.logger.Info("initial routing state loaded",
		zap.Int("pool_count", len(poolMap)),
	)

	// Write active models list to Redis L2 so all cluster nodes share it.
	// Convert to cache.ActiveModelEntry slice (same shape, different package).
	redisModels := make([]cache.ActiveModelEntry, len(activeModels))
	for i, am := range activeModels {
		redisModels[i] = cache.ActiveModelEntry{
			Pattern:      am.Pattern,
			Capabilities: am.Capabilities,
		}
	}
	sm.redisCacheMgr.SetJSON(ctx, cache.KeyActiveModels, redisModels, cache.DefaultCacheTTL)
	sm.redisCacheMgr.SetJSON(ctx, cache.KeyAllPools, redisModels, cache.DefaultCacheTTL)

	// Also load all tenants into cache
	return sm.loadTenants(ctx)
}

// loadTenants loads all active tenants into cache.
func (sm *SyncManager) loadTenants(ctx context.Context) error {
	tenants, err := database.ListTenants(ctx, sm.pool)
	if err != nil {
		return fmt.Errorf("failed to load tenants: %w", err)
	}

	for _, t := range tenants {
		if t.IsActive {
			sm.cache.Set(cache.TenantKey(t.APIKey), t, 200)
		}
	}
	sm.cache.Wait()

	sm.logger.Info("tenants loaded into cache", zap.Int("count", len(tenants)))
	return nil
}

// StartListener starts the PostgreSQL LISTEN/NOTIFY watcher.
// Runs in a background goroutine and hot-reloads affected pools
// when configuration changes are detected.
//
// Gap 3 Fix: The listener goroutine is wrapped in a panic defender
// to prevent config reload errors from crashing the gateway container.
func (sm *SyncManager) StartListener() {
	go sm.debouncer()
	go func() {
		defer func() {
			if r := recover(); r != nil {
				sm.logger.Error("recovered from LISTEN/NOTIFY listener panic",
					zap.Any("panic", r),
				)
				// Restart the listener to maintain config sync
				time.Sleep(5 * time.Second)
				go sm.listenLoop()
			}
		}()
		sm.listenLoop()
	}()
	sm.logger.Info("LISTEN/NOTIFY watcher started")
}

func (sm *SyncManager) listenLoop() {
	const (
		baseBackoff  = 5 * time.Second
		maxBackoff   = 30 * time.Second
		stableUptime = 5 * time.Minute
	)

	failures := 0
	for {
		select {
		case <-sm.ctx.Done():
			return
		default:
		}

		started := time.Now()
		err := sm.listen()
		if err == nil {
			continue
		}

		uptime := time.Since(started)
		// Long-lived connections reset the backoff — we only want to back off
		// when the database stays unreachable, not over occasional recycles of
		// healthy hourly connections.
		if uptime >= stableUptime {
			failures = 0
		}

		backoff := baseBackoff << uint(failures)
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
		// ±25% jitter desynchronizes replicas so they don't reconnect in
		// lockstep after a database restart.
		backoff += time.Duration(rand.Int63n(int64(backoff/4) + 1))

		if isExpectedListenDrop(err) {
			sm.logger.Warn("LISTEN/NOTIFY connection recycled (expected on cloud platforms) — reconnecting",
				zap.Duration("uptime", uptime),
				zap.Duration("retry_in", backoff),
				zap.Error(err),
			)
		} else {
			sm.logger.Error("LISTEN/NOTIFY connection error, reconnecting",
				zap.Duration("uptime", uptime),
				zap.Duration("retry_in", backoff),
				zap.Error(err),
			)
		}

		select {
		case <-time.After(backoff):
		case <-sm.ctx.Done():
			return
		}
		failures++
	}
}

// isExpectedListenDrop classifies connection-recycling errors that cloud
// platforms (Clever Cloud, Heroku…) routinely produce on otherwise healthy
// committed connections — every ~60-minute TCP lifetime reset surfaces as an
// unexpected EOF / reset. These are noisy, not incident-worthy.
func isExpectedListenDrop(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unexpected eof") ||
		strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection timed out")
}

func (sm *SyncManager) listen() error {
	conn, err := sm.pool.Acquire(sm.ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	_, err = conn.Exec(sm.ctx, "LISTEN config_change")
	if err != nil {
		return fmt.Errorf("failed to LISTEN: %w", err)
	}

	// Keep-alive goroutine: send SELECT 1 every 30 seconds to prevent Clever Cloud
	// PostgreSQL's network firewall from killing the idle LISTEN TCP connection
	// (which caused "unexpected EOF" errors every ~60 minutes in logs).
	done := make(chan struct{})
	defer close(done)

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-sm.ctx.Done():
				return
			case <-ticker.C:
				// Ping the connection to keep it alive through cloud network firewalls.
				// Using conn.Exec() on the pgx pooled connection is safe here.
				if _, pingErr := conn.Exec(sm.ctx, "SELECT 1"); pingErr != nil {
					// Connection is broken — the outer WaitForNotification will also fail.
					// Log at debug level; the main loop will log the actual error.
					return
				}
			}
		}
	}()

	for {
		notification, err := conn.Conn().WaitForNotification(sm.ctx)
		if err != nil {
			return fmt.Errorf("notification wait failed: %w", err)
		}

		sm.logger.Info("config change notification received",
			zap.String("payload", notification.Payload),
		)

		// Parse payload: "table_name:id"
		parts := strings.SplitN(notification.Payload, ":", 2)
		if len(parts) != 2 {
			continue
		}

		tableName := parts[0]

		// Queue the notification to the debounced processor
		select {
		case sm.reloadChan <- tableName:
		default:
			// Buffer full, skip (already pending)
		}
	}
}


// debouncer aggregates configuration updates in a sliding 100ms quiet window
// to prevent notification storms when batch inserts occur (e.g. provider discovery).
func (sm *SyncManager) debouncer() {
	var (
		timer          *time.Timer
		pendingPools   bool
		pendingTenants bool
	)
	for {
		select {
		case <-sm.ctx.Done():
			return
		case tableName := <-sm.reloadChan:
			if tableName == "model_pools" || tableName == "credentials" {
				pendingPools = true
			} else if tableName == "tenants" {
				pendingTenants = true
			}
			if timer != nil {
				timer.Stop()
			}
			timer = time.NewTimer(100 * time.Millisecond)
		case <-func() <-chan time.Time {
			if timer == nil {
				return nil
			}
			return timer.C
		}():
			timer = nil
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if pendingPools {
				if err := sm.reloadPools(ctx); err != nil {
					sm.logger.Error("failed to reload pools", zap.Error(err))
				}
				pendingPools = false
			}
			if pendingTenants {
				if err := sm.loadTenants(ctx); err != nil {
					sm.logger.Error("failed to reload tenants", zap.Error(err))
				}
				pendingTenants = false
			}
			cancel()
		}
	}
}

// reloadPools performs a full reload of all routing pools.
// Uses atomic pointer swap for zero-downtime updates.
// After reloading Ristretto L1, invalidates Redis L2 so all cluster nodes
// receive fresh data on their next request.
func (sm *SyncManager) reloadPools(ctx context.Context) error {
	if err := sm.LoadInitialState(ctx); err != nil {
		return err
	}
	// Invalidate stale Redis keys and broadcast sync event to all instances.
	// LoadInitialState already wrote fresh data; this DELs the old keys so any
	// node that hasn't reloaded yet will get a cache miss and re-read.
	sm.redisCacheMgr.InvalidateAndPublish(ctx)
	sm.logger.Info("routing pools hot-reloaded and redis cache refreshed")
	return nil
}

// InvalidateRedisCache purges all gateway-level Redis cache keys and broadcasts
// a sync event so all running instances clear their local Ristretto caches.
// Called by admin handlers after any mutation that changes pool/model data.
func (sm *SyncManager) InvalidateRedisCache(ctx context.Context) {
	sm.redisCacheMgr.InvalidateAndPublish(ctx)
}

// Stop shuts down the sync manager.
func (sm *SyncManager) Stop() {
	sm.cancel()
}

// GetPool returns a routing pool by model pattern (for direct access).
func (sm *SyncManager) GetPool(model string) *BalancedChannelPool {
	pools := sm.pools.Load()
	if pools == nil {
		return nil
	}
	return (*pools)[model]
}

// GetPoolForCluster returns a BalancedChannelPool as a cluster.PenalizerPool
// interface for use by the cluster broadcaster subscriber.
// Returns nil if the pool does not exist.
func (sm *SyncManager) GetPoolForCluster(pattern string) *BalancedChannelPool {
	return sm.GetPool(pattern)
}
