package credentials

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// OpenAIModelListResponse represents the standard OpenAI /v1/models response.
// Any provider that speaks the OpenAI API format returns this exact shape.
type OpenAIModelListResponse struct {
	Object string `json:"object"`
	Data   []struct {
		ID      string `json:"id"`
		OwnedBy string `json:"owned_by,omitempty"`
	} `json:"data"`
}

// DiscoverAndRegisterCustomModels connects to any OpenAI-compatible provider,
// validates the API key, discovers all available models, and registers them
// into the gateway's routing infrastructure.
//
// Key design: models are stored under their RAW name (no prefix). This means
// if multiple providers (OpenAI, Together, DeepInfra) host the same model
// (e.g. "meta-llama/Llama-3-8b"), they all land in the SAME pool and the
// BalancedChannelPool distributes traffic + does failover automatically.
//
// The provider label is stored in the credentials table so admins can
// distinguish between "together", "deepinfra", "vllm", etc. in the dashboard.
func DiscoverAndRegisterCustomModels(ctx context.Context, db *pgxpool.Pool, vault *Vault, apiKey, baseURL, providerLabel string, weight int, prefix string) (int, []string, error) {
	if apiKey == "" {
		return 0, nil, fmt.Errorf("api_key is required for OpenAI-compatible provider discovery")
	}

	// Normalise the base URL: trim trailing slash and /v1 suffix so we can
	// cleanly append /v1/models without doubling.
	base := strings.TrimRight(baseURL, "/")
	cleanBase := strings.TrimSuffix(base, "/v1")

	// 1. Validate the key by fetching the models list
	client := &http.Client{Timeout: 15 * time.Second}
	req, err := http.NewRequestWithContext(ctx, "GET", cleanBase+"/v1/models", nil)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to build model discovery request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("provider endpoint connection failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return 0, nil, fmt.Errorf("API key validation failed: provider returned %d — check your key", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, nil, fmt.Errorf("provider rejected request with status code: %d", resp.StatusCode)
	}

	var modelList OpenAIModelListResponse
	if err := json.NewDecoder(resp.Body).Decode(&modelList); err != nil {
		return 0, nil, fmt.Errorf("failed to parse models response — is this an OpenAI-compatible endpoint? error: %w", err)
	}

	if len(modelList.Data) == 0 {
		return 0, nil, fmt.Errorf("provider returned 0 models — verify your API key has access to models")
	}

	// Normalise the provider label for storage
	if providerLabel == "" {
		providerLabel = "custom"
	}
	providerLabel = strings.ToLower(strings.TrimSpace(providerLabel))

	// Encrypt the API key
	encryptedKey, err := vault.Encrypt(apiKey)
	if err != nil {
		return 0, nil, fmt.Errorf("vault encryption failed: %w", err)
	}

	var discoveredModels []string

	// Find which pools already have this apiKey bound to avoid duplicates.
	alreadyBound := make(map[int]bool)
	rows, err := db.Query(ctx, `SELECT pool_id, encrypted_key FROM credentials WHERE provider = $1 AND base_url = $2 AND COALESCE(prefix,'') = $3`, providerLabel, baseURL, prefix)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var poolID int
			var encKey string
			if err := rows.Scan(&poolID, &encKey); err == nil {
				decrypted, decErr := vault.Decrypt(encKey)
				if decErr == nil && decrypted == apiKey {
					alreadyBound[poolID] = true
				}
			}
		}
	}

	// 2. Open a transaction to safely register all models
	tx, err := db.Begin(ctx)
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback(ctx)

	for _, m := range modelList.Data {
		if m.ID == "" {
			continue
		}

		// Determine what pool patterns to register for this model.
		//
		// Without a prefix: store the model under its raw name only (no alias
		// needed — the name is already clean and has no provider-specific tag).
		//
		// With a prefix: register BOTH the prefixed form (e.g. "my-pvdr/gpt-4o")
		// AND the clean form (e.g. "gpt-4o"). Client tools that enforce strict
		// model name whitelists will use the clean form and bypass client-side
		// validation errors. The forwardRequest handler's cred.Prefix strip only
		// fires when the request model starts with "prefix/"; the clean alias
		// skips it — which is correct because the upstream model name in the JSON
		// body is already the clean ID the provider expects.
		trimmedPrefix := strings.TrimSpace(strings.Trim(strings.TrimSpace(prefix), "/"))

		var poolPatterns []string
		if trimmedPrefix != "" {
			pooledPattern := trimmedPrefix + "/" + m.ID
			poolPatterns = []string{pooledPattern, m.ID}
		} else {
			poolPatterns = []string{m.ID}
		}

		for _, modelPattern := range poolPatterns {
			var poolID int

			// Classify the model capabilities from its identifier
			caps := ClassifyModel(modelPattern)
			capsJSON, err := json.Marshal(caps.ToMap())
			if err != nil {
				capsJSON = []byte("{}")
			}

			// Upsert the model pool. ON CONFLICT updates capabilities so that
			// re-running discovery refreshes detected feature flags live.
			err = tx.QueryRow(ctx,
				`INSERT INTO model_pools (model_pattern, strategy, capabilities)
				 VALUES ($1, 'round-robin', $2)
				 ON CONFLICT (model_pattern) DO UPDATE
				 SET capabilities = EXCLUDED.capabilities
				 RETURNING id`,
				modelPattern, capsJSON,
			).Scan(&poolID)
			if err != nil {
				return 0, nil, fmt.Errorf("failed to upsert model pool for %s: %w", modelPattern, err)
			}

			// Bind the credential to the pool.
			if !alreadyBound[poolID] {
				_, err = tx.Exec(ctx,
					`INSERT INTO credentials (pool_id, provider, encrypted_key, base_url, weight, is_healthy, prefix)
					 VALUES ($1, $2, $3, $4, $5, true, $6)`,
					poolID, providerLabel, encryptedKey, baseURL, weight, prefix,
				)
				if err != nil {
					return 0, nil, fmt.Errorf("failed to bind credential to pool %s: %w", modelPattern, err)
				}
				alreadyBound[poolID] = true
			}

			discoveredModels = append(discoveredModels, modelPattern)
		}
	}

	// 3. Trigger instant cache reload across all gateway instances
	_, err = tx.Exec(ctx, "NOTIFY config_change, 'model_pools:reload'")
	if err != nil {
		return 0, nil, fmt.Errorf("failed to broadcast config change notification: %w", err)
	}

	return len(discoveredModels), discoveredModels, tx.Commit(ctx)
}

// MaskAPIKey returns a masked version of an API key for safe display and audit logs.
func MaskAPIKey(key string) string {
	k := strings.TrimSpace(key)
	if len(k) <= 8 {
		return "****"
	}
	return k[:4] + "..." + k[len(k)-4:]
}

// BatchKeyDiscoveryResult holds the outcome of discovering a single key in a batch.
type BatchKeyDiscoveryResult struct {
	Index         int
	KeyMasked     string
	Success       bool
	ModelsCount   int
	DiscoveredIDs []string
	Error         string
}

// DiscoverAndRegisterCustomModelsBatch processes multiple API keys against an OpenAI-compatible
// endpoint with the same base URL, provider label, weight, and prefix.
func DiscoverAndRegisterCustomModelsBatch(
	ctx context.Context,
	db *pgxpool.Pool,
	vault *Vault,
	apiKeys []string,
	baseURL, providerLabel string,
	weight int,
	prefix string,
) (totalKeys, successCount, failedCount, totalModels int, results []BatchKeyDiscoveryResult, allDiscovered []string, err error) {
	seenKeys := make(map[string]bool)
	var cleanKeys []string
	for _, raw := range apiKeys {
		k := strings.TrimSpace(raw)
		if k != "" && !seenKeys[k] {
			seenKeys[k] = true
			cleanKeys = append(cleanKeys, k)
		}
	}

	if len(cleanKeys) == 0 {
		return 0, 0, 0, 0, nil, nil, fmt.Errorf("no valid API keys provided")
	}

	totalKeys = len(cleanKeys)
	results = make([]BatchKeyDiscoveryResult, 0, totalKeys)
	discoveredMap := make(map[string]bool)

	for i, k := range cleanKeys {
		res := BatchKeyDiscoveryResult{
			Index:     i + 1,
			KeyMasked: MaskAPIKey(k),
		}

		count, models, dErr := DiscoverAndRegisterCustomModels(ctx, db, vault, k, baseURL, providerLabel, weight, prefix)
		if dErr != nil {
			res.Success = false
			res.Error = dErr.Error()
			failedCount++
		} else {
			res.Success = true
			res.ModelsCount = count
			res.DiscoveredIDs = models
			successCount++
			for _, m := range models {
				if !discoveredMap[m] {
					discoveredMap[m] = true
					allDiscovered = append(allDiscovered, m)
				}
			}
		}
		results = append(results, res)
	}

	totalModels = len(allDiscovered)
	return totalKeys, successCount, failedCount, totalModels, results, allDiscovered, nil
}

