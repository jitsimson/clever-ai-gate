package credentials

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const PuterBaseURL = "https://api.puter.com/puterai/openai/v1"
var PuterModelsURL = "https://api.puter.com/puterai/chat/models/details"

type puterModelDetail struct {
	ID       string   `json:"id"`
	Provider string   `json:"provider"`
	Name     string   `json:"name"`
	Aliases  []string `json:"aliases"`
}

type puterModelsResponse struct {
	Models []puterModelDetail `json:"models"`
}

// DiscoverAndRegisterPuterModels fetches all models available via Puter,
// creates model pools for each, and binds the credential in a single transaction.
func DiscoverAndRegisterPuterModels(ctx context.Context, db *pgxpool.Pool, vault *Vault, apiKey string, weight int) (int, []string, error) {
	if apiKey == "" {
		return 0, nil, fmt.Errorf("puter auth token (api key) is required for model discovery")
	}

	if weight <= 0 {
		weight = 1
	}

	// 1. Fetch available models from Puter
	models, err := fetchPuterModels(ctx, apiKey)
	if err != nil {
		return 0, nil, err
	}

	// 2. Encrypt the API key once
	encryptedKey, err := vault.Encrypt(apiKey)
	if err != nil {
		return 0, nil, fmt.Errorf("vault encryption failed: %w", err)
	}

	// Find which pools already have this apiKey bound to avoid duplicates.
	alreadyBound := make(map[int]bool)
	rows, err := db.Query(ctx, `SELECT pool_id, encrypted_key FROM credentials WHERE provider = $1`, "puter")
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

	// 3. Open a transaction to atomically write all pools and credentials
	tx, err := db.Begin(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var discoveredModels []string

	for _, m := range models {
		if m.ID == "" {
			continue
		}

		// Register only the prefixed pattern: e.g. "puter/gpt-4o-mini"
		modelPattern := "puter/" + m.ID

		caps := ClassifyModel(modelPattern)
		capsJSON, err := json.Marshal(caps.ToMap())
		if err != nil {
			capsJSON = []byte("{}")
		}

		var poolID int
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

		if !alreadyBound[poolID] {
			_, err = tx.Exec(ctx,
				`INSERT INTO credentials (pool_id, provider, encrypted_key, base_url, weight, is_healthy)
				 VALUES ($1, 'puter', $2, $3, $4, true)`,
				poolID, encryptedKey, PuterBaseURL, weight,
			)
			if err != nil {
				return 0, nil, fmt.Errorf("failed to bind credential for pool %s: %w", modelPattern, err)
			}
			alreadyBound[poolID] = true
		}

		discoveredModels = append(discoveredModels, modelPattern)
	}

	if _, err = tx.Exec(ctx, "NOTIFY config_change, 'model_pools:reload'"); err != nil {
		return 0, nil, fmt.Errorf("failed to broadcast config change notification: %w", err)
	}

	return len(discoveredModels), discoveredModels, tx.Commit(ctx)
}

func fetchPuterModels(ctx context.Context, apiKey string) ([]puterModelDetail, error) {
	client := &http.Client{Timeout: 20 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, PuterModelsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build puter discovery request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("puter api connection failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("puter rejected the api key (unauthorized/forbidden) — verify your token at puter.com/dashboard")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("puter models endpoint returned unexpected status: %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// Try decoding as object first: {"models": [...]}
	var responseEnvelope puterModelsResponse
	if err := json.Unmarshal(bodyBytes, &responseEnvelope); err == nil && len(responseEnvelope.Models) > 0 {
		return responseEnvelope.Models, nil
	}

	// Fallback to decoding as direct array: [...]
	var directList []puterModelDetail
	if err := json.Unmarshal(bodyBytes, &directList); err != nil {
		return nil, fmt.Errorf("failed to parse puter model catalog: %w (raw response: %s)", err, string(bodyBytes))
	}

	return directList, nil
}

// DiscoverAndRegisterPuterModelsBatch processes multiple Puter.com auth tokens one
// by one: each token is validated against the Puter models endpoint, and every token
// that passes discovery is bound to all puter/* model pools — mirroring the
// custom-provider batch workflow so admins can register a stack of free Puter
// accounts for round-robin load balancing in a single request.
//
// A per-token report (masked key, success flag, model count, error) is collected so
// the admin UI can display exactly which tokens were imported and which failed.
func DiscoverAndRegisterPuterModelsBatch(
	ctx context.Context,
	db *pgxpool.Pool,
	vault *Vault,
	apiKeys []string,
	weight int,
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
		return 0, 0, 0, 0, nil, nil, fmt.Errorf("no valid Puter auth tokens provided")
	}

	totalKeys = len(cleanKeys)
	results = make([]BatchKeyDiscoveryResult, 0, totalKeys)
	discoveredMap := make(map[string]bool)

	for i, k := range cleanKeys {
		res := BatchKeyDiscoveryResult{
			Index:     i + 1,
			KeyMasked: MaskAPIKey(k),
		}

		count, models, dErr := DiscoverAndRegisterPuterModels(ctx, db, vault, k, weight)
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
