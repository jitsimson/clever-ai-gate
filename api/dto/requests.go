package dto

import (
	"encoding/json"
	"math"
	"math/big"
	"strconv"
	"strings"
)

// --- Request DTOs ---

// CreateTenantRequest represents the body for creating a tenant.
type CreateTenantRequest struct {
	Name         string `json:"name" binding:"required" example:"Acme Corp"`
	TokenBalance int64  `json:"token_balance,omitempty" example:"1000000000"`
	RateLimitRPM int    `json:"rate_limit_rpm,omitempty" example:"60"`
}

// UnmarshalJSON implements custom JSON unmarshaling to gracefully handle
// arbitrarily large numbers (e.g. 100000000000000000000, 9999999999999999999),
// scientific notation, and string numbers, clamping values to math.MaxInt64 and math.MaxInt32.
func (r *CreateTenantRequest) UnmarshalJSON(data []byte) error {
	var raw struct {
		Name         string          `json:"name"`
		TokenBalance json.RawMessage `json:"token_balance"`
		RateLimitRPM json.RawMessage `json:"rate_limit_rpm"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	r.Name = raw.Name
	r.TokenBalance = parseClampedInt64(raw.TokenBalance, 0, math.MaxInt64)
	r.RateLimitRPM = parseClampedInt(raw.RateLimitRPM, 0, math.MaxInt32)
	return nil
}

// UpdateTenantRequest represents the body for updating a tenant.
type UpdateTenantRequest struct {
	Name         string `json:"name" binding:"required" example:"Acme Corp Updated"`
	TokenBalance int64  `json:"token_balance" example:"2000000000"`
	IsActive     bool   `json:"is_active" example:"true"`
	RateLimitRPM int    `json:"rate_limit_rpm" example:"120"`
}

// UnmarshalJSON implements custom JSON unmarshaling for updating a tenant,
// safely clamping values that exceed int64 or int32 bounds.
func (r *UpdateTenantRequest) UnmarshalJSON(data []byte) error {
	var raw struct {
		Name         string          `json:"name"`
		TokenBalance json.RawMessage `json:"token_balance"`
		IsActive     *bool           `json:"is_active"`
		RateLimitRPM json.RawMessage `json:"rate_limit_rpm"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	r.Name = raw.Name
	if raw.IsActive != nil {
		r.IsActive = *raw.IsActive
	} else {
		r.IsActive = true
	}
	r.TokenBalance = parseClampedInt64(raw.TokenBalance, 0, math.MaxInt64)
	r.RateLimitRPM = parseClampedInt(raw.RateLimitRPM, 0, math.MaxInt32)
	return nil
}

// parseClampedInt64 parses a json.RawMessage (number or string) and clamps it to [0, maxVal].
func parseClampedInt64(raw json.RawMessage, defaultVal, maxVal int64) int64 {
	if len(raw) == 0 {
		return defaultVal
	}
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return defaultVal
	}
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = strings.TrimSpace(s[1 : len(s)-1])
	}
	if s == "" || s == "null" {
		return defaultVal
	}
	lower := strings.ToLower(s)
	if lower == "unlimited" || lower == "infinity" || lower == "inf" || lower == "max" {
		return maxVal
	}

	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		if v < 0 {
			return 0
		}
		if v > maxVal {
			return maxVal
		}
		return v
	}

	bf, _, err := new(big.Float).Parse(s, 10)
	if err == nil {
		if bf.Sign() <= 0 {
			return 0
		}
		maxBf := new(big.Float).SetInt64(maxVal)
		if bf.Cmp(maxBf) >= 0 {
			return maxVal
		}
		val, _ := bf.Int64()
		return val
	}

	return defaultVal
}

// parseClampedInt parses a json.RawMessage (number or string) and clamps it to [0, maxVal].
func parseClampedInt(raw json.RawMessage, defaultVal, maxVal int) int {
	if len(raw) == 0 {
		return defaultVal
	}
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return defaultVal
	}
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = strings.TrimSpace(s[1 : len(s)-1])
	}
	if s == "" || s == "null" {
		return defaultVal
	}
	lower := strings.ToLower(s)
	if lower == "unlimited" || lower == "infinity" || lower == "inf" || lower == "max" {
		return maxVal
	}

	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		if v < 0 {
			return 0
		}
		if v > int64(maxVal) {
			return maxVal
		}
		return int(v)
	}

	bf, _, err := new(big.Float).Parse(s, 10)
	if err == nil {
		if bf.Sign() <= 0 {
			return 0
		}
		maxBf := new(big.Float).SetInt64(int64(maxVal))
		if bf.Cmp(maxBf) >= 0 {
			return maxVal
		}
		val, _ := bf.Int64()
		return int(val)
	}

	return defaultVal
}

// CreatePoolRequest represents the body for creating a model routing pool.
type CreatePoolRequest struct {
	ModelPattern   string `json:"model_pattern" binding:"required" example:"gpt-4o"`
	Strategy       string `json:"strategy,omitempty" example:"round-robin"`
	FallbackPoolID *int   `json:"fallback_pool_id,omitempty" example:"2"`
}

// UpdatePoolRequest represents the body for updating a model routing pool.
type UpdatePoolRequest struct {
	ModelPattern   string `json:"model_pattern" binding:"required" example:"gpt-4o"`
	Strategy       string `json:"strategy" example:"weighted-round-robin"`
	FallbackPoolID *int   `json:"fallback_pool_id,omitempty"`
}

// CreateCredentialRequest represents the body for creating a provider credential.
type CreateCredentialRequest struct {
	PoolID   int    `json:"pool_id" binding:"required" example:"1"`
	Provider string `json:"provider" binding:"required" example:"openai"`
	APIKey   string `json:"api_key" binding:"required" example:"sk-..."`
	BaseURL  string `json:"base_url" binding:"required" example:"https://api.openai.com"`
	Weight   int    `json:"weight,omitempty" example:"1"`
	Prefix   string `json:"prefix,omitempty" example:"exampleprefix"`
}

// UpdateCredentialRequest represents the body for updating a provider credential.
type UpdateCredentialRequest struct {
	Provider  string `json:"provider" binding:"required" example:"openai"`
	APIKey    string `json:"api_key,omitempty" example:"sk-..."`
	BaseURL   string `json:"base_url" binding:"required" example:"https://api.openai.com"`
	Weight    int    `json:"weight" example:"1"`
	IsHealthy bool   `json:"is_healthy" example:"true"`
	Prefix    string `json:"prefix,omitempty" example:"exampleprefix"`
}

type DiscoverProviderRequest struct {
	Provider string   `json:"provider" binding:"required" example:"nvidia"`
	APIKey   string   `json:"api_key,omitempty" example:"nvapi-..."`
	APIKeys  []string `json:"api_keys,omitempty"`
	BaseURL  string   `json:"base_url" binding:"required" example:"https://integrate.api.nvidia.com/v1"`
	Weight   int      `json:"weight" example:"1"`
	Label    string   `json:"label,omitempty" example:"together-ai"`
	Prefix   string   `json:"prefix,omitempty" example:"exampleprefix"`
}

type BatchKeyResult struct {
	Index         int      `json:"index"`
	KeyMasked     string   `json:"key_masked"`
	Success       bool     `json:"success"`
	ModelsCount   int      `json:"models_count,omitempty"`
	DiscoveredIDs []string `json:"discovered_models,omitempty"`
	Error         string   `json:"error,omitempty"`
}

type DiscoverProviderResponse struct {
	Message       string           `json:"message" example:"Successfully synchronized provider models"`
	ModelsCount   int              `json:"models_count" example:"45"`
	DiscoveredIDs []string         `json:"discovered_models"`
	TotalKeys     int              `json:"total_keys,omitempty"`
	SuccessCount  int              `json:"success_count,omitempty"`
	FailedCount   int              `json:"failed_count,omitempty"`
	Results       []BatchKeyResult `json:"results,omitempty"`
}

// DiscoverCloudflareRequest is the request body for POST /api/v1/admin/providers/cloudflare.
// Cloudflare Workers AI requires both an Account ID and an API Token, which are distinct
// credentials — using a dedicated DTO avoids misuse of the generic api_key/base_url fields.
type DiscoverCloudflareRequest struct {
	AccountID string `json:"account_id" binding:"required" example:"a1b2c3d4e5f6..."`
	APIToken  string `json:"api_token"  binding:"required" example:"..."`
	Weight    int    `json:"weight,omitempty" example:"1"`
}

// DiscoverSarvamRequest is the request body for POST /api/v1/admin/providers/sarvam.
// Sarvam AI only needs an API subscription key — the base URL is hardcoded to
// https://api.sarvam.ai. A dedicated minimal DTO keeps the "add provider" modal
// contract to just the API key (plus an optional weight).
type DiscoverSarvamRequest struct {
	APIKey string `json:"api_key" binding:"required" example:"sk_..."`
	Weight int    `json:"weight,omitempty" example:"1"`
}

// DiscoverPuterRequest is the request body for POST /api/v1/admin/providers/puter.
// Puter.com only needs an API token (api_key) — the base URL is hardcoded to
// https://api.puter.com/puterai/openai/v1.
type DiscoverPuterRequest struct {
	APIKey string `json:"api_key" binding:"required" example:"..."`
	Weight int    `json:"weight,omitempty" example:"1"`
}

// DiscoverAgentRouterRequest is the request body for POST /api/v1/admin/providers/agentrouter.
// AgentRouter only needs an API key — the base URL is hardcoded to
// https://ps.air-outer.com/v1. Multiple keys can be added for round-robin rotation
// with automatic 429/401 failover.
type DiscoverAgentRouterRequest struct {
	APIKey string `json:"api_key" binding:"required" example:"sk-..."`
	Weight int    `json:"weight,omitempty" example:"1"`
}

// DiscoverZenMuxRequest is the request body for POST /api/v1/admin/providers/zenmux.
// ZenMux only needs an API key — the base URL is hardcoded to https://zenmux.ai/api/v1.
type DiscoverZenMuxRequest struct {
	APIKey string `json:"api_key" binding:"required" example:"sk-..."`
	Weight int    `json:"weight,omitempty" example:"1"`
}

// DiscoverGeminiRequest is the request body for POST /api/v1/admin/providers/gemini.
// Google AI Studio only needs an API key — the base URL is hardcoded to
// https://generativelanguage.googleapis.com. A dedicated minimal DTO keeps the
// "add provider" modal contract to just the API key (plus an optional weight).
type DiscoverGeminiRequest struct {
	APIKey string `json:"api_key" binding:"required" example:"AIzaSy..."`
	Weight int    `json:"weight,omitempty" example:"1"`
}
// BulkDeleteRequest represents a request containing multiple IDs to be deleted.
type BulkDeleteRequest struct {
	IDs []int `json:"ids" binding:"required,min=1" example:"[1,2,3]"`
}

// BulkActivateRequest represents a request containing multiple pool IDs to activate.
type BulkActivateRequest struct {
	IDs []int `json:"ids" binding:"required,min=1" example:"[1,2,3]"`
}


