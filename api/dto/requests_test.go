package dto

import (
	"encoding/json"
	"math"
	"testing"
)

func TestCreateTenantRequest_LargeNumbers(t *testing.T) {
	tests := []struct {
		name         string
		jsonInput    string
		wantName     string
		wantBalance  int64
		wantRPM      int
	}{
		{
			name:        "massive token balance (10^20) and massive RPM (19 nines)",
			jsonInput:   `{"name":"Acme","token_balance":100000000000000000000,"rate_limit_rpm":9999999999999999999}`,
			wantName:    "Acme",
			wantBalance: math.MaxInt64,
			wantRPM:     math.MaxInt32,
		},
		{
			name:        "19 nines as numbers",
			jsonInput:   `{"name":"Nineteen Nines","token_balance":9999999999999999999,"rate_limit_rpm":9999999999999999999}`,
			wantName:    "Nineteen Nines",
			wantBalance: math.MaxInt64,
			wantRPM:     math.MaxInt32,
		},
		{
			name:        "19 nines as strings",
			jsonInput:   `{"name":"String Nines","token_balance":"9999999999999999999","rate_limit_rpm":"9999999999999999999"}`,
			wantName:    "String Nines",
			wantBalance: math.MaxInt64,
			wantRPM:     math.MaxInt32,
		},
		{
			name:        "unlimited keyword strings",
			jsonInput:   `{"name":"Unlimited Client","token_balance":"unlimited","rate_limit_rpm":"unlimited"}`,
			wantName:    "Unlimited Client",
			wantBalance: math.MaxInt64,
			wantRPM:     math.MaxInt32,
		},
		{
			name:        "scientific notation",
			jsonInput:   `{"name":"Sci Notation","token_balance":1e21,"rate_limit_rpm":1e10}`,
			wantName:    "Sci Notation",
			wantBalance: math.MaxInt64,
			wantRPM:     math.MaxInt32,
		},
		{
			name:        "standard normal values",
			jsonInput:   `{"name":"Standard","token_balance":5000000,"rate_limit_rpm":120}`,
			wantName:    "Standard",
			wantBalance: 5000000,
			wantRPM:     120,
		},
		{
			name:        "omitted values default to 0 for handler default assignment",
			jsonInput:   `{"name":"Default"}`,
			wantName:    "Default",
			wantBalance: 0,
			wantRPM:     0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var req CreateTenantRequest
			err := json.Unmarshal([]byte(tc.jsonInput), &req)
			if err != nil {
				t.Fatalf("json.Unmarshal failed: %v", err)
			}
			if req.Name != tc.wantName {
				t.Errorf("expected Name %q, got %q", tc.wantName, req.Name)
			}
			if req.TokenBalance != tc.wantBalance {
				t.Errorf("expected TokenBalance %d, got %d", tc.wantBalance, req.TokenBalance)
			}
			if req.RateLimitRPM != tc.wantRPM {
				t.Errorf("expected RateLimitRPM %d, got %d", tc.wantRPM, req.RateLimitRPM)
			}
		})
	}
}

func TestUpdateTenantRequest_LargeNumbers(t *testing.T) {
	jsonInput := `{"name":"Updated Corp","token_balance":100000000000000000000,"rate_limit_rpm":9999999999999999999,"is_active":false}`
	var req UpdateTenantRequest
	err := json.Unmarshal([]byte(jsonInput), &req)
	if err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if req.Name != "Updated Corp" {
		t.Errorf("expected Name 'Updated Corp', got %q", req.Name)
	}
	if req.TokenBalance != math.MaxInt64 {
		t.Errorf("expected TokenBalance %d, got %d", int64(math.MaxInt64), req.TokenBalance)
	}
	if req.RateLimitRPM != math.MaxInt32 {
		t.Errorf("expected RateLimitRPM %d, got %d", math.MaxInt32, req.RateLimitRPM)
	}
	if req.IsActive != false {
		t.Errorf("expected IsActive false, got true")
	}
}
