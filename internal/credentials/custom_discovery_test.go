package credentials

import (
	"testing"
)

func TestMaskAPIKey(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", "****"},
		{"1234", "****"},
		{"12345678", "****"},
		{"sk-1234567890abcdef", "sk-1...cdef"},
		{"  sk-together-1234567890  ", "sk-t...7890"},
	}

	for _, tt := range tests {
		got := MaskAPIKey(tt.input)
		if got != tt.expected {
			t.Errorf("MaskAPIKey(%q) = %q; want %q", tt.input, got, tt.expected)
		}
	}
}
