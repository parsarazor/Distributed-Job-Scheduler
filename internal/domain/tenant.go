package domain

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Per-tenant dispatch limits: at most MaxInFlight attempts may be announced or
// running at once.
const (
	DefaultTenantMaxInFlight = 10
	MaxTenantMaxInFlight     = 10000
)

// Tenant represents an authenticated tenant organization.
// APIKeyHash must always hold the hex-encoded SHA-256 of the raw key, never
// the raw key itself. Use NewTenant to enforce this invariant.
type Tenant struct {
	ID         uuid.UUID `json:"id"`
	Name       string    `json:"name"`
	APIKeyHash string    `json:"-"`
	CreatedAt  time.Time `json:"created_at"`
	// MaxInFlight caps the tenant's dispatched and running attempts.
	MaxInFlight int `json:"max_in_flight"`
}

// HashAPIKey returns the hex-encoded SHA-256 of a raw API key.
func HashAPIKey(rawAPIKey string) string {
	sum := sha256.Sum256([]byte(rawAPIKey))
	return hex.EncodeToString(sum[:])
}

// NewTenant constructs a Tenant, hashing the raw API key. It refuses empty
// names and empty keys so a plaintext key can never be stored silently.
func NewTenant(name, rawAPIKey string) (*Tenant, error) {
	if strings.TrimSpace(name) == "" {
		return nil, errors.New("tenant name must not be empty")
	}
	if rawAPIKey == "" {
		return nil, errors.New("api key must not be empty")
	}
	return &Tenant{
		ID:          uuid.New(),
		Name:        name,
		APIKeyHash:  HashAPIKey(rawAPIKey),
		CreatedAt:   time.Now().UTC(),
		MaxInFlight: DefaultTenantMaxInFlight,
	}, nil
}

// InFlightLimit returns MaxInFlight, or the default when it is unset.
func (t *Tenant) InFlightLimit() int {
	if t.MaxInFlight <= 0 {
		return DefaultTenantMaxInFlight
	}
	return min(t.MaxInFlight, MaxTenantMaxInFlight)
}

// VerifyAPIKey compares a raw candidate key against the stored hash in
// constant time.
func (t *Tenant) VerifyAPIKey(rawAPIKey string) bool {
	candidate := HashAPIKey(rawAPIKey)
	if len(candidate) != len(t.APIKeyHash) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(candidate), []byte(t.APIKeyHash)) == 1
}
