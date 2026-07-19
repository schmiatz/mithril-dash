// Derives mithril's validator identity public key from its
// `validator.identity_keypair` file — the standard Solana CLI keypair JSON
// format (a 64-byte ed25519 [seed||pubkey] array), the exact same file
// mithril's own solana.PrivateKeyFromSolanaKeygenFile reads (see
// cmd/mithril/node/node.go). Read once at startup — an identity keypair
// never changes while a validator is running, so there's no need to poll
// it. Only the derived PUBLIC key ever leaves this function; the private
// key material is never logged, stored, or returned.
package collect

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"os"

	"github.com/mr-tron/base58"
)

// LoadIdentityPubkey reads path (validator.identity_keypair from mithril's
// config.toml) and returns the base58-encoded public key.
func LoadIdentityPubkey(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("no identity keypair path configured")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("reading keypair file: %w", err)
	}
	var raw []byte
	if err := json.Unmarshal(data, &raw); err != nil {
		return "", fmt.Errorf("parsing keypair JSON: %w", err)
	}
	if len(raw) != ed25519.PrivateKeySize {
		return "", fmt.Errorf("expected a %d-byte Solana keypair array, got %d bytes", ed25519.PrivateKeySize, len(raw))
	}
	pub := ed25519.PrivateKey(raw).Public().(ed25519.PublicKey)
	return base58.Encode(pub), nil
}
