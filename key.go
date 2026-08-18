package main

import (
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/crypto"
)

// loadIssuerKey loads the private key that funds the run from a file holding
// exactly 64 hex characters. Whether the derived account is actually funded is
// checked live against the chain at startup.
func loadIssuerKey(path string) (*ecdsa.PrivateKey, common.Address, error) {
	if path == "" {
		return nil, common.Address{}, fmt.Errorf("--key is required: path to a file holding the issuer private key as 64 hex characters")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, common.Address{}, fmt.Errorf("read issuer key %s: %w", path, err)
	}
	material := strings.TrimSpace(string(raw))
	if keyBytes, err := hex.DecodeString(material); err != nil || len(keyBytes) != 32 {
		return nil, common.Address{}, fmt.Errorf("%s must be exactly 64 hex characters", path)
	}
	key, err := crypto.HexToECDSA(material)
	if err != nil {
		return nil, common.Address{}, fmt.Errorf("%s is not a valid secp256k1 key: %w", path, err)
	}
	return key, crypto.PubkeyToAddress(key.PublicKey), nil
}
