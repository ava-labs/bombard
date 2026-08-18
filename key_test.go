package main

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/ava-labs/libevm/crypto"
)

func TestLoadIssuerKey(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	keyHex := hex.EncodeToString(crypto.FromECDSA(key))
	path := filepath.Join(t.TempDir(), "issuer.key")
	if err := os.WriteFile(path, []byte(keyHex+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	loaded, loadedAddress, err := loadIssuerKey(path)
	if err != nil {
		t.Fatalf("loadIssuerKey: %v", err)
	}
	if loadedAddress != crypto.PubkeyToAddress(key.PublicKey) {
		t.Fatalf("loadIssuerKey address = %s, want %s", loadedAddress, crypto.PubkeyToAddress(key.PublicKey))
	}
	if hex.EncodeToString(crypto.FromECDSA(loaded)) != keyHex {
		t.Fatal("loadIssuerKey returned a different key")
	}
}

func TestLoadIssuerKeyRejectsMalformedKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "issuer.key")
	if err := os.WriteFile(path, []byte("not-hex"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadIssuerKey(path); err == nil {
		t.Fatal("expected an error for a malformed key file")
	}
}

func TestLoadIssuerKeyRequiresPath(t *testing.T) {
	if _, _, err := loadIssuerKey(""); err == nil {
		t.Fatal("expected an error for an empty key path")
	}
}
