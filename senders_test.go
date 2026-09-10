package main

import (
	"testing"

	"github.com/ava-labs/libevm/crypto"
)

func TestDeriveSendersDeterministicAndDistinct(t *testing.T) {
	root, _ := crypto.HexToECDSA("56289e99c94b6912bfc12adc093c9b51124f0dc54ac7a766b2bc5ccf558d8027")
	a := deriveSenders(root, 64)
	b := deriveSenders(root, 64)
	seen := map[string]bool{}
	for i := range a {
		if a[i].addr != b[i].addr {
			t.Fatalf("sender %d differs between derivations", i)
		}
		if seen[a[i].addr.Hex()] {
			t.Fatalf("sender %d repeats address %s", i, a[i].addr.Hex())
		}
		seen[a[i].addr.Hex()] = true
	}
	if a[0].addr != crypto.PubkeyToAddress(root.PublicKey) {
		t.Fatal("sender 0 must be the root key")
	}
}
