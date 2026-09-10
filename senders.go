package main

import (
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/ethclient"
)

// sender is one issuing account. Sender 0 is the root key from -key; the rest
// are derived from it, so a rerun with the same key and -senders finds the
// same funded accounts.
type sender struct {
	key       *ecdsa.PrivateKey
	addr      common.Address
	nextNonce uint64 // owned by the issuer goroutine
}

// txKey identifies an in-flight tx: which sender, which nonce.
type txKey struct {
	sender uint32
	nonce  uint64
}

// deriveSenders returns n senders: the root first, then keccak(root || i) for
// i >= 1, rehashed in the astronomically rare case the digest is not a valid
// scalar.
func deriveSenders(root *ecdsa.PrivateKey, n int) []*sender {
	out := make([]*sender, 0, n)
	out = append(out, &sender{key: root, addr: crypto.PubkeyToAddress(root.PublicKey)})
	seed := crypto.FromECDSA(root)
	for i := 1; i < n; i++ {
		var idx [4]byte
		binary.BigEndian.PutUint32(idx[:], uint32(i))
		d := crypto.Keccak256(seed, idx[:])
		key, err := crypto.ToECDSA(d)
		for err != nil {
			d = crypto.Keccak256(d)
			key, err = crypto.ToECDSA(d)
		}
		out = append(out, &sender{key: key, addr: crypto.PubkeyToAddress(key.PublicKey)})
	}
	return out
}

// fundSenders tops up every derived sender whose balance is under half of
// `amount` with `amount` from the root, then waits until every transfer is
// visible. Returns how many were funded.
func fundSenders(ctx context.Context, client *ethclient.Client, bc *broadcaster, senders []*sender, signer types.Signer, amount *big.Int) (int, error) {
	half := new(big.Int).Rsh(amount, 1)
	need := make([]*sender, 0, len(senders))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 32)
	var firstErr error
	for _, s := range senders[1:] {
		wg.Add(1)
		sem <- struct{}{}
		go func(s *sender) {
			defer wg.Done()
			defer func() { <-sem }()
			bal, err := client.BalanceAt(ctx, s.addr, nil)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			if bal.Cmp(half) < 0 {
				need = append(need, s)
			}
		}(s)
	}
	wg.Wait()
	if firstErr != nil {
		return 0, fmt.Errorf("read sender balances: %w", firstErr)
	}
	if len(need) == 0 {
		return 0, nil
	}
	root := senders[0]
	nonce, err := client.NonceAt(ctx, root.addr, nil)
	if err != nil {
		return 0, err
	}
	total := new(big.Int).Mul(amount, big.NewInt(int64(len(need))))
	if bal, err := client.BalanceAt(ctx, root.addr, nil); err == nil && bal.Cmp(total) < 0 {
		return 0, fmt.Errorf("root %s holds %s wei, funding %d senders needs %s", root.addr.Hex(), bal, len(need), total)
	}
	for _, s := range need {
		tx := types.NewTransaction(nonce, s.addr, amount, gasLimitNative, gasPrice(), nil)
		signed, err := types.SignTx(tx, signer, root.key)
		if err != nil {
			return 0, err
		}
		bc.broadcast(signed)
		nonce++
	}
	// Wait for the root's nonce to reach the last funding tx; re-broadcast
	// nothing, funding txs are cheap to re-run on the next start.
	deadline := time.Now().Add(2 * time.Minute)
	for {
		n, err := client.NonceAt(ctx, root.addr, nil)
		if err == nil && n >= nonce {
			return len(need), nil
		}
		if time.Now().After(deadline) {
			return len(need), fmt.Errorf("funding did not land within 2 minutes (root nonce %d, want %d)", n, nonce)
		}
		select {
		case <-ctx.Done():
			return len(need), ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// loadStartNonces sets every sender's next nonce to its accepted nonce, the
// max across the nodes (see the note on NonceAt(latest) in main).
func loadStartNonces(ctx context.Context, bc *broadcaster, senders []*sender, timeout time.Duration) error {
	var wg sync.WaitGroup
	sem := make(chan struct{}, 32)
	var mu sync.Mutex
	failed := 0
	for _, s := range senders {
		wg.Add(1)
		sem <- struct{}{}
		go func(s *sender) {
			defer wg.Done()
			defer func() { <-sem }()
			n, ok := maxAcceptedNonce(ctx, bc, s.addr)
			if !ok {
				mu.Lock()
				failed++
				mu.Unlock()
				return
			}
			s.nextNonce = n
		}(s)
	}
	wg.Wait()
	if failed > 0 {
		return fmt.Errorf("could not read the nonce of %d sender(s) from any node", failed)
	}
	return nil
}
