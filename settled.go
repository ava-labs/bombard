package main

// SAE (streaming asynchronous execution) chains accept blocks before executing
// them; the executed frontier is the SETTLED head, k blocks behind. -settled
// polls edb_settledNumber on the first endpoint and reports settled tx/s and the
// settlement lag next to minedTps, so a lag that grows instead of staying
// bounded is visible in the same STATS line.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/libevm/rpc"
)

var settled = struct {
	mu       sync.Mutex
	txsAt    map[uint64]int // block number -> tx count, from the watchers
	settledN uint64         // last settled height seen
	txs      uint64         // txs in blocks <= settledN
	enabled  atomic.Bool
	lagBlk   atomic.Uint64
	accepted atomic.Uint64
}{txsAt: make(map[uint64]int)}

// noteBlockTxs records a block's tx count so settled txs can be summed when the
// settled head passes it.
func noteBlockTxs(num uint64, txCount int) {
	if !settled.enabled.Load() {
		return
	}
	settled.mu.Lock()
	if num <= settled.settledN {
		// The settled head already passed this block before the watcher saw it:
		// count it now instead of leaving it uncounted forever.
		settled.txs += uint64(txCount)
	} else {
		settled.txsAt[num] = txCount
	}
	settled.mu.Unlock()
}

// settledLoop polls edb_settledNumber and advances the settled tx counter.
func settledLoop(ctx context.Context, rc *rpc.Client, every time.Duration) {
	settled.enabled.Store(true)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		var r struct {
			Settled  hexOrUint `json:"settled"`
			Accepted hexOrUint `json:"accepted"`
			Lag      hexOrUint `json:"lag"`
		}
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := rc.CallContext(cctx, &r, "edb_settledNumber")
		cancel()
		if err != nil {
			continue
		}
		settled.lagBlk.Store(uint64(r.Lag))
		settled.accepted.Store(uint64(r.Accepted))
		settled.mu.Lock()
		for n := settled.settledN + 1; n <= uint64(r.Settled); n++ {
			if c, ok := settled.txsAt[n]; ok {
				settled.txs += uint64(c)
				delete(settled.txsAt, n)
			}
		}
		if uint64(r.Settled) > settled.settledN {
			settled.settledN = uint64(r.Settled)
		}
		settled.mu.Unlock()
	}
}

var settledPrev struct {
	txs uint64
	at  time.Time
}

// settledStats renders " settledTps=N lag=B" for the STATS line, per second since the last call.
func settledStats() string {
	txs, _, lag := settledSnapshot()
	now := time.Now()
	tps := 0.0
	if !settledPrev.at.IsZero() {
		if dt := now.Sub(settledPrev.at).Seconds(); dt > 0 {
			tps = float64(txs-settledPrev.txs) / dt
		}
	}
	settledPrev.txs, settledPrev.at = txs, now
	return fmt.Sprintf(" settledTps=%.0f lag=%d", tps, lag)
}

// settledSnapshot returns settled txs so far, the settled head and the lag in blocks.
func settledSnapshot() (txs, head, lag uint64) {
	settled.mu.Lock()
	txs, head = settled.txs, settled.settledN
	settled.mu.Unlock()
	return txs, head, settled.lagBlk.Load()
}

// hexOrUint decodes a JSON number given either as 0x-hex string or as a number.
type hexOrUint uint64

func (h *hexOrUint) UnmarshalJSON(b []byte) error {
	s := string(b)
	if len(s) >= 2 && s[0] == '"' {
		s = s[1 : len(s)-1]
	}
	*h = hexOrUint(hexToUint64(s))
	return nil
}
