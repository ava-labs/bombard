package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/ethclient"
	"github.com/ava-labs/libevm/rpc"
)

// sendConcPerNode is the number of sender goroutines (and thus the cap on
// concurrent keep-alive connections) per node; -conns overrides it. With
// batching, many workers split the stream into tiny batches (64 workers at 14k
// tx/s per node gave 6-tx batches), so use few workers and a longer -batchwait.
var sendConcPerNode = 64

// sendBatchWait is how long a worker collects more txs for a batch after the
// first one; -batchwait overrides it.
var sendBatchWait = time.Millisecond

const (
	// sendQueueLen is the per-node buffered queue depth. A dead or slow node
	// fills its queue and then drops, it never blocks the issuer or other
	// nodes. Resubmission and the other nodes cover the dropped sends.
	sendQueueLen = 4096

	// Healthy ingress routing (mirrors a production load-balancer health check):
	// drop an endpoint from the send rotation once it falls ingressDropBehind
	// blocks behind the furthest-ahead endpoint, and re-add it once within
	// ingressRejoinWithin. Routing ingress to a node that isn't caught up wastes
	// sends and, worse, keeps a recovering node (e.g. a wiped RPC) from ever
	// catching up. Hysteresis (drop >> rejoin) avoids flapping at the boundary.
	ingressDropBehind    = 200
	ingressRejoinWithin  = 50
	ingressCheckInterval = 3 * time.Second
	// ingressAtTipWithin gates ISSUANCE (not just rotation): bombard only sends the
	// sequential-nonce stream to an endpoint within this many blocks of the tip.
	// "Healthy" (within ingressDropBehind) is the right bar for a load balancer, but
	// fatal for sequential nonces, a tx whose nonce sits above a behind endpoint's
	// accepted frontier is an UNFILLABLE GAP: the node queues it but cannot mine it
	// until it catches up, pinning throughput at 0. This bit is what keeps a graceful
	// restore from stalling bombard: when the validator majority (and thus the active
	// RPC set) flips to the recovering site, its archive RPCs are still catching up, // so we keep issuing to the at-tip site we are restoring FROM until the recovering
	// RPCs are genuinely at tip, not merely "in rotation". Well above normal under-load
	// lag, well below a restore catch-up backlog, so it neither flaps nor sends into a gap.
	ingressAtTipWithin = 25
)

// broadcaster sends every tx to every node over HTTP. Each node drains its own
// buffered queue with a fixed pool of sender goroutines, so a down or slow node
// only backs up (and drops) its own queue. All nodes share ONE keep-alive HTTP
// transport, so connections are reused rather than recreated per send.
type broadcaster struct {
	nodes   []*nodeSender
	timeout time.Duration
	fanout  int             // nodes each tx is queued to; 0 = every eligible node
	rr      atomic.Uint64   // round-robin start for fanout
	done    <-chan struct{} // the run's ctx; a blocked broadcast gives up here at shutdown
}

type nodeSender struct {
	url     string
	rc      *rpc.Client
	client  *ethclient.Client
	queue   chan *types.Transaction
	healthy atomic.Bool // in send rotation while within ingressDropBehind of tip
	atTip   atomic.Bool // within ingressAtTipWithin of tip, safe to issue nonces to
	active  atomic.Bool // routed ingress only while on the active (validator) site
}

// newBroadcaster dials every rpcURL (lazily, over HTTP, a down node is included
// and simply errors on send) behind a single tuned transport and starts the
// per-node sender pools. sendTimeout bounds each individual send.
func newBroadcaster(ctx context.Context, rpcURLs []string, sendTimeout time.Duration) (*broadcaster, error) {
	if len(rpcURLs) == 0 {
		return nil, fmt.Errorf("broadcaster needs at least one URL")
	}

	// One shared transport. Keep-alive connections are pooled per host, bounded
	// at sendConcPerNode so we never exceed what the sender goroutines can use, // no ephemeral-port churn, no fd blowup.
	tr := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: sendTimeout, KeepAlive: 30 * time.Second}).DialContext,
		MaxConnsPerHost:     sendConcPerNode,
		MaxIdleConnsPerHost: sendConcPerNode,
		MaxIdleConns:        sendConcPerNode * len(rpcURLs),
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: sendTimeout,
	}
	httpClient := &http.Client{Transport: tr} // no client-level timeout: per-call ctx bounds each send

	b := &broadcaster{timeout: sendTimeout, done: ctx.Done()}
	for _, url := range rpcURLs {
		rc, err := rpc.DialOptions(ctx, url, rpc.WithHTTPClient(httpClient))
		if err != nil {
			// HTTP dial is lazy, so this is rare; skip a malformed URL but keep going.
			fmt.Printf("broadcaster: skipping unusable endpoint %s (%v)\n", url, err)
			continue
		}
		ns := &nodeSender{
			url:    url,
			rc:     rc,
			client: ethclient.NewClient(rc),
			queue:  make(chan *types.Transaction, sendQueueLen),
		}
		ns.healthy.Store(true)
		ns.atTip.Store(true)  // assume at-tip until monitorIngress measures otherwise
		ns.active.Store(true) // all endpoints active until an active-rpcs file narrows it
		b.nodes = append(b.nodes, ns)
		for i := 0; i < sendConcPerNode; i++ {
			go ns.run(ctx, sendTimeout)
		}
	}
	if len(b.nodes) == 0 {
		return nil, fmt.Errorf("no usable endpoints among %d", len(rpcURLs))
	}
	go b.monitorIngress(ctx)
	return b, nil
}

// sendBatch is how many txs one HTTP request carries (a JSON-RPC batch of
// eth_sendRawTransaction). 1 keeps one request per tx; set from -batch.
var sendBatch = 1

// refused counts sends the node turned away with "txpool is full". The worker
// backs off refusalPause and re-queues them, so the offered rate follows what
// the node admits instead of hammering it until the resubmit interval.
var refused atomic.Uint64

// sendErrs counts txs the node rejected for any other non-benign reason (and
// whole batches that failed); the first few messages are printed so a silent
// loss (a tx that never reaches the pool strands its sender until resubmit)
// names itself.
var sendErrs atomic.Uint64

// clientQueued reports txs issued but still sitting in this process's per-node
// send queues (not yet handed to any node); printed on STATS so "in flight" can
// be split between the client and the chain.
var clientQueued func() int

func (b *broadcaster) queued() int {
	n := 0
	for _, ns := range b.nodes {
		n += len(ns.queue)
	}
	return n
}
var sendErrSamples atomic.Uint64

func noteSendErr(err error) {
	sendErrs.Add(1)
	if sendErrSamples.Add(1) <= 5 {
		fmt.Printf("send error: %v\n", err)
	}
}

const refusalPause = 100 * time.Millisecond

func poolFull(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "full")
}

// requeue puts refused txs back on this node's queue after a pause; it gives
// up when the run ends.
func (n *nodeSender) requeue(ctx context.Context, txs []*types.Transaction) {
	refused.Add(uint64(len(txs)))
	select {
	case <-time.After(refusalPause):
	case <-ctx.Done():
		return
	}
	for _, tx := range txs {
		select {
		case n.queue <- tx:
		case <-ctx.Done():
			return
		}
	}
}

// run drains the node's queue, sending each tx with a tight per-call timeout and
// ignoring all errors (already-known, nonce races, a down node, all expected).
// With sendBatch > 1 a worker takes one tx, then whatever else is queued up to
// the batch size (waiting at most sendBatchWait), and posts them as one batch.
func (n *nodeSender) run(ctx context.Context, timeout time.Duration) {
	for {
		var first *types.Transaction
		select {
		case <-ctx.Done():
			return
		case first = <-n.queue:
		}
		if sendBatch <= 1 {
			sctx, cancel := context.WithTimeout(ctx, timeout)
			err := n.client.SendTransaction(sctx, first)
			cancel()
			if poolFull(err) {
				n.requeue(ctx, []*types.Transaction{first})
			}
			continue
		}
		batch := make([]rpc.BatchElem, 0, sendBatch)
		txs := make([]*types.Transaction, 0, sendBatch)
		add := func(tx *types.Transaction) {
			raw, err := tx.MarshalBinary()
			if err != nil {
				return
			}
			batch = append(batch, rpc.BatchElem{Method: "eth_sendRawTransaction", Args: []any{hexutil.Bytes(raw)}, Result: new(common.Hash)})
			txs = append(txs, tx)
		}
		add(first)
		wait := time.NewTimer(sendBatchWait)
	fill:
		for len(batch) < sendBatch {
			select {
			case tx := <-n.queue:
				add(tx)
			case <-wait.C:
				break fill
			case <-ctx.Done():
				wait.Stop()
				return
			}
		}
		wait.Stop()
		sctx, cancel := context.WithTimeout(ctx, timeout)
		if err := n.rc.BatchCallContext(sctx, batch); err != nil {
			// The whole batch failed (timeout, connection reset): nothing reached
			// the node. Put every tx back on the queue; dropping them here made
			// each sender wait for the 20 s resubmit behind a nonce gap ("pool gaps:
			// nonce N: no record" on the validators).
			cancel()
			sendErrs.Add(uint64(len(batch)))
			noteSendErr(err)
			n.requeue(ctx, txs)
			continue
		}
		cancel()
		var again []*types.Transaction
		for i, el := range batch {
			switch {
			case el.Error == nil:
			case poolFull(el.Error):
				again = append(again, txs[i])
			case !benignSendErr(el.Error):
				noteSendErr(el.Error)
			}
		}
		if len(again) > 0 {
			n.requeue(ctx, again)
		}
	}
}

// broadcast enqueues signed to every eligible node. A full queue (that node is
// down or lagging) is skipped as long as another node takes the tx; when every
// eligible queue is full the call blocks, so the offered rate can never drop a
// tx and strand its sender behind a nonce gap.
func (b *broadcaster) broadcast(signed *types.Transaction) {
	// Pick the send pool in priority order. Sequential-nonce issuance must land on an
	// endpoint AT THE TIP: a tx whose nonce is above a behind endpoint's accepted
	// frontier is an unfillable gap that pins throughput at 0 (see ingressAtTipWithin).
	//   1. active + at-tip, steady state, and the end state of a completed migration.
	//   2. any at-tip, covers the restore window: the active set has flipped to the
	//                        recovering site whose RPCs aren't caught up yet, so keep
	//                        issuing to the at-tip site we are restoring FROM.
	//   3. any healthy, nothing is fully at tip but something is in rotation; a brief
	//                        small-gap hop beats a stall.
	//   4. everything, nothing looks caught up at all; spray all so ingress never
	//                        hard-stops, and the resubmit loop retries as nodes recover.
	activeAtTip, anyAtTip, anyHealthy := false, false, false
	for _, n := range b.nodes {
		if n.atTip.Load() {
			anyAtTip = true
			if n.active.Load() {
				activeAtTip = true
			}
		}
		if n.healthy.Load() {
			anyHealthy = true
		}
	}
	eligible := func(n *nodeSender) bool {
		switch {
		case activeAtTip:
			return n.active.Load() && n.atTip.Load()
		case anyAtTip:
			return n.atTip.Load()
		case anyHealthy:
			return n.healthy.Load()
		default:
			return true
		}
	}
	delivered := 0
	var first *nodeSender
	start := int(b.rr.Add(1) % uint64(len(b.nodes)))
	for i := range b.nodes {
		n := b.nodes[(start+i)%len(b.nodes)]
		if !eligible(n) {
			continue
		}
		if b.fanout > 0 && delivered >= b.fanout {
			break
		}
		if first == nil {
			first = n
		}
		select {
		case n.queue <- signed:
			delivered++
		default:
			// This node is saturated or down; skip it as long as another one
			// takes the tx.
		}
	}
	if delivered == 0 && first != nil {
		// Every eligible node is saturated: the offered rate is above what the
		// nodes admit. Block here so the issuer feels the backpressure instead
		// of losing the tx, which would leave this sender behind a nonce gap.
		select {
		case first.queue <- signed:
		case <-b.done:
		}
	}
}

// monitorIngress polls every endpoint's height and routes ingress only to those
// caught up to the tip, mirroring a load-balancer health check. A node that has
// fallen behind (e.g. a freshly-wiped RPC rejoining mid-load) is taken out of the
// send rotation so it can catch up without also serving load, then re-added once
// within range. The furthest-ahead node is behind=0, so at least one endpoint is
// always healthy.
func (b *broadcaster) monitorIngress(ctx context.Context) {
	t := time.NewTicker(ingressCheckInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			heights := make([]uint64, len(b.nodes))
			var maxH uint64
			for i, n := range b.nodes {
				hctx, cancel := context.WithTimeout(ctx, 4*time.Second)
				h, err := n.client.BlockNumber(hctx)
				cancel()
				if err == nil {
					heights[i] = h
					if h > maxH {
						maxH = h
					}
				}
			}
			if maxH == 0 {
				continue // nothing reachable yet, leave routing unchanged
			}
			for i, n := range b.nodes {
				behind := maxH - heights[i]
				switch {
				case n.healthy.Load() && behind > ingressDropBehind:
					n.healthy.Store(false)
					fmt.Fprintf(os.Stderr, "ingress: %s out of rotation, %d blocks behind tip (catching up)\n", n.url, behind)
				case !n.healthy.Load() && behind <= ingressRejoinWithin:
					n.healthy.Store(true)
					fmt.Fprintf(os.Stderr, "ingress: %s back in rotation, caught up (%d behind)\n", n.url, behind)
				}
				// atTip is the stricter issuance gate (see ingressAtTipWithin). No
				// hysteresis: a few blocks of lag is normal under load, so flapping
				// across this boundary is harmless (it just shifts issuance between
				// equally-at-tip endpoints), and a restore backlog is far past it.
				atTip := behind <= ingressAtTipWithin
				if n.atTip.Swap(atTip) != atTip && !atTip {
					fmt.Fprintf(os.Stderr, "ingress: %s no longer at tip, %d behind (holding issuance off it)\n", n.url, behind)
				}
			}
		}
	}
}

func (b *broadcaster) Close() {
	for _, n := range b.nodes {
		n.client.Close()
	}
}

// httpRPCToWS converts a subnet-evm HTTP RPC URL into its WebSocket equivalent:
//
//	http://host:port/ext/bc/<id>/rpc  ->  ws://host:port/ext/bc/<id>/ws
//	https://...                       ->  wss://...
func httpRPCToWS(rpcURL string) string {
	u := rpcURL
	switch {
	case strings.HasPrefix(u, "https://"):
		u = "wss://" + strings.TrimPrefix(u, "https://")
	case strings.HasPrefix(u, "http://"):
		u = "ws://" + strings.TrimPrefix(u, "http://")
	}
	if pre, ok := strings.CutSuffix(u, "/rpc"); ok {
		u = pre + "/ws"
	}
	return u
}

// dialRPC connects over websocket and falls back to plain HTTP on the /rpc
// path for nodes that serve no /ws endpoint; block watching polls either way.
func dialRPC(ctx context.Context, wsURL string) (*rpc.Client, error) {
	if c, err := rpc.DialWebsocket(ctx, wsURL, ""); err == nil {
		return c, nil
	}
	u := wsURL
	switch {
	case strings.HasPrefix(u, "wss://"):
		u = "https://" + strings.TrimPrefix(u, "wss://")
	case strings.HasPrefix(u, "ws://"):
		u = "http://" + strings.TrimPrefix(u, "ws://")
	}
	if pre, ok := strings.CutSuffix(u, "/ws"); ok {
		u = pre + "/rpc"
	}
	return rpc.DialContext(ctx, u)
}
