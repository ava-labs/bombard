# bombard

Single-issuer EVM load generator for Avalanche L1s. One funded key, one
strictly increasing nonce, a production-shaped workload: a single gateway
issuing native transfers in order at a target rate.

Extracted from `avalanche-benchmark`, where it lived as `cmd/bombard`. It is
deployment-agnostic now: no inventory files, no discovery. You tell it where
to send and what key to use.

## Run

```sh
go run . \
  -rpc http://<host>:9650/ext/bc/<CHAIN_ID>/rpc,http://<host2>:9650/ext/bc/<CHAIN_ID>/rpc \
  -key path/to/issuer.key \
  -rps 1000 -duration 60s
```

- `-rpc` (required): comma-separated RPC URLs. Sends fan out across all of
  them; block watchers race across all of them. Endpoints that fall behind the
  tip are dropped from the send rotation and rejoin when they catch up.
- `-key` (required): path to a file holding the issuer private key as exactly
  64 hex characters. The account must be funded; bombard checks the balance at
  startup and refuses to run an all-reject workload.

Everything else has defaults: `-rps`, `-txs`, `-duration`, `-inflight`,
`-resubmit`, `-overshoot`, `-sendtimeout`, `-tui`, `-scrape`, `-sample`.
See `go run . -h`.

## Design

Two governors and no timeouts:

1. A rolling token bucket paces issuance at `rps` (plus `-overshoot`), with
   carry-over bounded to one second so a stall never triggers an unbounded
   catch-up flood.
2. An in-flight cap: the issuer never gets more than `-inflight` nonces ahead
   of the last mined nonce. Hitting the cap is the backpressure signal.

A transaction leaves the system only when its nonce mines. Anything still in
flight after `-resubmit` is re-sent verbatim (same bytes, same hash), so
mempool loss from node crashes or failover is survived without gaps.
