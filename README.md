# bombard

Load generator for Avalanche L1s.

Generic EVM load tools assume block times of a second or more. Avalanche
produces subsecond blocks, so a load tool has to read Avalanche's own block
fields (`timestampMilliseconds`) to measure block times and latency at all,
and it has to scrape the node's Prometheus metrics (`/ext/metrics`) to see
consensus, proposer, and txpool behavior under load instead of just counting
receipts. bombard does both, with a live terminal UI.

```text
Bombard  target=100 rps  elapsed=42s  status=at-cap

 45 ┤                                                             ╭╮  ╭╮
 42 ┤                                                 ╭╮          ││  ││     ╭
 38 ┤                                                 ││        ╭╮││  ││     │
 35 ┤                                         ╭╮      ││  ╭╮    ││││  ││    ╭╯
 31 ┤                                ╭╮       ││  ╭─╮ ││ ╭╯│    ││││  ││╭╮  │
 28 ┤                                ││   ╭╮  ││  │ │ ││ │ │    │││╰╮ ││││ ╭╯
 24 ┤                                ││  ╭╯│  ││  │ │ ││ │ │╭╮  │││ │ ││││ │
 21 ┤                                ││╭╮│ │ ╭╯│╭╮│ │╭╯│ │ │││  │││ │ ││││╭╯
 17 ┤                                │╰╯╰╯ ╰─╯ ╰╯╰╯ ╰╯ │ │ ││╰──╯││ ╰─╯││││
 14 ┤                                │                 ╰─╯ ╰╯    ││    ╰╯╰╯
 10 ┤                               ╭╯                           ╰╯
  7 ┤                               │
  3 ┤                               │
  0 ┼───────────────────────────────╯
                           mined transactions per second

issued=1312  mined=1012  inflight=300/300  resubmits=7044  minedTps=42/100
p50=11.39s  p95=11.66s  samples=42
```

## Run

```sh
go run . \
  -rpc http://<host>:9650/ext/bc/<CHAIN_ID>/rpc,http://<host2>:9650/ext/bc/<CHAIN_ID>/rpc \
  -key issuer.key \
  -rps 1000 -duration 60s
```

- `-rpc` (required): comma-separated RPC endpoints. Sends fan out across all;
  endpoints that fall behind the tip are dropped and rejoin when caught up.
- `-key` (required): file holding the issuer private key as 64 hex characters.
  The account must be funded; bombard checks the balance at startup.

Everything else has defaults; see `-h`.

## Design

Single issuer, one strictly increasing nonce, production-shaped. A token
bucket paces issuance at `-rps`; an in-flight cap is the only backpressure.
A tx leaves the system only when its nonce mines: anything still in flight
after `-resubmit` is re-sent verbatim (same bytes, same hash), so the run
survives mempool loss from node crashes and failovers without nonce gaps.
