# bombard

Load generator for Avalanche L1s.

Generic EVM load tools assume block times of a second or more. Avalanche
produces subsecond blocks, so a load tool has to read Avalanche's own block
fields (`timestampMilliseconds`) to measure block times and latency at all,
and it has to scrape the node's Prometheus metrics (`/ext/metrics`) to see
consensus, proposer, and txpool behavior under load instead of just counting
receipts. bombard does both, with a live terminal UI.

```text
Bombard  target=4000 rps  elapsed=59s  status=tracking

 5334 ┤                                        ╭╮
 4923 ┤                                        ││
 4513 ┤              ╭╮           ╭╮  ╭╮╭╮     ││  ╭╮  ╭╮      ╭╮ ╭╮        ╭╮
 4103 ┤              │╰───╮╭╮╭─╮╭╮││ ╭╯││╰───╮ │╰╮ │╰─╮│╰─╮╭──╮││ ││╭╮╭─╮╭─╮│╰
 3693 ┤            ╭─╯    ╰╯╰╯ ╰╯╰╯╰─╯ ╰╯    │ │ ╰─╯  ╰╯  ╰╯  ╰╯╰─╯╰╯╰╯ ╰╯ ╰╯
 3282 ┤            │                         ╰─╯
 2872 ┤            │
 2462 ┤            │
 2051 ┤            │
 1641 ┤            │
 1231 ┤            │
  821 ┤            │
  410 ┤            │
    0 ┼────────────╯
                            mined transactions per second

issued=235993  mined=235241  inflight=198/2000  resubmits=0  minedTps=4010/4000
p50=54ms  p95=209ms  samples=4012
```

## Run

```sh
go run github.com/ava-labs/bombard@latest \
  -rpc http://<host>:9650/ext/bc/<CHAIN_ID>/rpc,http://<host2>:9650/ext/bc/<CHAIN_ID>/rpc \
  -key issuer.key \
  -rps 1000 -duration 60s
```

Or grab a prebuilt binary (linux/darwin, amd64/arm64) from
[releases](https://github.com/ava-labs/bombard/releases).

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
