# mithril-dash

A read-only web dashboard for monitoring an
[Overclock-Validator/mithril](https://github.com/Overclock-Validator/mithril)
node in real time. It runs as its own process alongside mithril and observes
it purely from the outside (log tailing, Prometheus scraping, state-file and
RPC polling) — no changes to mithril, no code dependency on it.

It shows:

- **Replay pipeline speed** — per-stage latency (preprocess, load accounts,
  tx loop, reward, rent, bank hash, …), live and as 5m/30m/1h averages
- **Voting health** — votes cast, network-landed votes, landed rate,
  broadcast queue/drop and peer-send stats
- **Block production** — leader slots broadcast vs. missed (and why),
  per-slot txns/CU/exec time, shred assembly/repair timing

## Build

```sh
go build -o mithril-dash .
```

## Run

```sh
./mithril-dash \
  -log-dir /mnt/mithril-logs \
  -accounts-path /mnt/mithril-accounts \
  -prometheus-url http://127.0.0.1:9090/metrics \
  -rpc-url http://127.0.0.1:8899 \
  -http-addr :8090
```

Then open `http://<host>:8090/`.

| Flag | Env var | Default | Meaning |
|---|---|---|---|
| `-log-dir` | `MITHRIL_DASH_LOG_DIR` | `/mnt/mithril-logs` | mithril's `storage.logs` dir (contains the `latest` run symlink) |
| `-accounts-path` | `MITHRIL_DASH_ACCOUNTS_PATH` | `/mnt/mithril-accounts` | mithril's `storage.accounts` dir (contains `mithril_state.json`) |
| `-prometheus-url` | `MITHRIL_DASH_PROMETHEUS_URL` | `http://127.0.0.1:9090/metrics` | mithril's Prometheus exporter |
| `-rpc-url` | `MITHRIL_DASH_RPC_URL` | `http://127.0.0.1:8899` | mithril's JSON-RPC endpoint |
| `-http-addr` | `MITHRIL_DASH_HTTP_ADDR` | `:8090` | address mithril-dash's own dashboard listens on |
| `-scrape-interval` | – | `3s` | Prometheus scrape interval |
| `-state-poll-interval` | – | `2s` | `mithril_state.json` poll interval |
| `-rpc-poll-interval` | – | `5s` | mithril RPC poll interval |
| `-mithril-config` | – | – | path to mithril's own `config.toml`; seeds `log-dir`/`accounts-path`/`rpc-url`/cluster/consensus-mode defaults (explicit flags/env still win) |

Instead of setting the paths/ports by hand, you can just point at mithril's
own config:

```sh
./mithril-dash -mithril-config /path/to/mithril/config.toml -http-addr :8090
```
