# GeyserBench

GeyserBench benchmarks the speed and reliability of Solana gRPC-compatible data feeds so you can compare providers with consistent metrics.

## Highlights

- Benchmark multiple feeds at once (Yellowstone, aRPC, Thor, Shredstream, Jetstream, and custom gRPC endpoints)
- Track first-detection share, latency percentiles (P50/P95/P99), valid transaction counts, and backfill events
- Stream results to the SolStack backend for shareable reports, or keep runs local with a single flag
- Generate a ready-to-edit TOML config on first launch; supply auth tokens and endpoints without code changes

## Installation

### Prebuilt binaries
- Download the latest release from the [GitHub releases page](https://github.com/solstackapp/geyserbench/releases) and place the binary on your `PATH`.

### Build from source
```bash
cargo build --release
```
The compiled binary is written to `target/release/geyserbench`.

## Quick Start

1. Run the binary once to scaffold `config.toml` in the current directory:
   ```bash
   ./target/release/geyserbench
   ```
2. Edit `config.toml` with the accounts, endpoints, and tokens you want to test.
3. Run the benchmark. Use `--config <PATH>` to point at another file or `--private` to disable backend streaming:
   ```bash
   ./target/release/geyserbench --private
   ```

During a run, GeyserBench prints progress updates followed by a side-by-side comparison table. When streaming is enabled the tool also returns a shareable link once the backend finalizes the report.

## Example Output

![CLI output showing endpoint win rates and latency percentiles](./assets/cli_screenshot.png)

## Configuration Reference

`geyserbench` reads a single TOML file that defines the run parameters and endpoints:

```toml
[config]
transactions = 1000
account = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
commitment = "processed"  # processed | confirmed | finalized

[[endpoint]]
name = "Jito Shredstream"
url = "http://localhost:10000"
kind = "shredstream"

[[endpoint]]
name = "Corvus aRPC"
url = "https://fra.corvus-labs.io:20202"
kind = "arpc"

[[endpoint]]
name = "Corvus gRPC"
url = "https://fra.corvus-labs.io:10101"
x_token = "optional-auth-token"
kind = "yellowstone"
```

- `config.transactions` sets how many signatures to evaluate (backend streaming automatically disables itself for extremely large runs).
- `config.account` is the pubkey monitored for transactions during the benchmark.
- `config.commitment` accepts `processed`, `confirmed`, or `finalized`.
- Repeat `[[endpoint]]` blocks for each feed. Supported `kind` values: `yellowstone`, `deshred`, `arpc`, `thor`, `shredstream`, `shreder`, and `jetstream`. Use a unique `name` for each endpoint. `x_token` is optional; an empty token is omitted.

## Agave 4.2.2: deshred versus processed

The client uses `yellowstone-grpc-client 13.5.1` and `yellowstone-grpc-proto 12.7.0`.
The proto version matches the Yellowstone [Agave 4.2.2 release](https://github.com/rpcpool/yellowstone-grpc/releases/tag/v15.2.1%2Bsolana.4.2.2);
the client includes the latest published patch. The server must enable deshred
transaction notifications and implement `SubscribeDeshred`.

The checked-in `config.toml` is ready for a local server exposing both RPCs:

```toml
[config]
transactions = 1000
account = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
commitment = "processed"

[[endpoint]]
name = "Agave Deshred"
url = "http://127.0.0.1:10002"
kind = "deshred"
x_token = ""

[[endpoint]]
name = "Local Processed"
url = "http://127.0.0.1:10002"
kind = "yellowstone"
x_token = ""
```

```bash
cargo build --release
./target/release/geyserbench --config config.toml --private
```

`deshred` calls `SubscribeDeshred`; `yellowstone` calls `Subscribe` with the configured
commitment. Deshred has no commitment or execution metadata. Both use the server's
`account_include` transaction filter, including ALT-loaded accounts, without a
second static-key filter. Votes and failed transactions are not excluded.

Receive timestamps are captured immediately after the decoded stream message becomes
available, before inspecting the transaction, encoding the signature or writing logs.
Comparisons use a shared monotonic clock, never the server's `created_at` timestamp.
The earliest observation of each signature is retained. Progress and the existing
First%, P50/P95/P99 and Valid Tx table count signatures seen by every configured endpoint.
Transactions seen only by deshred do not advance the target.

With `commitment = "processed"`, an additional table reports signed P50/P95/P99 of
`Δt = processed_receive_time - deshred_receive_time` for each deshred/Yellowstone pair,
using only signatures present in both streams. Positive values mean deshred arrived
earlier; negative values mean processed arrived earlier. The original table continues
to report nonnegative delays relative to the first endpoint for each signature.
The signed delta table is local output; the backend metrics format is unchanged.
These are client arrival differences, including transport and scheduling latency.

## CLI Options

- `--config <PATH>` &mdash; load configuration from a different TOML file (defaults to `config.toml`).
- `--private` &mdash; keep results local by skipping the streaming backend, even when the run qualifies for sharing.
- `-h`, `--help` &mdash; show usage information.

Streaming is enabled by default for standard-sized runs and publishes to `https://runs.solstack.app`. You can always opt out with `--private` or by configuring the backend section to point at your own infrastructure.
