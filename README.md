# A trading terminal

A trading-desk wall over one replayed session: a quote panel, a watchlist,
most-active and trending grids, a candlestick chart with volume, peer
performance against an index, a price-spread heat map and KPI tiles, all
viewers of a single data router. A one-second stream is replayed on a clock
the page can pause, speed up (1x, 2x, 5x, 10x) and seek, and the stream of
the selected ticker is folded into the one-minute candle that is still
open.

**[See it running](https://toclocoinc.github.io/lattice-grid-demo-trading-terminal/)**

| | |
| --- | --- |
| Grid on npm | [@toclocoinc/lattice-grid](https://www.npmjs.com/package/@toclocoinc/lattice-grid) |
| Grid repository | [toclocoinc/latticegrid](https://github.com/toclocoinc/latticegrid) |
| Product site | [latticegrid.dev](https://www.latticegrid.dev) |

## What it shows

- **Watchlist** — ten tickers with last, change, % change, high, low and
  volume, updating every replayed second; direction and high-low columns
  render as in-cell indicators.
- **Quote** — the selected ticker's full quote, with analyst recommendation,
  beta vs index and news sentiment.
- **Most active** and **Trending** — rollups of the router (trades, volume,
  % since 08:00), re-ranked as the session moves.
- **Price** — a candlestick chart of the selected ticker with volume bars,
  the open one-minute candle updating live as the one-second stream arrives.
- **Peer performance vs index** — a line chart of % since 08:00 across the
  peer group.
- **Price spread across peers** — a heat map.
- **KPI tiles** — advancers and decliners on the session, and a funds
  total.

Every window reads the same router: one stream in, many viewers, nothing
counted twice, and the page keeps no row copies of its own.

## Data

**Nothing here is real market data.** `data/` holds a synthetic session for
24 fictional tickers covering 2026-09-24 08:00–12:59:59 UTC, as one-second
and one-minute candles per ticker (`data/candle1s/`, `data/candle1m/`, gzip
NDJSON, 5.5 MB in total) plus `data/session.json` describing how it was
generated. Every price and volume comes from a seeded generator; the
generator's statistical calibration was fitted from public exchange data,
but no rows from any exchange are included. The session is ours and is
released under the same MIT licence as the code.

## Grid features used

The data router module (`createDataRouter`: one stream, rollups, per-viewer
subscriptions), `createGrid` and `createHeadlessGrid` over router views,
`createChart` with the `candlestick`, `bar`, `line` and `heatmap` types,
`registerScheme` for the ticker palette, `currency`, `number` and
`timestamp` column types, the layout module for the draggable, resizable
windows, and the KPI module. Modules loaded: `layout`, `data-router`,
`charts`, `kpi`.

## Run it locally

Any static file server will do, for example:

```
npx serve .
```

or Python's built-in server:

```
python3 -m http.server
```

Open the page it prints. No licence key is needed on localhost; a key is
only required once the page is published on a real address, which is why
one appears in `index.html` for this demo's own published address.

## Licence

The code and the synthetic session in this repository are available under
the MIT licence. See [LICENSE](LICENSE).

Lattice Grid itself is a separate commercial product with its own terms. It
is free to use on localhost, with no key and no watermark, so a copy of
this repository runs unrestricted on your own machine. This demo carries a
key for its own published address only, which is why you will find one in
the source. Keys for your own sites come from
[latticegrid.dev](https://www.latticegrid.dev).

This demo is built on Lattice Grid 1.73.0.
