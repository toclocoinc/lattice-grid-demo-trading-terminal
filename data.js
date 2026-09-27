/*
 * Lattice Trade - the data layer.
 *
 * Loads the synthetic session (24 fictional tickers, 2026-09-24 08:00-12:59:59 UTC),
 * replays it into a Lattice Grid data router on a clock the page can pause, speed up
 * and seek, and folds the one-second stream of the selected ticker into the minute
 * candle that is still open.
 *
 * Nothing here is real market data: every price and volume comes from a seeded
 * generator (see the dataset README). The page keeps no row copies of its own - the
 * grids are viewers of the router, and the per-symbol summary is a router rollup.
 */
(function (global) {
    'use strict';

    // ---------------------------------------------------------------------
    // The five things a reader changes.
    // ---------------------------------------------------------------------

    /** The ticker universe: every symbol loaded, summarised and charted. */
    const TICKERS = ['ACME', 'VLTX', 'NMBS', 'ORBT', 'HLCN', 'FRRV', 'ZPHR', 'TDLN',
        'PXLR', 'KRNW', 'SLVN', 'MRDR', 'BRGT', 'CSCW', 'OKMT', 'LMRO',
        'VTGO', 'HXLN', 'PLSR', 'TRNQ', 'GLDR', 'ARBL', 'NOVQ', 'WNDL'];

    /** The ten tickers the Watchlist panel shows. */
    const WATCHLIST = ['ACME', 'VLTX', 'NMBS', 'ORBT', 'HLCN', 'MRDR', 'KRNW', 'WNDL', 'TDLN', 'GLDR'];

    /** The replay speeds the header offers, and the one the page starts on. */
    const SPEEDS = [1, 2, 5, 10];
    const START_SPEED = 10;

    /** Where the clock starts, in minutes after the session open (08:00 UTC). */
    const START_OFFSET_MIN = 120;

    /** How many minutes of pre-session context are loaded before 08:00. */
    const LEAD_IN_MIN = 60;

    // ---------------------------------------------------------------------

    /** The synthetic equal-weighted index drawn beside the peers. */
    const INDEX = 'INDEX';
    /** How often, in wall-clock ms, the open minute's candle is re-published. */
    const PARTIAL_MS = 250;
    /** How often, in wall-clock ms, the page is told the clock moved. */
    const TICK_MS = 66;
    /** One minute, in ms. */
    const MINUTE = 60000;

    /**
     * Read one gzipped NDJSON file and parse it.
     *
     * The files are served as-is (`application/gzip`, no `Content-Encoding`), so the
     * browser hands back the compressed bytes and `DecompressionStream` unpacks them.
     *
     * @param {string} url where the file is
     * @returns {Promise<object[]>} one object per line
     */
    async function loadRows(url) {
        const res = await fetch(url);
        if (!res.ok) throw new Error(url + ': HTTP ' + res.status);
        const text = await new Response(res.body.pipeThrough(new DecompressionStream('gzip'))).text();
        return text.split('\n').filter(Boolean).map((line) => JSON.parse(line));
    }

    /**
     * Enrich one candle with the fields every viewer reads.
     *
     * A derived source reads its parent's stored rows, not its computed columns, so
     * the fields the summaries, the heatmap and the peer chart group and reduce by
     * are real fields on the feed's rows rather than grid-level computations.
     *
     * @param {object} row the candle
     * @param {number} base the symbol's session-open price
     * @param {string} company the company name
     * @param {boolean} partial whether the minute is still open
     * @returns {object} the same candle with the derived fields set
     */
    function enrich(row, base, company, partial) {
        row.company = company;
        row.dir = row.close >= row.open ? 'up' : 'down';
        row.pct = base ? ((row.close / base) - 1) * 100 : 0;
        row.spread = row.close ? ((row.high - row.low) / row.close) * 100 : 0;
        row.m5 = Math.floor(row.at / 300000) * 300000;
        row.partial = partial;
        return row;
    }

    /**
     * The per-symbol quote behind the summary rollup: one pass over the group.
     *
     * Memoised on the array the rollup hands each aggregate, so the eight summary
     * fields cost one pass between them rather than eight.
     */
    const quoteCache = new WeakMap();

    /**
     * Reduce one symbol's candles to the figures a watchlist row shows.
     *
     * @param {object[]} rows every candle the route holds for this symbol
     * @param {number} sessionAt the session open, epoch ms
     * @returns {object} last, chg, chgPct, the 15-minute move, session volume and the trend
     */
    function quote(rows, sessionAt) {
        const hit = quoteCache.get(rows);
        if (hit) return hit;
        const ordered = rows.slice().sort((a, b) => a.at - b.at);
        const latest = ordered[ordered.length - 1] || {};
        let base = 0;
        let volume = 0;
        const closes = [];
        for (const row of ordered) {
            if (row.at === sessionAt) base = row.open;
            if (row.at >= sessionAt) volume += row.volume || 0;
            closes.push(row.close);
        }
        if (!base) base = ordered.length ? ordered[0].open : 0;
        // The move over the last fifteen minutes: the close fifteen minutes back,
        // or the earliest close the route holds when the session is younger than that.
        const cutoff = (latest.at || 0) - 15 * MINUTE;
        let then = ordered[0] ? ordered[0].close : 0;
        for (const row of ordered) { if (row.at <= cutoff) then = row.close; }
        const chg15 = then ? ((latest.close / then) - 1) * 100 : 0;
        const out = {
            symbol: latest.symbol,
            company: latest.company,
            at: latest.at,
            last: latest.close,
            chg: latest.close - base,
            chgPct: base ? ((latest.close / base) - 1) * 100 : 0,
            chg15: chg15,
            chg15Abs: Math.abs(chg15),
            volume: volume,
            trend: closes.slice(-30),
        };
        quoteCache.set(rows, out);
        return out;
    }

    /**
     * Build the demo's data layer.
     *
     * @param {{dataDir?: string}} [opts] where the dataset sits
     * @returns {object} the router, the replay clock and the session facts
     */
    function create(opts) {
        const dir = (opts && opts.dataDir) || 'data';
        // `overlap: true` because several viewers read the same route: the candle grid,
        // the per-symbol summary rollup and the session KPI panel all want the candles.
        const router = LatticeGridDataRouter.createDataRouter({ key: 'kind', rowKey: 'id', overlap: true });
        const listeners = { tick: [], ready: [] };
        const state = {
            session: null,        // the manifest
            queue: [],            // every candle, in the order it becomes due
            cursor: 0,            // how far the replay has got
            clockAt: 0,           // the session clock, epoch ms
            wallAt: 0,
            rate: START_SPEED,
            playing: false,
            raf: 0,
            symbol: TICKERS[0],   // whose one-second stream is being folded
            ticks: null,          // that symbol's one-second candles
            ticksFor: null,       // which symbol they belong to
            tickAt: 0,            // how far into them the fold has got
            open: null,           // the minute candle still being built
            emitted: 0,           // when it was last published
            ticked: 0,            // when the page was last told the clock moved
            bases: {},            // each symbol's session-open price
            companies: {},
            bySymbol: {},         // each symbol's minute candles, in time order
            // The price pane's own feed: the selected symbol resampled to the interval
            // the terminal asked for, covering the window it is showing.
            bar: { seconds: 60, span: 30 * MINUTE, rows: new Map() },
        };

        /**
         * Tell the page something happened.
         *
         * @param {string} name the event
         * @param {object} [payload] what to hand the handlers
         * @returns {void}
         */
        function emit(name, payload) {
            for (const fn of listeners[name] || []) fn(payload || {});
        }

        /**
         * Load the session: every ticker's minute candles, plus the equal-weighted index.
         *
         * @returns {Promise<void>} resolved once the router has been seeded
         */
        async function load() {
            const session = await (await fetch(dir + '/session.json')).json();
            state.session = session;
            state.companies = session.companies;
            const sessionAt = session.window.startAt;
            const from = sessionAt - LEAD_IN_MIN * MINUTE;
            const to = session.window.endAt;

            const files = await Promise.all(TICKERS.map((t) => loadRows(dir + '/candle1m/' + t + '.ndjson.gz')));
            const perMinute = new Map();      // minute -> normalised opens/highs/lows/closes
            const rows = [];
            files.forEach((file, i) => {
                const symbol = TICKERS[i];
                const window = file.filter((r) => r.at >= from && r.at <= to);
                const open = window.find((r) => r.at === sessionAt);
                const base = open ? open.open : (window[0] ? window[0].open : 0);
                state.bases[symbol] = base;
                for (const row of window) {
                    rows.push(enrich(row, base, session.companies[symbol], false));
                    if (!base) continue;
                    let cell = perMinute.get(row.at);
                    if (!cell) { cell = { n: 0, open: 0, high: 0, low: 0, close: 0 }; perMinute.set(row.at, cell); }
                    cell.n += 1;
                    cell.open += row.open / base;
                    cell.high += row.high / base;
                    cell.low += row.low / base;
                    cell.close += row.close / base;
                }
            });
            // The index: each minute's equal-weighted mean of the tickers rebased to
            // 100 at the session open. It is a candle like any other, so the peer
            // chart draws it as one more series.
            for (const [at, cell] of perMinute) {
                if (cell.n !== TICKERS.length) continue;
                rows.push(enrich({
                    kind: 'candle1m', id: INDEX + ':1m:' + at, symbol: INDEX, at,
                    open: (cell.open / cell.n) * 100, high: (cell.high / cell.n) * 100,
                    low: (cell.low / cell.n) * 100, close: (cell.close / cell.n) * 100,
                    volume: 0, trades: 0,
                }, 100, 'Equal-weighted index', false));
            }
            state.bases[INDEX] = 100;

            // A minute candle is only knowable once the minute has closed, so it
            // becomes due at its last millisecond rather than at its open.
            state.queue = rows.sort((a, b) => (a.at - b.at) || (a.symbol < b.symbol ? -1 : 1));
            // The same rows indexed by symbol, so resampling one ticker's price does not
            // walk the whole market.
            state.bySymbol = {};
            for (const row of state.queue) (state.bySymbol[row.symbol] = state.bySymbol[row.symbol] || []).push(row);
            state.clockAt = startAt();
            seed();
            emit('ready', { session });
        }

        /**
         * Where the clock starts, and where it returns to when the session runs out.
         *
         * @returns {number} epoch ms
         */
        function startAt() {
            return state.session.window.startAt + START_OFFSET_MIN * MINUTE;
        }

        /**
         * Hand the router every candle that has already closed, in one keyed diff.
         *
         * @returns {void}
         */
        function seed() {
            state.cursor = 0;
            while (state.cursor < state.queue.length
                && state.queue[state.cursor].at + MINUTE - 1 <= state.clockAt) state.cursor += 1;
            router.load(state.queue.slice(0, state.cursor));
        }

        /**
         * Fetch the one-second candles of the ticker whose chart is showing.
         *
         * Only the selected ticker's seconds are ever held: 18,000 rows, ~0.4 MB
         * gzipped, replaced when the selection changes.
         *
         * @param {string} symbol the ticker
         * @returns {Promise<void>} resolved when its seconds are in hand
         */
        async function loadTicks(symbol) {
            if (state.ticksFor === symbol && state.ticks) return;   // already in hand
            state.ticks = null;
            state.ticksFor = symbol;
            state.open = null;
            const rows = await loadRows(dir + '/candle1s/' + symbol + '.ndjson.gz');
            if (state.symbol !== symbol) return;       // the reader moved on while it loaded
            state.ticks = rows;
            state.tickAt = 0;
        }

        /**
         * Fold the seconds of the open minute into a candle and publish it.
         *
         * The candle carries the id the closed minute will carry, so when the real
         * row becomes due it replaces this one exactly. Published at most four times
         * a second so the charts redraw at a readable rate rather than per frame.
         *
         * @param {number} now the wall clock, ms
         * @returns {void}
         */
        function foldSeconds(now) {
            if (!state.ticks) return;
            const minute = Math.floor(state.clockAt / MINUTE) * MINUTE;
            if (!state.open || state.open.at !== minute) {
                state.open = null;
                state.tickAt = 0;
                while (state.tickAt < state.ticks.length && state.ticks[state.tickAt].at < minute) state.tickAt += 1;
            }
            let moved = false;
            while (state.tickAt < state.ticks.length) {
                const tick = state.ticks[state.tickAt];
                if (tick.at >= minute + MINUTE || tick.at + 999 > state.clockAt) break;
                if (!state.open) {
                    state.open = {
                        kind: 'candle1m', id: state.symbol + ':1m:' + minute, symbol: state.symbol,
                        at: minute, open: tick.open, high: tick.high, low: tick.low,
                        close: tick.close, volume: 0, trades: 0,
                    };
                }
                state.open.high = Math.max(state.open.high, tick.high);
                state.open.low = Math.min(state.open.low, tick.low);
                state.open.close = tick.close;
                state.open.volume += tick.volume;
                state.open.trades += tick.trades;
                state.tickAt += 1;
                moved = true;
            }
            if (!moved || now - state.emitted < PARTIAL_MS) return;
            state.emitted = now;
            const row = enrich(Object.assign({}, state.open), state.bases[state.symbol],
                state.companies[state.symbol], true);
            router.apply([{ op: 'upsert', row }]);
        }

        /**
         * The first index in a sorted row array at or after `at`.
         *
         * @param {object[]} rows the rows, in time order
         * @param {number} at the timestamp
         * @returns {number} the index
         */
        function seekIndex(rows, at) {
            let lo = 0;
            let hi = rows.length;
            while (lo < hi) {
                const mid = (lo + hi) >> 1;
                if (rows[mid].at < at) lo = mid + 1; else hi = mid;
            }
            return lo;
        }

        /**
         * Resample the selected ticker into the bars the price pane is showing.
         *
         * A candlestick and a bar chart both band their x axis whatever `axis.x.scale`
         * says (F-TRADE-L), so a pane can only move when a whole slot arrives. The
         * interval therefore follows the range - five-second bars on a thirty-minute
         * window - and this rebuilds the window's slots from the one-second stream, or
         * from the minute candles where a slot is a minute or longer.
         *
         * Keyed by `id`, not by time: the interval is part of a bar's identity, so a
         * range change must retire every old bar rather than leave the ones whose
         * timestamp happens to coincide with a new slot.
         *
         * @returns {Map<string, object>} the window's bars, by id
         */
        function buildBars() {
            const { seconds, span } = state.bar;
            const step = seconds * 1000;
            const symbol = state.symbol;
            const base = state.bases[symbol] || 0;
            const company = state.companies[symbol];
            const from = Math.floor((state.clockAt - span) / step) * step;
            const openSlot = Math.floor(state.clockAt / step) * step;
            const bySlot = new Map();

            /**
             * Fold one source row into its slot.
             *
             * @param {object} row a one-second or one-minute candle
             * @returns {void}
             */
            const fold = (row) => {
                const at = Math.floor(row.at / step) * step;
                if (at < from) return;
                let bar = bySlot.get(at);
                if (!bar) {
                    bar = { kind: 'bar', id: symbol + ':' + seconds + ':' + at, symbol, at,
                        open: row.open, high: row.high, low: row.low, close: row.close, volume: 0, trades: 0 };
                    bySlot.set(at, bar);
                }
                bar.high = Math.max(bar.high, row.high);
                bar.low = Math.min(bar.low, row.low);
                bar.close = row.close;
                bar.volume += row.volume || 0;
                bar.trades += row.trades || 0;
            };

            const ticks = state.ticks;
            if (step >= MINUTE) {
                // Closed slots from the minute candles, which cover the whole day.
                const minutes = state.bySymbol[symbol] || [];
                for (let i = seekIndex(minutes, from); i < minutes.length; i += 1) {
                    if (minutes[i].at + MINUTE - 1 > state.clockAt) break;
                    fold(minutes[i]);
                }
                // The slot still open has no minute candle yet, so it comes from the seconds.
                if (ticks) {
                    for (let i = seekIndex(ticks, openSlot); i < ticks.length; i += 1) {
                        if (ticks[i].at + 999 > state.clockAt) break;
                        fold(ticks[i]);
                    }
                }
            } else if (ticks) {
                for (let i = seekIndex(ticks, from); i < ticks.length; i += 1) {
                    if (ticks[i].at + 999 > state.clockAt) break;
                    fold(ticks[i]);
                }
            }

            const out = new Map();
            // A slot that has just opened has no closed second in it yet. Without this it
            // would appear a moment after it began, and the window would lose its oldest
            // slot before gaining its newest - two re-pitches of every band per close,
            // which is the jitter. A candle that has just opened sits at the last traded
            // price with no volume, which is what a live chart shows.
            if (!bySlot.has(openSlot)) {
                const previous = [...bySlot.values()].pop();
                const price = previous ? previous.close : null;
                if (price !== null) {
                    bySlot.set(openSlot, { kind: 'bar', id: symbol + ':' + seconds + ':' + openSlot,
                        symbol, at: openSlot, open: price, high: price, low: price, close: price,
                        volume: 0, trades: 0 });
                }
            }

            for (const bar of bySlot.values()) {
                bar.company = company;
                bar.dir = bar.close >= bar.open ? 'up' : 'down';
                bar.pct = base ? ((bar.close / base) - 1) * 100 : 0;
                bar.spread = bar.close ? ((bar.high - bar.low) / bar.close) * 100 : 0;
                bar.m5 = Math.floor(bar.at / 300000) * 300000;
                bar.partial = bar.at === openSlot;
                out.set(bar.id, bar);
            }
            return out;
        }

        /**
         * Publish what changed in that window: the open bar every time, a new bar as a
         * slot closes, and a delete as the oldest slides out of the range.
         *
         * @returns {void}
         */
        function pumpBars() {
            if (!state.session) return;
            const next = buildBars();
            const deltas = [];
            for (const [id, bar] of next) {
                const was = state.bar.rows.get(id);
                if (!was || was.close !== bar.close || was.high !== bar.high
                    || was.low !== bar.low || was.volume !== bar.volume) deltas.push({ op: 'upsert', row: bar });
            }
            for (const [id, bar] of state.bar.rows) if (!next.has(id)) deltas.push({ op: 'delete', row: bar });
            state.bar.rows = next;
            if (deltas.length) router.apply(deltas);
        }

        /**
         * One animation frame of replay.
         *
         * @param {number} now the frame's timestamp
         * @returns {void}
         */
        function tick(now) {
            if (!state.playing) return;
            state.clockAt += (now - state.wallAt) * state.rate;
            state.wallAt = now;
            const due = [];
            while (state.cursor < state.queue.length
                && state.queue[state.cursor].at + MINUTE - 1 <= state.clockAt) {
                due.push({ op: 'upsert', row: state.queue[state.cursor] });
                state.cursor += 1;
            }
            if (due.length) router.apply(due);
            foldSeconds(now);
            pumpBars();
            // The header clock, the headline and the rolling window are re-cut off this
            // event, so it fires on a frame budget rather than on every frame.
            if (now - state.ticked >= TICK_MS) { state.ticked = now; emit('tick', { at: state.clockAt }); }
            // The session ends at 13:00; a terminal left open should still be moving, so
            // the replay starts the day again rather than freezing on the last candle.
            if (state.cursor >= state.queue.length) replay.seek(startAt());
            state.raf = requestAnimationFrame(tick);
        }

        const replay = {
            /** Start the clock. @returns {void} */
            play() {
                if (state.playing) return;
                state.playing = true;
                state.wallAt = performance.now();
                state.raf = requestAnimationFrame(tick);
            },
            /** Stop it where it is. @returns {void} */
            pause() { state.playing = false; cancelAnimationFrame(state.raf); },
            /** Whether it is running. @returns {boolean} true while playing */
            playing() { return state.playing; },
            /** Set the rate. @param {number} x the multiple of real time @returns {void} */
            speed(x) { state.rate = x; },
            /** The rate in force. @returns {number} the multiple */
            rate() { return state.rate; },
            /** The session clock. @returns {number} epoch ms */
            now() { return state.clockAt; },
            /** Jump the clock. @param {number} at epoch ms @returns {void} */
            seek(at) { state.clockAt = at; state.open = null; seed(); },
        };

        return {
            router,
            replay,
            tickers: TICKERS,
            watchlist: WATCHLIST,
            speeds: SPEEDS,
            index: INDEX,
            /** The session manifest, once loaded. @returns {object} the manifest */
            session() { return state.session; },
            /** The company behind a ticker. @param {string} s the ticker @returns {string} its name */
            company(s) { return state.companies[s] || s; },
            /** Which ticker's seconds are being folded. @returns {string} the ticker */
            symbol() { return state.symbol; },
            /**
             * The route options that turn the candle feed into one summary row per
             * symbol - the grid every watchlist, trending and most-active panel derives from.
             *
             * @returns {object} the route options `attach` takes
             */
            summaryRoute() {
                return {
                    // No route-level `rowKey`: the route's partition keeps every candle
                    // (keyed by `id`), and the rollup groups that partition by symbol. A
                    // `rowKey: 'symbol'` here would leave one candle per symbol to roll up.
                    filter: (row) => row.symbol !== INDEX,
                    rollup: {
                        groupBy: 'symbol',
                        aggregate: {
                            company: (rows) => quote(rows, state.session.window.startAt).company,
                            at: (rows) => quote(rows, state.session.window.startAt).at,
                            last: (rows) => quote(rows, state.session.window.startAt).last,
                            chg: (rows) => quote(rows, state.session.window.startAt).chg,
                            chgPct: (rows) => quote(rows, state.session.window.startAt).chgPct,
                            chg15: (rows) => quote(rows, state.session.window.startAt).chg15,
                            chg15Abs: (rows) => quote(rows, state.session.window.startAt).chg15Abs,
                            volume: (rows) => quote(rows, state.session.window.startAt).volume,
                            trend: (rows) => quote(rows, state.session.window.startAt).trend,
                        },
                    },
                };
            },
            /**
             * Show the price as bars of `seconds`, covering `span` milliseconds.
             *
             * The terminal asks for the interval its range needs; the feed publishes that
             * series on the `bar` route and keeps exactly the window in it.
             *
             * @param {number} seconds the bar interval
             * @param {number} span how much history to publish, in ms
             * @returns {void}
             */
            bars(seconds, span) {
                state.bar.seconds = seconds;
                state.bar.span = span;
                pumpBars();
            },
            /**
             * Choose the ticker the main chart, the gauges and the headline follow.
             *
             * @param {string} symbol the ticker
             * @returns {Promise<void>} resolved once its seconds are loaded
             */
            select(symbol) {
                state.symbol = symbol;
                pumpBars();                              // the old symbol's bars leave at once
                return loadTicks(symbol).then(pumpBars);
            },
            /**
             * Subscribe to `ready` or `tick`.
             *
             * @param {string} name the event
             * @param {Function} fn the handler
             * @returns {void}
             */
            on(name, fn) { (listeners[name] = listeners[name] || []).push(fn); },
            /**
             * Load the session and seed the router. Attach every viewer first.
             *
             * @returns {Promise<void>} resolved when the first rows are in the grids
             */
            async start() {
                await load();
                await loadTicks(state.symbol);
                replay.play();
            },
        };
    }

    global.TradingData = { create };
}(window));
