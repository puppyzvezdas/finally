# Market Simulator

Approach and code structure for simulating stock prices when `MASSIVE_API_KEY` is not set. The simulator is the default data source: zero dependencies, no network, works offline, and looks alive.

Code: `backend/app/market/simulator.py` (logic) and `backend/app/market/seed_prices.py` (constants). It plugs into the system through `SimulatorDataSource`, which implements `MarketDataSource` (see `MARKET_INTERFACE.md`).

## Goals

- Plausible price behavior: multiplicative moves, never negative, volatility that differs per stock.
- Correlated sectors (tech moves together) so the heatmap and watchlist look realistic.
- Occasional dramatic jumps for visual interest.
- ~500ms cadence so flash animations and sparklines feel live.
- Dynamic watchlist: add/remove tickers at runtime.

## Model: Geometric Brownian Motion

Per tick, for each ticker:

```
S(t+dt) = S(t) · exp( (μ − σ²/2)·dt + σ·√dt·Z )
```

| Symbol | Meaning |
|---|---|
| `S` | price |
| `μ` | annualized drift (expected return) |
| `σ` | annualized volatility |
| `dt` | tick length as a fraction of a trading year |
| `Z` | standard normal draw (correlated across tickers, see below) |

The `exp()` form guarantees prices stay positive. Using `μ − σ²/2` (Itô correction) makes the expected growth rate match `μ`.

### Time step

```
TRADING_SECONDS_PER_YEAR = 252 · 6.5 · 3600 = 5,896,800
dt = 0.5 / 5,896,800 ≈ 8.48e-8
```

Per-tick move size is `σ·√dt ≈ σ · 2.9e-4`: for σ = 0.25 that's ≈ 0.007% per tick (~$0.014 on a $190 stock). Moves accumulate naturally: over one simulated trading day (46,800 ticks) the standard deviation is about σ/√252 ≈ 1.6%. That is a realistic daily range, but *observed* wall-clock motion is gentle. Raise `dt` (or `σ`) for a livelier demo.

## Correlated moves

Independent draws would make every stock wander separately. Instead:

1. Build a correlation matrix `C` from ticker groups.
2. Compute `L = cholesky(C)` (lower-triangular, `C = L·Lᵀ`).
3. Each tick draw independent `Z_ind ~ N(0, I)` and use `Z = L @ Z_ind`.

Correlation rules (`_pairwise_correlation`, constants in `seed_prices.py`):

| Pair | ρ |
|---|---|
| Any pair involving TSLA | 0.3 (checked first; TSLA is in the tech set but trades on its own) |
| Both tech (AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX) | 0.6 |
| Both finance (JPM, V) | 0.5 |
| Cross-sector or unknown ticker | 0.3 |

All off-diagonals are in [0.3, 0.6] with a unit diagonal, so `C` is positive definite and Cholesky always succeeds. The matrix is rebuilt on every add/remove: O(n²) build, O(n³) factorization; negligible for n < 50.

## Random events

Each tick, each ticker has probability `0.001` of a shock: `price *= 1 ± U(2%, 5%)` with random sign. With 10 tickers at 2 ticks/sec that is roughly one event somewhere every ~50s, which is enough drama without making the data look broken. Configurable via `event_probability` (set `0` for deterministic-ish tests).

## Seed data (`seed_prices.py`)

```python
SEED_PRICES = {"AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00,
               "TSLA": 250.00, "NVDA": 800.00, "META": 500.00, "JPM": 195.00,
               "V": 280.00, "NFLX": 600.00}

TICKER_PARAMS = {              # annualized
    "AAPL": {"sigma": 0.22, "mu": 0.05}, "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05}, "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03}, "NVDA":  {"sigma": 0.40, "mu": 0.08},
    "META": {"sigma": 0.30, "mu": 0.05}, "JPM":   {"sigma": 0.18, "mu": 0.04},
    "V":    {"sigma": 0.17, "mu": 0.04}, "NFLX":  {"sigma": 0.35, "mu": 0.05},
}
DEFAULT_PARAMS = {"sigma": 0.25, "mu": 0.05}     # tickers added at runtime
```

- Unknown tickers (added via the UI or AI) start at `random.uniform(50, 300)` with `DEFAULT_PARAMS` and 0.3 correlation to everything.
- Prices drift from the seeds over a long session and are **not** real quotes; they reset to the seeds on each restart (the simulator holds no persistent state).

## Code structure

### `GBMSimulator` (pure logic, no asyncio)

```python
class GBMSimulator:
    def __init__(self, tickers, dt=DEFAULT_DT, event_probability=0.001, seed=None): ...
    def step(self) -> dict[str, float]      # hot path: advance all tickers, return rounded prices
    def add_ticker(self, ticker) -> None    # seeds price/params, rebuilds Cholesky
    def remove_ticker(self, ticker) -> None
    def get_price(self, ticker) -> float | None
    def get_tickers(self) -> list[str]
```

`step()` flow:

```python
z = L @ np.random.standard_normal(n)                       # correlated draws
for i, t in enumerate(tickers):
    drift = (mu - 0.5 * sigma**2) * dt
    price[t] *= exp(drift + sigma * sqrt(dt) * z[i])       # GBM
    if random.random() < event_prob:                       # shock
        price[t] *= 1 + uniform(0.02, 0.05) * choice([-1, 1])
    out[t] = round(price[t], 2)
```

Internal prices keep full float precision; only the returned values are rounded to cents, so tiny moves are not lost to rounding.

### `SimulatorDataSource` (asyncio adapter)

```python
class SimulatorDataSource(MarketDataSource):
    def __init__(self, price_cache, update_interval=0.5, event_probability=0.001, seed=None)
    async def start(tickers)   # build GBMSimulator, seed cache, create_task(_run_loop)
    async def stop()           # cancel task, await it
    async def add_ticker(t)    # validate + sim.add_ticker + immediate cache seed (also the daily-change baseline)
    async def remove_ticker(t) # sim.remove_ticker + cache.remove
    async def _run_loop()      # while True: step → cache.update → sleep(interval); exceptions logged
```

Separation rationale: `GBMSimulator` is synchronous and deterministic under a seeded RNG, so it is unit-testable without an event loop; `SimulatorDataSource` owns lifecycle and timing.

## Behavioral notes

- Never negative, never NaN (exp of finite values).
- Rounding to 2dp means very quiet tickers can show a `flat` direction for several ticks; that is normal.
- `previous_price` in the cache is the prior *tick*, so flashes occur nearly every tick for volatile names and less for low-vol ones.
- Outside market hours the simulator keeps running (no session calendar); this is intentional for demo purposes.
- Threading: `step()` runs on the event loop thread; it takes microseconds for typical n, so no thread offload is needed.

## Testing approach

Existing tests (`tests/market/test_simulator.py`, `test_simulator_source.py`) cover: valid positive prices, initial seeds, add/remove including Cholesky rebuild, unknown tickers, shock behavior with `event_probability=1.0` / `0.0`, and the async source lifecycle (start seeds the cache, stop cancels, add/remove reflect in the cache).

Also covered (added after the 2026-11-04 review): log-return std-dev ≈ `σ√dt`, TSLA vs V volatility ratio, AAPL/MSFT correlation ≈ 0.6 vs AAPL/JPM ≈ 0.3, all ten default tickers factor and stay positive, shock size in 2-5%, ticker normalization, and seeded reproducibility.

**Determinism:** `GBMSimulator(..., seed=N)` and `SimulatorDataSource(..., seed=N)` use per-instance RNGs (`numpy.random.default_rng` and `random.Random`), so paths (including random seed prices for unknown tickers) are reproducible for E2E runs and never touch the global RNG state.

## Tuning cheat sheet

| Want | Change |
|---|---|
| More visible motion | Increase `dt` (e.g. ×10) or per-ticker `sigma` |
| More drama | Increase `event_probability` (0.002 to 0.005) |
| Tighter sector co-movement | Raise `INTRA_TECH_CORR` (keep < 1; matrix must stay positive definite) |
| Calmer/testing | `event_probability=0`, lower `sigma` |
| New default ticker | Add to `SEED_PRICES`, `TICKER_PARAMS`, and `CORRELATION_GROUPS` |
