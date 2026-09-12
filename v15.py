"""
extended_v14.py -- anchor / non-blocking cancel / continuous delta hedging.

ENGINE
======
  1. One wallet posts a SINGLE post-only limit at the touch. The other idles.
  2. On the FIRST fill event, cancel the anchor remainder and fire the hedge
     IOC CONCURRENTLY (asyncio.gather) - not in sequence.
  3. Re-anchor for whatever quantity is left and repeat.
  4. If nothing fills within the market's refresh window, cancel and repost at
     the new touch. Per-market: BTC only.

MEASURED BASIS FOR EACH CHOICE
==============================
[V14-1] CRITICAL FIX to the v13 drain loop: `filled_here` was initialised to
        zero while `pending` was initialised to `seen`. Since the order still
        reports filled_qty == seen, the loop hedged `seen`, then immediately
        re-added it as if it were a new fill, and hedged it a SECOND time.
        Traced: anchor filled 2.00 -> hedged 4.00 -> hedge wallet naked 2.00 in
        the wrong direction. Every partial fill would have done this.
[V14-2] Removed an unreachable `else: break` in the same loop; its condition was
        identical to the break immediately above it.
[V14-3] Deleted other_wallet(), register_resting(), cancel_market_orders_for()
        and the self._resting dict - v11 self-match machinery, dead since v12
        stopped leaving an anchor resting during the cross.
[V14-4] TopOfBook.depth() now feeds the TAKER-HEDGE log line instead of being
        orphaned by the deletion of the v11 depth gate.
[V14-5] Removed unused imports: dataclasses.field, typing.Callable.

[V13-1] THE CANCEL IS NON-BLOCKING. This is the correction to v12, derived from
        the L1 telemetry. At a 250ms cancel RTT the anchor keeps filling after
        the cancel is fired in 69% (BTC) / 56% (HYPE) of cases and fills
        COMPLETELY in 45% / 42%. The cancel simply loses the race about half the
        time -- because the instant our first fill lands, everything ahead of us
        in the L1 queue has drained and we are at the FRONT, absorbing the full
        consumption rate of that price level.

        v12 awaited the cancel before discovering that extra fill, so the extra
        sat naked for tau + hedge_rtt. v13 fires the cancel and keeps consuming
        order events, hedging every delta the moment it lands:
            BTC   v12 avg 252ms worst 400ms  ->  v13 avg 150ms worst 150ms
            HYPE  v12 avg 241ms worst 400ms  ->  v13 avg 150ms worst 150ms
            XAU   v12 avg 165ms worst 400ms  ->  v13 avg 150ms worst 150ms
        Cost is unchanged (-1.250 -> -1.249 bps). This buys risk, not return.

        P(extra fill) and P(anchor fills completely) by your cancel RTT:
            tau      BTC              HYPE             XAU
             50ms    39.2% / 18.1%    24.3% / 18.7%    2.0% / 1.5%
            100ms    55.3% / 27.3%    33.6% / 26.8%    2.7% / 1.9%
            150ms    62.4% / 34.6%    40.0% / 33.0%    4.0% / 2.9%
            250ms    69.2% / 45.2%    56.3% / 41.8%    6.4% / 5.3%
            400ms    73.4% / 54.1%    64.6% / 51.7%    8.1% / 7.1%

[V13-2] DUST IS RARE AND BOUNDED, measured rather than assumed:
            BTC  P(fill delta < min_qty) = 6.6%, max exposure $7.96
            HYPE P(fill delta < min_qty) = 3.0%, max exposure $8.87
            XAU  P(fill delta < min_qty) = 0.9%, max exposure $4.43
        The carry-to-exit policy is fine. No code change needed.

[V12-1] CANCEL ON FIRST FILL. Costs nothing and removes two guards:
            BTC  hold anchor -1.513 bps | cancel-on-partial -1.516 bps
            HYPE hold anchor -1.694 bps | cancel-on-partial -1.660 bps
            XAU  hold anchor -1.208 bps | cancel-on-partial -1.222 bps
        With no order resting when the IOC fires, self-match is impossible by
        construction and unhedged size is bounded by the fill itself.

[V12-2] CONCURRENT, NOT SEQUENTIAL. Cancelling before hedging adds a full round
        trip of naked exposure:
            BTC  concurrent -1.576 | cancel-then-hedge -1.610
            HYPE concurrent -1.663 | cancel-then-hedge -1.763
        The hedge is what matters; the cancel is best-effort alongside it.

[V12-3] PER-MARKET REFRESH. A global timer would hurt XAU. % of anchors that
        NEVER fill inside 420s:
            BTC   no refresh 22.0%  ->  refresh 20-38s  1.6%   ADOPT
            HYPE  no refresh  3.0%  ->  refresh 20-38s  0.0%   not needed
            XAU   no refresh  0.3%  ->  refresh 20-38s  4.1%   HARMFUL
        XAU's ask changes 38 times per HOUR; resetting queue position there
        throws away a slot that takes minutes to regain.
        NOTE: refreshing is not adverse selection in this engine. In v8/v9 both
        legs were makers so the anchor price set the capture directly. Here the
        hedge always executes at market, so only the GAP between anchor fill and
        hedge fill matters, and immediate hedging collapses that to ~0.

[V12-4] MAX_CROSS_TICKS REPLACES IOC_SLIPPAGE. 0.0015 (15 bps) permitted
        sweeping 119 levels on BTC, 132 on HYPE, 66 on XAU. Capping the cross in
        TICKS bounds the worst hedge fill at a known number:
            5 ticks = BTC 0.628 bps | HYPE 0.569 bps | XAU 1.128 bps
        versus 15 bps before - against a 2.5 bps taker fee.

FIXED FROM v11 (found by line-by-line audit)
============================================
[V12-F1] CAPTURE SIGN was inverted for anchor SELL. _note() always computed
         (vwap_hedge - anchor_price), which is correct only when the anchor
         BOUGHT. The anchor side is randomised, so the ledger was wrong about
         half the time. ExecResult now carries anchor_side and the sign follows
         it.
[V12-F2] HEDGE VWAP was estimated from book.mid. A sell hedge fills near the
         BID and a buy hedge near the ASK, so using mid biased every capture by
         half a spread. Now tracked from the side-correct reference price.
[V12-F3] DELETED DEAD CONFIG: UNWIND_WINDOW_SEC, EXIT_SPREAD_WAIT_SEC,
         EXIT_SPREAD_RELAX_AFTER_SEC, TRIM_TIMEOUT_SEC (never referenced) and
         ROTATION_SLICES (appeared only inside a log string). All were still in
         the .env implying they did something.
[V12-F4] DELETED Gate B (SELF_MATCH_DEPTH_MULT) and Gate C (MAX_UNHEDGED_MULT).
         Both guarded a condition that cancel-on-first-fill removes.
[V12-F5] reconcile_delta logged "Maker trim" while firing a taker IOC.
[V12-F6] ExecResult.balanced was a tautology (dust IS filled-hedged). Removed.
[V12-F7] maker_legs counted executions while taker_legs counted orders, so the
         ledger's maker/taker split compared unlike things. Both count legs now.
[V12-F8] note_entry accepted pos_a/pos_b and ignored them.

Carried over: per-market calibration and quote staleness, scoped mass-cancel,
no shared market state, concurrent scheduler, confirmed cancels, WS-preferred
position reads with REST resync, [V11-1] absent-market-is-not-flat fix.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN, ROUND_UP
from typing import Any, Sequence

from dotenv import load_dotenv

from x10.config import MAINNET_CONFIG, TESTNET_CONFIG
from x10.core.stark_account import StarkPerpetualAccount
from x10.clients.rest import RestApiClient
from x10.clients.streamrpc.streamrpc_client import StreamRpcClient
from x10.clients.streamrpc.subscription_params import AccountParams, OrderbooksParams
from x10.models.order import OrderSide, OrderStatus, TimeInForce, SelfTradeProtectionLevel
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
if (_script_dir / ".env").exists():
    load_dotenv(_script_dir / ".env")
if (_script_dir / "v15 env.txt").exists():
    load_dotenv(_script_dir / "v15 env.txt")
elif (_script_dir / "v14 env.txt").exists():
    load_dotenv(_script_dir / "v14 env.txt")
else:
    load_dotenv()

# ================================================================
# DECIMAL CONSTANTS (allocation-free hot paths)
# ================================================================
_ZERO = Decimal("0")
_ONE = Decimal("1")
_TWO = Decimal("2")
_TEN_THOUSAND = Decimal("10000")

_TERMINAL_STATUSES = frozenset({
    OrderStatus.FILLED,
    OrderStatus.CANCELLED,
    OrderStatus.EXPIRED,
    OrderStatus.REJECTED,
})

# ================================================================
# GLOBAL CONFIG
# ================================================================
ENVIRONMENT = os.getenv("ENVIRONMENT", "MAINNET").upper()

MARGIN_FRACTION = Decimal(os.getenv("MARGIN_FRACTION", "0.85"))
RESERVE_USDC = Decimal(os.getenv("RESERVE_USDC", "2"))
MAX_MARGIN_USDC = Decimal(os.getenv("MAX_MARGIN_USDC", "43"))
MAINNET_MICRO_TEST = os.getenv("MAINNET_MICRO_TEST", "0") == "1"
MAINNET_MICRO_MAX_MARGIN_USDC = Decimal(os.getenv("MAINNET_MICRO_MAX_MARGIN_USDC", "5"))

# [V7-8] Concurrency. "at most 2 markets hedged at the same time"
MAX_CONCURRENT_MARKETS = int(os.getenv("MAX_CONCURRENT_MARKETS", "2"))
# Total hedged pairs to open per full rotation. Your "TWAP slices = 4",
# but spread ACROSS markets rather than stacked in one book.
# Hard rule: exactly one pair per market visit. Stacking a second slice into a
# book we are already hedged in is what produced the 2:1 imbalance in v6.
SLICES_PER_MARKET = 1
# After a market fully exits, don't immediately re-enter it.
MARKET_COOLDOWN_SEC = float(os.getenv("MARKET_COOLDOWN_SEC", "81"))

# [V8-1] THE CORE PARAMETER. How many ticks an order may EVER move from the
# price it was first posted at. 0 = never chase; the order rests and waits for
# the market to come back. This single value is worth ~3.3 bps of volume.
#   measured: chase = -4.21 bps/side | anchored = +0.23 bps/side

# Telemetry: books are 1-tick wide 89-98% of the time, so stepping inside is
# rarely available. 2 = take it whenever it exists.

# [V9-1] THE HEDGE RULE. Seconds to wait after the FIRST leg fills before
# crossing the partner. 0 = cross immediately.
#   Measured full-round-trip cost per $ of volume, naked risk priced in:
#       T=0s   -1.456 bps   p05  -2.04   worst  -2.8   naked    0s
#       T=10s  -1.983 bps   p05  -5.06   worst  -6.6   naked   10s
#       T=180s -2.280 bps   p05 -10.97   worst -15.7   naked  180s
#       never  -3.516 bps   p05 -17.99   worst -27.3   naked 1200s
# Waiting NEVER pays: naked risk grows faster than the 2.5 bps fee it saves.

# [V10-1] WS position reads. Inside polling loops v10 reads positions from the
# private WebSocket instead of REST, and force-resyncs from REST every
# WS_POSITION_RESYNC_SEC so a missed WS event cannot persist. Authoritative
# "am I flat" decisions still always use REST.
WS_POSITION_RESYNC_SEC = float(os.getenv("WS_POSITION_RESYNC_SEC", "10"))
# Slower resync during the hold: the pair is already hedged, we are only
# watching for drift, and holds are 6-15 minutes long.
HOLD_DRIFT_RESYNC_SEC = float(os.getenv("HOLD_DRIFT_RESYNC_SEC", "15"))

# Retries for a hedge IOC before giving up and de-risking. Default is 3 with progressive tick cross escalation.
HEDGE_MAX_RETRIES = int(os.getenv("HEDGE_MAX_RETRIES", "3"))
# [V12-4] Max ticks the hedge IOC may cross. Bounds the worst possible hedge
# fill; default 1 tick to eliminate aggressive taker slippage across order book levels.
MAX_CROSS_TICKS = int(os.getenv("MAX_CROSS_TICKS", "1"))
# Poll interval while waiting on anchor order events.
ORDER_POLL_SEC = float(os.getenv("ORDER_POLL_SEC", "0.05"))
# [V12-4] Absolute cap: refuse to cross if MAX_CROSS_TICKS exceeds this in bps.
MAX_CROSS_BPS = Decimal(os.getenv("MAX_CROSS_BPS", "2"))

# [V9-3] How long to wait for the FIRST leg to fill. This wait is RISK-FREE --
# nothing has filled, so nothing can move against us. Long is fine and gives a
# chance at a free double-fill (both legs maker, zero fee).
MAKER_WAIT_SEC = float(os.getenv("MAKER_WAIT_SEC", "42"))

# Short fallback used only if a cross fails and we must retry passively.

ENTRY_TIMEOUT_SEC = MAKER_WAIT_SEC
EXIT_TIMEOUT_SEC = float(os.getenv("EXIT_TIMEOUT_SEC", str(MAKER_WAIT_SEC)))

# [V8-4] Self-match prevention. A and B quote opposite sides of one book; any
# crossing order can match the other wallet. Such a fill is economically void
# and produces no real volume.
ORDER_EVENT_TIMEOUT_SEC = float(os.getenv("ORDER_EVENT_TIMEOUT_SEC", "1.5"))
CANCEL_CONFIRM_TIMEOUT_SEC = float(os.getenv("CANCEL_CONFIRM_TIMEOUT_SEC", "1.0"))
FLATTEN_RETRIES = int(os.getenv("FLATTEN_RETRIES", "8"))
SCHEDULER_TICK_SEC = float(os.getenv("SCHEDULER_TICK_SEC", "1.0"))

# Hold window
HOLD_MIN_SEC = float(os.getenv("HOLD_MIN_SEC", "240"))   # 6 min
HOLD_MAX_SEC = float(os.getenv("HOLD_MAX_SEC", "812"))   # 15 min
# [V7-13] bounded wait for a favourable exit spread

MAX_HEDGE_MISMATCH_QTY = Decimal(os.getenv("MAX_HEDGE_MISMATCH_QTY", "0.00001"))
FUNDING_BUFFER_SEC = int(os.getenv("FUNDING_BUFFER_SEC", "120"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
STATUS_INTERVAL_SEC = float(os.getenv("STATUS_INTERVAL_SEC", "15"))

if ENVIRONMENT not in {"TESTNET", "MAINNET"}:
    raise RuntimeError("ENVIRONMENT must be TESTNET or MAINNET")

if ENVIRONMENT == "MAINNET" and os.getenv("ENABLE_LIVE_TRADING", "") != "I_UNDERSTAND_LIVE_TRADING":
    raise RuntimeError(
        "MAINNET is locked. Set ENABLE_LIVE_TRADING=I_UNDERSTAND_LIVE_TRADING in .env "
        "after reviewing the configuration."
    )

if ENVIRONMENT == "MAINNET" and MAINNET_MICRO_TEST:
    MAX_MARGIN_USDC = min(MAX_MARGIN_USDC, MAINNET_MICRO_MAX_MARGIN_USDC)

CONFIG = TESTNET_CONFIG if ENVIRONMENT == "TESTNET" else MAINNET_CONFIG

# ================================================================
# LOGGING
# ================================================================
script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0] or "extended_v7"
run_num = 1
while os.path.exists(f"{script_name}_log_run_{run_num}.txt"):
    run_num += 1
log_filename = f"{script_name}_log_run_{run_num}.txt"

logger = logging.getLogger("extended_ws_bot")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
_formatter = logging.Formatter(
    "%(asctime)s.%(msecs)03dZ %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"
)
if sys.platform == "win32":
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

_console = logging.StreamHandler(sys.stdout)
_console.setFormatter(_formatter)
logger.addHandler(_console)
_file = logging.FileHandler(log_filename, encoding="utf-8")
_file.setFormatter(_formatter)
logger.addHandler(_file)
logger.info("Auto-logger initialized. Saving run data to: %s", log_filename)


# ================================================================
# PER-MARKET CALIBRATION  (derived from 1h L1 telemetry)
# ================================================================
# refresh_lo/hi   : per-market anchor refresh window (0,0 = never). [V12-3]
# quote_age_sec    : ~p99 of observed inter-update gaps. [V7-3]
# enabled          : XAU off by default. [V7-6]
_MARKET_TUNING: dict[str, dict] = {
    "HYPE-USD": {
        "min_bps": Decimal("0.01"), "max_bps": Decimal("0.16"),
        "lev": (1, 2, 3, 4),
        "quote_age_sec": 12.0,   # 33.0% "stale" at 0.5s; max gap 9.9s
        "enabled": os.getenv("ENABLE_HYPE", "1") == "1",
        "refresh_lo": float(os.getenv("HYPE_REFRESH_LO", "5")),
        "refresh_hi": float(os.getenv("HYPE_REFRESH_HI", "13")),
        "note": "P75 shift 2.64s. Refresh ON (5-13s) prevents toxic stranding.",
    },
    "BTC-USD": {
        "min_bps": Decimal("0.01"), "max_bps": Decimal("1.3"),
        "lev": (4, 5, 6, 7),
        "quote_age_sec": 15.0,
        "enabled": os.getenv("ENABLE_BTC", "1") == "1",
        "refresh_lo": float(os.getenv("BTC_REFRESH_LO", "20")),
        "refresh_hi": float(os.getenv("BTC_REFRESH_HI", "38")),
        "note": "1-tick 97.6% of ticks. Refresh ON: 22.0% -> 1.6% never-fill.",
    },
    "BNB-USD": {
        "min_bps": Decimal("0.01"), "max_bps": Decimal("0.15"),
        "lev": (4, 5, 6, 7),
        "quote_age_sec": 12.0,
        "enabled": os.getenv("ENABLE_BNB", "1") == "1",
        "refresh_lo": float(os.getenv("BNB_REFRESH_LO", "4")),
        "refresh_hi": float(os.getenv("BNB_REFRESH_HI", "8")),
        "note": "Dynamic touch at 0.82Hz. Refresh ON (4-8s) tracks active touch.",
    },
    "XAG-USD": {
        "min_bps": Decimal("0.01"), "max_bps": Decimal("0.32"),
        "lev": (3, 4, 5),
        "quote_age_sec": 20.0,
        "enabled": os.getenv("ENABLE_XAG", "1") == "1",
        "refresh_lo": float(os.getenv("XAG_REFRESH_LO", "0")),
        "refresh_hi": float(os.getenv("XAG_REFRESH_HI", "0")),
        "note": "Low churn (0.12Hz). Refresh OFF: preserves time priority.",
    },
}


@dataclass(frozen=True)
class MarketSpec:
    """[V7-7] Immutable per-market state. Replaces v6's mutable self.cached_*."""
    name: str
    price_step: Decimal
    qty_step: Decimal
    min_qty: Decimal
    min_bps: Decimal
    max_bps: Decimal
    lev_choices: tuple
    quote_age_sec: float
    maker_fee: Decimal
    taker_fee: Decimal
    # [V12-3] per-market anchor refresh window. 0,0 = never refresh.
    refresh_lo: float = 0.0
    refresh_hi: float = 0.0

    def refresh_delay(self) -> float:
        """Randomised seconds before an UNFILLED anchor is repriced. inf = never."""
        if self.refresh_hi <= 0:
            return float("inf")
        return random.uniform(self.refresh_lo, self.refresh_hi)

    def cross_price(self, book: "TopOfBook", side: OrderSide) -> Decimal:
        """Marketable limit, delegating to cross_price_escalated at base multiplier (attempt 1)."""
        return self.cross_price_escalated(book, side, _ONE)

    def cross_price_escalated(self, book: "TopOfBook", side: OrderSide, multiplier: Decimal = _ONE) -> Decimal:
        """Marketable limit with multiplier for progressive retry escalation (attempt 1: 1 tick, attempt 2: 2 ticks, attempt 3+: 3 ticks max)."""
        step = self.price_step
        ticks = min(multiplier, Decimal(str(MAX_CROSS_TICKS)), Decimal("3"))
        offset = step * ticks
        ref = book.ask if side == OrderSide.BUY else book.bid
        cap = ref * min(MAX_CROSS_BPS * multiplier, Decimal("3")) / _TEN_THOUSAND
        if offset > cap:
            offset = cap
        raw = ref + offset if side == OrderSide.BUY else ref - offset
        return self.quantize_price(raw, side)

    def quantize_price(self, price: Decimal, side: OrderSide) -> Decimal:
        """Never round a bid up or an ask down -- that turns post-only into a cross."""
        if side == OrderSide.BUY:
            return floor_step(price, self.price_step)
        return ceil_step(price, self.price_step)


# ================================================================
# HELPERS
# ================================================================
def require_env(prefix: str, suffix: str) -> str:
    key = f"{prefix}_{suffix}"
    value = os.getenv(key, "").strip()
    if not value:
        raise RuntimeError(f"Missing {key} in .env")
    return value


def dec(value: Any) -> Decimal:
    return Decimal(str(value))


def floor_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise ValueError("step must be > 0")
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def ceil_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise ValueError("step must be > 0")
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


def fmt_money(value: Decimal) -> str:
    return f"${value:,.4f}"


def fmt_signed_money(value: Decimal) -> str:
    if value > _ZERO:
        return f"+${value:,.4f}"
    elif value < _ZERO:
        return f"-${abs(value):,.4f}"
    return "$0.0000"


def side_of(position: Decimal) -> OrderSide:
    return OrderSide.SELL if position > 0 else OrderSide.BUY


# ================================================================
# MARKET DATA
# ================================================================
@dataclass
class TopOfBook:
    bid: Decimal
    ask: Decimal
    ts_monotonic: float
    # [V11-5] L1 sizes are needed to make the self-match check deterministic.
    # v10 subscribed with depth="1" and threw the quantities away.
    bid_qty: Decimal = _ZERO
    ask_qty: Decimal = _ZERO

    def depth(self, side: OrderSide) -> Decimal:
        """Resting L1 size on the side an IOC would consume.

        [V14-4] This was added in v11 for the self-match depth gate, which v12
        deleted. It is kept because it now earns its place in the TAKER-HEDGE
        log line: if a hedge ever fills badly you want to see whether L1 was
        thin at that moment. It is diagnostic, not control flow.
        """
        return self.bid_qty if side == OrderSide.SELL else self.ask_qty

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / _TWO

    @property
    def spread(self) -> Decimal:
        return self.ask - self.bid

    @property
    def spread_bps(self) -> Decimal:
        mid = self.mid
        if mid <= 0:
            return _ZERO
        return self.spread / mid * _TEN_THOUSAND

    def age(self) -> float:
        return time.monotonic() - self.ts_monotonic

    def is_fresh(self, max_age_sec: float) -> bool:
        """[V7-3] Freshness is now per-market. A stable book is not a stale book."""
        return self.age() <= max_age_sec


class MarketDataStream:
    """Best-bid/ask cache fed by Extended StreamRPC, one entry per market."""

    def __init__(self, config, markets: Sequence[str]) -> None:
        self._config = config
        self.client = StreamRpcClient(api_url=config.endpoints.stream_rpc_url)
        self.book_events: dict[str, asyncio.Event] = {}
        self.books: dict[str, TopOfBook] = {}
        self.markets_to_track = list(markets)

    async def start(self) -> None:
        await self.client.connect()
        for market_name in self.markets_to_track:
            self.book_events[market_name] = asyncio.Event()
            params = OrderbooksParams(market=market_name, depth="1")
            await self.client.subscribe(params=params, handler=self._make_handler(market_name))
            logger.info("Market WS subscribed: %s", params.topic_id)

    def _make_handler(self, market_name: str):
        async def _on_book(message) -> None:
            try:
                data = message.data
                bid_levels = getattr(data, "bid", None) or []
                ask_levels = getattr(data, "ask", None) or []
                if bid_levels and ask_levels:
                    bid = dec(bid_levels[0].price)
                    ask = dec(ask_levels[0].price)
                    if bid > 0 and ask > bid:
                        # [V11-5] keep L1 sizes for the self-match depth check
                        bqty = dec(getattr(bid_levels[0], "qty", None)
                                   or getattr(bid_levels[0], "amount", None) or "0")
                        aqty = dec(getattr(ask_levels[0], "qty", None)
                                   or getattr(ask_levels[0], "amount", None) or "0")
                        self.books[market_name] = TopOfBook(bid, ask, time.monotonic(),
                                                            bqty, aqty)
                        self.book_events[market_name].set()
            except Exception:
                pass
        return _on_book

    def event(self, market_name: str) -> asyncio.Event:
        ev = self.book_events.get(market_name)
        if ev is None:
            raise RuntimeError(f"Market {market_name} not subscribed")
        return ev

    async def wait_for_fresh(self, market_name: str, max_age_sec: float,
                             timeout: float = 5.0) -> TopOfBook:
        """Raises TimeoutError (not RuntimeError) so callers can catch one type. [V7-14]"""
        deadline = time.monotonic() + timeout
        event = self.event(market_name)
        while True:
            book = self.books.get(market_name)
            if book and book.is_fresh(max_age_sec):
                return book
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"No fresh quote for {market_name} within {timeout}s")
            event.clear()
            try:
                await asyncio.wait_for(event.wait(), remaining)
            except asyncio.TimeoutError:
                raise TimeoutError(f"No fresh quote for {market_name} within {timeout}s")

    async def close(self) -> None:
        await self.client.close()


# ================================================================
# ACCOUNT STREAM
# ================================================================
class AccountStream:
    """Private order/position event cache for one Extended account."""

    def __init__(self, label: str, account_id: str, api_key: str, config) -> None:
        self.label = label
        self.account_id = str(account_id)
        self.api_key = api_key
        self.client = StreamRpcClient(api_url=config.endpoints.stream_rpc_url)
        self.order_events: dict[int, asyncio.Event] = {}
        self.orders: dict[int, Any] = {}
        self.positions: list[Any] = []
        self.positions_by_market: dict[str, Decimal] = {}
        self.positions_seen = False
        self.last_position_monotonic = 0.0
        self.ready = asyncio.Event()
        self.last_message_monotonic = 0.0

    async def start(self) -> None:
        await self.client.connect()
        params = AccountParams(account=self.account_id, api_key=self.api_key)
        await self.client.subscribe(params=params, handler=self._on_account)
        logger.info("%s private WS subscribed: %s", self.label, params.topic_id)

    async def _on_account(self, message) -> None:
        try:
            self.last_message_monotonic = time.monotonic()
            data = message.data
            msg_type = getattr(message, "type", "")
            if msg_type in {"ORDER", "ACCOUNT.ORDER"}:
                for order in data.orders:
                    oid = int(order.id)
                    self.orders[oid] = order
                    self.order_events.setdefault(oid, asyncio.Event()).set()
                self.ready.set()
            elif msg_type in {"POSITION", "ACCOUNT.POSITION"}:
                self.positions = list(getattr(data, "positions", []) or [])
                for p in self.positions:
                    m_name = str(getattr(p, "market", ""))
                    if m_name:
                        st = getattr(p, "status", None)
                        st_str = st.value if hasattr(st, "value") else str(st)
                        if st_str.upper() == "CLOSED":
                            self.positions_by_market[m_name] = _ZERO
                            continue
                        sz = dec(getattr(p, "size", "0"))
                        s_attr = getattr(p, "side", "")
                        s_str = s_attr.value if hasattr(s_attr, "value") else str(s_attr)
                        signed_sz = sz if s_str.upper() == "LONG" else -sz
                        self.positions_by_market[m_name] = signed_sz
                self.positions_seen = True
                self.last_position_monotonic = time.monotonic()
                self.ready.set()
            else:
                self.ready.set()
            self._prune_orders_if_needed()
        except Exception as exc:
            logger.warning("%s account WS message error: %s", self.label, exc)

    def _prune_orders_if_needed(self) -> None:
        """Prune oldest terminal orders to bound memory during 24/7 execution."""
        if len(self.orders) > 600:
            terminal_oids = [
                oid for oid, o in self.orders.items()
                if getattr(o, "status", None) in _TERMINAL_STATUSES
            ]
            for oid in terminal_oids[:300]:
                self.orders.pop(oid, None)
                self.order_events.pop(oid, None)

    async def wait_ready(self, timeout: float = 10.0) -> None:
        await asyncio.wait_for(self.ready.wait(), timeout)

    def latest_order(self, order_id: int):
        return self.orders.get(int(order_id))

    def update_position_from_rest(self, market_name: str, signed_qty: Decimal) -> None:
        self.positions_by_market[market_name] = signed_qty
        self.positions_seen = True

    def signed_position_ws(self, market_name: str) -> Decimal | None:
        """[V11-1] Signed position from private WS feed or authoritative cache."""
        if not self.positions_seen:
            return None
        if market_name in self.positions_by_market:
            return self.positions_by_market[market_name]
        total = _ZERO
        seen = False
        for p in self.positions:
            if str(getattr(p, "market", "")) != market_name:
                continue
            seen = True
            st = getattr(p, "status", None)
            st_str = st.value if hasattr(st, "value") else str(st)
            if st_str.upper() == "CLOSED":
                continue
            size = dec(p.size)
            side_attr = getattr(p, "side", "")
            side_str = side_attr.value if hasattr(side_attr, "value") else str(side_attr)
            total += size if side_str.upper() == "LONG" else -size
        if not seen:
            return None            # ABSENT != FLAT. This is the v10 bug.
        return total

    async def wait_order_terminal(self, order_id: int, timeout: float) -> Any:
        order_id = int(order_id)
        event = self.order_events.setdefault(order_id, asyncio.Event())
        deadline = time.monotonic() + timeout
        while True:
            order = self.orders.get(order_id)
            if order is not None:
                filled = dec(order.filled_qty or "0")
                if order.status in _TERMINAL_STATUSES or (order.qty and filled >= dec(order.qty)):
                    return order
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                order = self.orders.get(order_id)
                if order is not None and (order.status in _TERMINAL_STATUSES or (order.qty and dec(order.filled_qty or "0") >= dec(order.qty))):
                    return order
                return None
            event.clear()
            try:
                await asyncio.wait_for(event.wait(), remaining)
            except asyncio.TimeoutError:
                order = self.orders.get(order_id)
                if order is not None and (order.status in _TERMINAL_STATUSES or (order.qty and dec(order.filled_qty or "0") >= dec(order.qty))):
                    return order
                return None

    async def close(self) -> None:
        await self.client.close()


# ================================================================
# WALLET
# ================================================================
class Wallet:
    def __init__(self, label: str, config) -> None:
        self.label = label
        self.vault_id = require_env(label, "VAULT_ID")
        self.public_key = require_env(label, "PUBLIC_KEY")
        self.private_key = require_env(label, "PRIVATE_KEY")
        self.api_key = require_env(label, "API_KEY")
        account = StarkPerpetualAccount(
            vault=self.vault_id,
            private_key=self.private_key,
            public_key=self.public_key,
            api_key=self.api_key,
        )
        self.rest = RestApiClient(config, account)
        self._config = config
        # The private StreamRPC selector authenticates against the platform
        # account id (AccountModel.id), NOT the Starknet l2_vault. Resolved
        # lazily in resolve_account_id(), which MUST run before stream.start().
        self.account_id: str | None = None
        self.stream: AccountStream | None = None

    async def resolve_account_id(self) -> None:
        result = await self.rest.account.get_account()
        self.account_id = str(result.data.id)
        if self.account_id != self.vault_id:
            logger.info(
                "%s: REST account id=%s differs from l2_vault=%s (expected). "
                "Using account id for the private WS subscription.",
                self.label, self.account_id, self.vault_id,
            )
        self.stream = AccountStream(self.label, self.account_id, self.api_key, self._config)






# ================================================================
# ANCHOR ENGINE  [V12]
# ================================================================
def opposite(side: OrderSide) -> OrderSide:
    return OrderSide.SELL if side == OrderSide.BUY else OrderSide.BUY


@dataclass
class ExecResult:
    """Outcome of one anchor-and-hedge execution."""
    anchor_side: OrderSide | None = None      # [V12-F1] needed for capture sign
    anchor_filled: Decimal = _ZERO
    hedged: Decimal = _ZERO
    dust: Decimal = _ZERO
    anchor_notional: Decimal = _ZERO          # [V12-F2] sum(qty * actual price)
    hedge_notional: Decimal = _ZERO
    taker_fee_paid: Decimal = _ZERO
    anchor_orders: int = 0
    hedge_orders: int = 0
    refreshes: int = 0
    reason: str = ""

    @property
    def traded(self) -> bool:
        return self.anchor_filled > _ZERO

    @property
    def vwap_anchor(self) -> Decimal | None:
        return (self.anchor_notional / self.anchor_filled) if self.anchor_filled > 0 else None

    @property
    def vwap_hedge(self) -> Decimal | None:
        return (self.hedge_notional / self.hedged) if self.hedged > 0 else None

    @property
    def capture(self) -> Decimal:
        """[V12-F1] Signed spread capture, correct for BOTH anchor sides.

        anchor BUY : bought at A, hedge sold at H  -> profit = H - A
        anchor SELL: sold  at A, hedge bought at H -> profit = A - H
        v11 always used (H - A), which was backwards half the time.
        """
        a, h = self.vwap_anchor, self.vwap_hedge
        if a is None or h is None:
            return _ZERO
        diff = (h - a) if self.anchor_side == OrderSide.BUY else (a - h)
        return diff * self.hedged


class AnchorEngine:
    """Post one maker anchor; on the FIRST fill, cancel the remainder and hedge
    it CONCURRENTLY; then re-anchor for whatever is left.

    Deleted from v11: the L1-depth self-match gate and the unhedged-size
    backpressure gate. Both guarded a window that only existed because v11 left
    the anchor resting while hedging. Cancelling on first fill closes that window
    by construction, so neither guard has anything left to protect. [V12-F4]
    """

    def __init__(self, bot: "DeltaNeutralProductionBot") -> None:
        self.bot = bot

    # ------------------------------------------------------------------
    async def execute(self, spec: MarketSpec, anchor_wallet: Wallet,
                      hedge_wallet: Wallet, anchor_side: OrderSide, qty: Decimal,
                      *, reduce_only: bool, tag: str, timeout: float,
                      leverage: Decimal = Decimal("1")) -> ExecResult:
        bot = self.bot
        market = spec.name
        res = ExecResult(anchor_side=anchor_side)
        hedge_side = opposite(anchor_side)
        deadline = time.monotonic() + timeout

        qty = floor_step(qty, spec.qty_step)
        if qty < spec.min_qty:
            res.reason = "below_min_qty"
            return res

        logger.info("[%s] ANCHOR PLAN %s %s qty=%s | hedge on %s | refresh=%s",
                    market, anchor_wallet.label, anchor_side.value, qty,
                    hedge_wallet.label,
                    f"{spec.refresh_lo:.0f}-{spec.refresh_hi:.0f}s"
                    if spec.refresh_hi > 0 else "OFF")

        try:
            while res.anchor_filled < qty and time.monotonic() < deadline:
                if bot.stop_event.is_set() or (bot.draining and not reduce_only):
                    res.reason = "stopping" if bot.stop_event.is_set() else "draining"
                    break
                remaining = floor_step(qty - res.anchor_filled, spec.qty_step)
                if remaining < spec.min_qty:
                    res.reason = "complete"
                    break

                cycle = await self._anchor_cycle(
                    spec, anchor_wallet, hedge_wallet, anchor_side, hedge_side,
                    remaining, reduce_only, tag, deadline, res, leverage=leverage,
                )
                if cycle == "expired":
                    res.reason = "timeout"
                    break
                if cycle == "aborted":
                    if not res.reason:
                        res.reason = "hedge_failed"
                    break
            else:
                if res.anchor_filled >= qty:
                    res.reason = "complete"
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("[%s] anchor engine error: %s", market, exc)
            res.reason = f"error:{exc}"

        res.dust = res.anchor_filled - res.hedged
        if res.dust > _ZERO:
            # [V11-4] a fill smaller than min_order_size cannot be hedged.
            # Carried and netted by the exit, which sizes from REST positions.
            logger.warning("[%s] DUST %s unhedged (min_qty=%s) -- carried to exit.",
                           market, res.dust, spec.min_qty)

        logger.info("[%s] anchor done: filled=%s hedged=%s dust=%s | anchors=%s "
                    "hedges=%s refreshes=%s fee=%s (%s)",
                    market, res.anchor_filled, res.hedged, res.dust,
                    res.anchor_orders, res.hedge_orders, res.refreshes,
                    fmt_money(res.taker_fee_paid), res.reason)
        return res

    # ------------------------------------------------------------------
    async def _anchor_cycle(self, spec, anchor_wallet, hedge_wallet, anchor_side,
                            hedge_side, remaining, reduce_only, tag, deadline, res,
                            leverage: Decimal = Decimal("1")):
        """Place one anchor, wait for a fill or the refresh timer, then act.

        Returns "filled" | "refresh" | "expired" | "aborted".
        """
        bot = self.bot
        market = spec.name
        pos_before = await bot.rest_position_single(anchor_wallet, market)

        async def _check_pos_delta(current_seen: Decimal) -> Decimal:
            try:
                pos_curr = await bot.rest_position_single(anchor_wallet, market)
                delta = abs(pos_curr - pos_before)
                if delta > _ZERO:
                    return max(current_seen, min(delta, remaining))
            except Exception as e:
                logger.debug("[%s] _check_pos_delta error: %s", market, e)
            return current_seen

        book = bot.market_stream.books.get(market)
        if book is None or not book.is_fresh(spec.quote_age_sec):
            try:
                book = await bot.market_stream.wait_for_fresh(
                    market, spec.quote_age_sec, timeout=10.0)
            except TimeoutError:
                return "expired"

        # Pre-placement Spread Gate:
        # Never submit a maker quote if the spread is already blown out (> spec.max_bps)
        # or too narrow (< spec.min_bps). Wait up to 5.0s for band recovery; if still out, abort cleanly.
        # On EXIT (reduce_only), allow wider band (up to 3x max_bps, capped at 10 bps) to prioritize
        # maker closing over catastrophic double-taker emergency flatten.
        effective_max_bps = min(spec.max_bps * Decimal("3.0"), Decimal("10.0")) if reduce_only else spec.max_bps
        if book.spread_bps > effective_max_bps or book.spread_bps < spec.min_bps:
            logger.warning("[%s] Pre-quote spread %.2f bps outside band [%.2f, %.2f] bps (reduce_only=%s) - waiting for band.",
                           market, book.spread_bps, spec.min_bps, effective_max_bps, reduce_only)
            band_deadline = time.monotonic() + 5.0
            while time.monotonic() < band_deadline and not bot.stop_event.is_set() and not bot.draining:
                try:
                    book = await bot.market_stream.wait_for_fresh(market, spec.quote_age_sec, timeout=1.0)
                    if spec.min_bps <= book.spread_bps <= effective_max_bps:
                        break
                except TimeoutError:
                    pass
            else:
                if book.spread_bps > effective_max_bps or book.spread_bps < spec.min_bps:
                    logger.warning("[%s] Spread %.2f bps remained outside band. Aborting cycle.", market, book.spread_bps)
                    res.reason = "adverse_spread"
                    return "aborted"

        price = spec.quantize_price(
            book.bid if anchor_side == OrderSide.BUY else book.ask, anchor_side)

        # [FIX-7] Pre-Trade Risk Control (PTRC):
        # Only fire maker order if BOTH wallets have capital to buy |P| + min_qty (>= min_qty)
        # and maintain sufficient margin for this order.
        if not reduce_only:
            can_afford = await bot.can_afford_maker(
                spec, anchor_wallet, hedge_wallet, remaining, price, leverage
            )
            if not can_afford:
                # Check if anchor wallet holds an unhedged position from a late fill that needs immediate hedging!
                pa, pb = await bot.rest_positions(market)
                net = pa + pb
                anchor_pos = pa if anchor_wallet.label == "WALLET_A" else pb
                if abs(anchor_pos) >= spec.min_qty and abs(net) >= spec.min_qty:
                    logger.critical(
                        "[%s] PTRC detected active unhedged position (%s=%s, net=%s)! Fulfilling hedge immediately.",
                        market, anchor_wallet.label, anchor_pos, net
                    )
                    hedge_qty = floor_step(abs(net), spec.qty_step)
                    if hedge_qty >= spec.min_qty:
                        h_side = OrderSide.SELL if net > _ZERO else OrderSide.BUY
                        hedged_here = await self._hedge(spec, hedge_wallet, h_side, hedge_qty, reduce_only, tag, res)
                        res.hedged += hedged_here
                        res.anchor_filled += hedge_qty
                        _, open_px = await bot.rest_position_detail(anchor_wallet, market)
                        fill_px = open_px if open_px > _ZERO else price
                        bot.record_trade(
                            anchor_wallet.label, market, anchor_side, hedge_qty,
                            fill_px, fee=_ZERO, is_maker=True, tag=f"ANCHOR-PTRC-{tag}", record_cash_flow=True
                        )
                        return "filled"
                logger.warning(
                    "[%s] PTRC ABORT: Cannot afford maker order with |P|+min_qty capital buffer on both wallets. "
                    "Aborting maker order placement.", market
                )
                res.reason = "insufficient_capital"
                return "aborted"

        try:
            oid = await bot.place_limit(
                anchor_wallet, spec, remaining, price, anchor_side,
                post_only=True, reduce_only=reduce_only,
                tif=TimeInForce.GTT, tag=f"ANCHOR-{tag}")
        except Exception as exc:
            logger.warning("[%s] anchor placement failed: %s", market, exc)
            await asyncio.sleep(0.5)
            return "refresh"

        res.anchor_orders += 1
        stream = anchor_wallet.stream
        event = stream.order_events.setdefault(int(oid), asyncio.Event())
        # [V12-3] per-market refresh; inf when the market should never refresh
        refresh_at = time.monotonic() + spec.refresh_delay()
        seen = _ZERO
        ord_c = None

        # ---- wait for the FIRST fill ----
        while True:
            order = stream.latest_order(oid)
            if order is not None:
                seen = min(dec(order.filled_qty or "0"), remaining)
                # [FIX-2] Do not trigger cancel on partial fill < min_qty!
                # Wait until at least min_qty has filled so counterparty CAN hedge.
                if seen >= spec.min_qty:
                    break
                if order.status in _TERMINAL_STATUSES:
                    seen = await _check_pos_delta(seen)
                    if seen > _ZERO:
                        break  # terminal with partial fill < min_qty
                    return "refresh"          # cancelled/rejected with no fill
            now = time.monotonic()
            if now >= deadline:
                ord_c = await bot.cancel_order_confirmed(anchor_wallet, oid)
                order = ord_c or stream.latest_order(oid)
                seen = min(dec(order.filled_qty or "0"), remaining) if order else _ZERO
                seen = await _check_pos_delta(seen)
                if seen > _ZERO:
                    break
                return "expired"
            current_book = bot.market_stream.books.get(market)
            if current_book is not None and current_book.is_fresh(spec.quote_age_sec):
                # 1. Adverse selection protection: pull quote if spread widened beyond maximum profitable threshold
                if current_book.spread_bps > effective_max_bps:
                    logger.warning("[%s] Adverse spread detected (%.2f bps > max %.2f bps, reduce_only=%s) - pulling anchor to avoid toxic fill.",
                                   market, current_book.spread_bps, effective_max_bps, reduce_only)
                    ord_c = await bot.cancel_order_confirmed(anchor_wallet, oid)
                    order = ord_c or stream.latest_order(oid)
                    late = min(dec(order.filled_qty or "0"), remaining) if order else _ZERO
                    late = await _check_pos_delta(late)
                    if late > _ZERO:
                        seen = late
                        break
                    # Wait up to 5.0s for spread normalization instead of immediately cycling and spamming orders
                    band_deadline = time.monotonic() + 5.0
                    while time.monotonic() < band_deadline and not bot.stop_event.is_set() and not bot.draining:
                        try:
                            b_fresh = await bot.market_stream.wait_for_fresh(market, spec.quote_age_sec, timeout=1.0)
                            if spec.min_bps <= b_fresh.spread_bps <= effective_max_bps:
                                return "refresh"
                        except TimeoutError:
                            pass
                    logger.warning("[%s] Spread remained adverse (%.2f bps > max %.2f bps) after pull. Aborting cycle.",
                                   market, current_book.spread_bps, effective_max_bps)
                    res.reason = "adverse_spread"
                    return "aborted"

                # 2. Queue priority preservation: if timer expired but our price is STILL at the best touch,
                # maintain our hard-earned FIFO queue position rather than resetting to the back of the queue!
                if now >= refresh_at:
                    current_touch = spec.quantize_price(
                        current_book.bid if anchor_side == OrderSide.BUY else current_book.ask,
                        anchor_side
                    )
                    if current_touch == price:
                        refresh_at = now + spec.refresh_delay()
                    else:
                        # Price has genuinely moved away: cancel and reprice at the new touch
                        ord_c = await bot.cancel_order_confirmed(anchor_wallet, oid)
                        order = ord_c or stream.latest_order(oid)
                        late = min(dec(order.filled_qty or "0"), remaining) if order else _ZERO
                        late = await _check_pos_delta(late)
                        if late > _ZERO:
                            seen = late
                            break
                        res.refreshes += 1
                        logger.info("[%s] Anchor price %s out of touch (now %s) - repricing at new touch.",
                                    market, price, current_touch)
                        return "refresh"
            elif now >= refresh_at:
                # Book unavailable or stale: safe fallback cancel and refresh
                ord_c = await bot.cancel_order_confirmed(anchor_wallet, oid)
                order = ord_c or stream.latest_order(oid)
                late = min(dec(order.filled_qty or "0"), remaining) if order else _ZERO
                late = await _check_pos_delta(late)
                if late > _ZERO:
                    seen = late               # filled during the cancel; hedge it
                    break
                res.refreshes += 1
                logger.info("[%s] anchor unfilled after refresh window - repricing.",
                            market)
                return "refresh"
            if bot.stop_event.is_set() or (bot.draining and not reduce_only):
                ord_c = await bot.cancel_order_confirmed(anchor_wallet, oid)
                order = ord_c or stream.latest_order(oid)
                seen = min(dec(order.filled_qty or "0"), remaining) if order else _ZERO
                seen = await _check_pos_delta(seen)
                if seen > _ZERO:
                    break
                return "expired"
            event.clear()
            try:
                await asyncio.wait_for(event.wait(), ORDER_POLL_SEC * 4)
            except asyncio.TimeoutError:
                pass

        # [FIX-2] If anchor became terminal with fill < min_qty, counterparty cannot hedge it.
        # Clean it up IMMEDIATELY on the anchor wallet via 2-step roundtrip dust cleanup!
        if seen < spec.min_qty:
            logger.warning("[%s] Anchor partial fill %s is below min_qty %s. Cleaning up immediately on anchor wallet.",
                           market, seen, spec.min_qty)
            avg_p = dec(getattr(order, "average_price", None) or "0") if order else _ZERO
            fill_price = avg_p if avg_p > _ZERO else price
            actual_fee = dec(getattr(order, "payed_fee", None) or "0") if order else _ZERO
            anchor_fee = actual_fee if actual_fee > _ZERO else (seen * fill_price * spec.maker_fee)
            bot.record_trade(anchor_wallet.label, market, anchor_side, seen, fill_price, anchor_fee, is_maker=True, tag="ANCHOR-SUBMIN")
            res.anchor_filled += seen
            res.anchor_notional += seen * fill_price

            actual_pos = await bot.rest_position_single(anchor_wallet, market)
            if abs(actual_pos) > MAX_HEDGE_MISMATCH_QTY:
                book = bot.market_stream.books.get(market)
                if book is None or not book.is_fresh(spec.quote_age_sec):
                    try:
                        book = await bot.market_stream.wait_for_fresh(market, spec.quote_age_sec, timeout=2.0)
                    except TimeoutError:
                        book = bot.market_stream.books.get(market)
                if book is not None:
                    if abs(actual_pos) < spec.min_qty:
                        await bot.clean_sub_min_qty(anchor_wallet, spec, actual_pos, book)
                    else:
                        await bot.emergency_flatten(spec)
            res.reason = "anchor_sub_min_cleaned"
            return "aborted"

        # ---- [V14-FIX-1] FIRST FILL: confirm cancel BEFORE hedging to prevent self-trade ----
        # In v13/v14, firing the cancel non-blocking while concurrently firing the crossing hedge IOC
        # caused Wallet B's hedge IOC to match directly against Wallet A's resting anchor order on the CLOB.
        # This triggered self-trades, double-fills, and regulatory wash-trading flags.
        # Awaiting cancel confirmation guarantees the resting anchor remainder is dead before the crossing hedge IOC fires.
        # [V14-FIX-2] FAST HEDGE ACCELERATOR:
        # If the anchor order is ALREADY terminal (FILLED or CANCELLED) or filled >= remaining,
        # there is ZERO active remainder resting on the CLOB. Fire the hedge IOC IMMEDIATELY (0ms delay)!
        # Only await cancel confirmation if there is an actual live partial remainder resting on the CLOB.
        order = stream.latest_order(oid)
        is_already_terminal = (order is not None and (
            order.status in _TERMINAL_STATUSES or dec(order.filled_qty or "0") >= remaining
        ))
        if not is_already_terminal and (ord_c is None or getattr(ord_c, "status", None) not in _TERMINAL_STATUSES):
            logger.info("[%s] Partial fill %s/%s - confirming anchor cancel before hedging to eliminate self-trades.",
                        market, seen, remaining)
            ord_c = await bot.cancel_order_confirmed(anchor_wallet, oid)
            order = ord_c or stream.latest_order(oid)
        else:
            order = ord_c or order

        # Confirmed ground truth final filled quantity from websocket stream / REST
        final = min(dec(order.filled_qty or "0"), remaining) if order else seen
        final = max(final, seen)
        res.anchor_filled += final

        avg_p = dec(getattr(order, "average_price", None) or "0") if order else _ZERO
        fill_price = avg_p if avg_p > _ZERO else price
        res.anchor_notional += final * fill_price

        # Record anchor trade with exact fill price and fee
        actual_fee = dec(getattr(order, "payed_fee", None) or "0") if order else _ZERO
        anchor_fee = actual_fee if actual_fee > _ZERO else (final * fill_price * spec.maker_fee)
        bot.record_trade(anchor_wallet.label, market, anchor_side, final, fill_price, anchor_fee, is_maker=True, tag=f"ANCHOR-{tag}")

        # Hedge the confirmed filled quantity in one consolidated IOC
        hedge_qty = floor_step(final, spec.qty_step)
        hedged_here = _ZERO
        if hedge_qty >= spec.min_qty:
            hedged_here = await self._hedge(spec, hedge_wallet, hedge_side,
                                            hedge_qty, reduce_only, tag, res)
            res.hedged += hedged_here
        else:
            logger.warning("[%s] Anchor final fill %s is below min_qty %s. Cleaning up immediately on anchor wallet.",
                           market, final, spec.min_qty)
            actual_pos = await bot.rest_position_single(anchor_wallet, market)
            if abs(actual_pos) > MAX_HEDGE_MISMATCH_QTY:
                book = bot.market_stream.books.get(market)
                if book is not None:
                    if abs(actual_pos) < spec.min_qty:
                        await bot.clean_sub_min_qty(anchor_wallet, spec, actual_pos, book)
                    else:
                        await bot.emergency_flatten(spec)
            res.reason = "anchor_sub_min_cleaned"
            return "aborted"

        # Anything that landed in the last instant before the cancel confirmed that wasn't hedged.
        leftover = floor_step(res.anchor_filled - res.hedged, spec.qty_step)
        if leftover >= spec.min_qty:
            more = await self._hedge(spec, hedge_wallet, hedge_side, leftover,
                                     reduce_only, tag, res)
            res.hedged += more
            if res.hedged < res.anchor_filled:
                logger.critical("[%s] UNHEDGED GAP after final hedge: filled=%s hedged=%s gap=%s. Reconciling immediately!",
                                market, res.anchor_filled, res.hedged, res.anchor_filled - res.hedged)
                await bot.reconcile_delta(spec)
                return "aborted"
        elif leftover > _ZERO:
            logger.warning("[%s] Sub-min_qty leftover after hedge: filled=%s hedged=%s leftover=%s. Cleaning up.",
                           market, res.anchor_filled, res.hedged, leftover)
            await bot.reconcile_delta(spec)
            pa, pb = await bot.rest_positions(market)
            if abs(pa + pb) <= MAX_HEDGE_MISMATCH_QTY:
                book = bot.market_stream.books.get(market)
                recon_px = book.mid if (book and book.mid > _ZERO) else (res.vwap_anchor or _ZERO)
                res.hedge_notional += leftover * recon_px
                res.hedged = res.anchor_filled
        elif hedged_here <= _ZERO and seen >= spec.min_qty:
            return "aborted"
        return "filled"

    # ------------------------------------------------------------------
    async def _hedge(self, spec, hedge_wallet, hedge_side, size, reduce_only,
                     tag, res) -> Decimal:
        """Fire one IOC. Returns filled quantity. Records side-correct notional."""
        bot = self.bot
        market = spec.name
        size = floor_step(size, spec.qty_step)
        if size < spec.min_qty:
            return _ZERO

        for attempt in range(1, HEDGE_MAX_RETRIES + 1):
            book = bot.market_stream.books.get(market)
            if book is None or not book.is_fresh(spec.quote_age_sec):
                try:
                    book = await bot.market_stream.wait_for_fresh(
                        market, spec.quote_age_sec, timeout=2.0)
                except TimeoutError:
                    book = bot.market_stream.books.get(market)
            if book is None:
                await asyncio.sleep(0.1)
                continue

            multiplier = Decimal(str(attempt))
            price = spec.cross_price_escalated(book, hedge_side, multiplier)
            ref = book.ask if hedge_side == OrderSide.BUY else book.bid
            try:
                oid = await bot.place_limit(
                    hedge_wallet, spec, size, price, hedge_side,
                    post_only=False, reduce_only=reduce_only,
                    tif=TimeInForce.IOC, tag=f"HEDGE-{tag}")
            except Exception as exc:
                is_429 = "429" in str(exc) or "Rate limited" in str(exc)
                backoff = min(2.0 * attempt, 5.0) if is_429 else (0.02 * attempt)
                logger.warning("[%s] hedge IOC attempt %d failed (rate_limit=%s): %s. Backing off %.2fs",
                               market, attempt, is_429, exc, backoff)
                await asyncio.sleep(backoff)
                continue

            order = await bot.wait_filled(hedge_wallet, oid, ORDER_EVENT_TIMEOUT_SEC)
            got = dec(order.filled_qty or "0") if order is not None else _ZERO
            if got > _ZERO:
                avg_p = dec(getattr(order, "average_price", None) or "0") if order else _ZERO
                fill_px = avg_p if avg_p > _ZERO else ref
                actual_fee = dec(getattr(order, "payed_fee", None) or "0") if order else _ZERO
                fee = actual_fee if actual_fee > _ZERO else (got * fill_px * spec.taker_fee)
                res.hedge_notional += got * fill_px
                res.taker_fee_paid += fee
                res.hedge_orders += 1
                bot.record_trade(hedge_wallet.label, market, hedge_side, got, fill_px, fee, is_maker=False, tag=f"HEDGE-{tag}")
                logger.info("TAKER-HEDGE [%s] %s %s qty=%s ref=%s cap=%s fee=%s "
                            "l1_depth=%s attempt=%d",
                            market, hedge_wallet.label, hedge_side.value,
                            got, ref, price, fmt_money(fee),
                            book.depth(hedge_side), attempt)
                if got < size:
                    logger.warning("[%s] hedge IOC partial %s of %s -- retrying rest.",
                                   market, got, size)
                    rest = await self._hedge(spec, hedge_wallet, hedge_side,
                                             size - got, reduce_only, tag, res)
                    return got + rest
                return got
            logger.warning("[%s] hedge IOC attempt %d no fill (cap %s, mult=%s).",
                           market, attempt, price, multiplier)
            await asyncio.sleep(0.01 * attempt)

        logger.error("[%s] HEDGE FAILED after %d attempts -- %s could not hedge %s.",
                     market, HEDGE_MAX_RETRIES, hedge_wallet.label, size)
        return _ZERO
# ================================================================
# PER-MARKET WORKER  [V11-8]
# ================================================================
class MarketWorker:
    """Full lifecycle of ONE hedged pair in ONE market, via the anchor engine.

    All order/cancel/flatten operations are scoped to this worker's market so
    concurrent workers cannot interfere with each other.
    """

    def __init__(self, bot: "DeltaNeutralProductionBot", spec: MarketSpec,
                 leverage: Decimal, margin: Decimal) -> None:
        self.bot = bot
        self.spec = spec
        self.leverage = leverage
        self.margin = margin
        self.market = spec.name
        self.qty = _ZERO
        self.state = "INIT"
        self.entry_mid = _ZERO
        self.opened_at = 0.0

    def __repr__(self) -> str:
        return f"<MarketWorker {self.market} {self.state} qty={self.qty}>"

    async def run(self) -> bool:
        bot = self.bot
        spec = self.spec
        try:
            # ---------- 1. SCAN ----------
            self.state = "SCANNING"
            book = await self._wait_for_band(spec.min_bps, spec.max_bps, 30.0)
            if book is None or bot.draining or bot.stop_event.is_set():
                logger.info("[%s] spread wait cancelled/timed out or bot draining/stopping. Skipping entry.",
                            self.market)
                return False

            await bot.cancel_market_orders(self.market)
            if not await bot.ensure_flat(spec):
                logger.error("[%s] could not establish flat start.", self.market)
                return False

            # [FIX-7] Pre-Trade Risk Control (PTRC) Capital Gate & Sizing Reconciliation:
            bal_a, bal_b = await bot.balances(max_age=2.5)
            avail_a = dec(bal_a.data.available_for_trade)
            avail_b = dec(bal_b.data.available_for_trade)

            # Sizing: account for margin_buffer (1.05 + taker fee) directly so sized orders
            # never fail can_afford_maker check due to double-buffering.
            fee_rate = spec.taker_fee if spec.taker_fee > _ZERO else Decimal("0.0005")
            margin_buffer = Decimal("1.05") + fee_rate
            max_safe_margin = (min(avail_a, avail_b) - RESERVE_USDC) / margin_buffer
            usable_margin = min(self.margin, max_safe_margin)
            if usable_margin <= _ZERO:
                logger.warning(
                    "[%s] PTRC GATE: Usable margin %s <= 0 after reserving USDC buffer. Skipping entry.",
                    self.market, fmt_money(usable_margin)
                )
                return False

            self.qty = self._qty_for_margin(usable_margin, book.mid)
            if self.qty < spec.min_qty:
                logger.warning(
                    "[%s] PTRC GATE: Sized qty %s below exchange minimum %s (usable_margin=%s). Skipping entry.",
                    self.market, self.qty, spec.min_qty, fmt_money(usable_margin)
                )
                return False

            self.entry_mid = book.mid

            # ---------- 2. ENTRY: anchor + proportional hedge ----------
            if bot.draining or bot.stop_event.is_set():
                logger.info("[%s] Bot is draining/stopping. Skipping entry.", self.market)
                return False
            self.state = "ENTERING"
            # Institutional Zero-Drift Controller: dynamically assigns anchor/hedge roles
            # to mean-revert equity difference (A - B) to zero, and inverts directional stance
            # to guarantee zero cumulative market drift across holds.
            notional = self.qty * book.mid
            anchor_w, hedge_w, anchor_side = bot.choose_entry_roles(self.market, notional)

            logger.info("[%s] ENTRY bid=%s ask=%s (%.4f bps) lev=%sx qty=%s | "
                        "anchor=%s %s, hedge=%s",
                        self.market, book.bid, book.ask, book.spread_bps,
                        self.leverage, self.qty, anchor_w.label,
                        anchor_side.value, hedge_w.label)

            res = await bot.engine.execute(
                spec, anchor_w, hedge_w, anchor_side, self.qty,
                reduce_only=False, tag=f"ENTRY-{self.market}",
                timeout=MAKER_WAIT_SEC, leverage=self.leverage,
            )

            if not res.traded or res.hedged <= _ZERO:
                if res.reason == "anchor_sub_min_cleaned":
                    logger.info("[%s] anchor partial fill below min_qty was cleaned up; no hedged pair established.",
                                self.market)
                elif res.anchor_filled <= _ZERO:
                    logger.info("[%s] anchor never filled (%s). Clean abort: zero fills, zero cost, zero risk.",
                                self.market, res.reason)
                else:
                    logger.critical("[%s] UNHEDGED FILL DETECTED ON ABORT (%s): filled=%s hedged=%s. Initiating orphan recovery!",
                                    self.market, res.reason, res.anchor_filled, res.hedged)
                pa, pb = _ZERO, _ZERO
                for retry in range(1, 4):
                    try:
                        pa, pb = await bot.rest_positions(self.market)
                        break
                    except Exception as exc:
                        is_429 = "429" in str(exc) or "Rate limited" in str(exc)
                        backoff = min(2.0 * retry, 5.0) if is_429 else (0.5 * retry)
                        logger.warning("[%s] orphan check rest_positions attempt %d failed: %s. Backing off %.2fs",
                                       self.market, retry, exc, backoff)
                        await asyncio.sleep(backoff)
                if abs(pa) > MAX_HEDGE_MISMATCH_QTY or abs(pb) > MAX_HEDGE_MISMATCH_QTY:
                    logger.critical("[%s] ORPHAN POSITION DETECTED after untraded abort! pa=%s pb=%s. Reconciling and flattening immediately!",
                                    self.market, pa, pb)
                    await bot.reconcile_orphan_position(spec, pa, pb)
                    await bot.emergency_flatten(spec)
                return False

            # ---------- 3. AUTHORITATIVE NEUTRALITY CHECK (REST) ----------
            self.state = "RECONCILING"
            pos_a, pos_b = await bot.rest_positions(self.market)
            net = pos_a + pos_b
            if abs(net) > MAX_HEDGE_MISMATCH_QTY:
                logger.warning("[%s] post-entry imbalance A=%s B=%s net=%s -- trimming.",
                               self.market, pos_a, pos_b, net)
                if not await bot.reconcile_delta(spec):
                    await bot.emergency_flatten(spec)
                    return False
                pos_a, pos_b = await bot.rest_positions(self.market)

            if abs(pos_a) <= MAX_HEDGE_MISMATCH_QTY and abs(pos_b) <= MAX_HEDGE_MISMATCH_QTY:
                logger.info("[%s] entry produced no net position.", self.market)
                return False

            # [FIX-6] STRICT DELTA NEUTRALITY GATE: Never hold unhedged risk!
            if abs(pos_a + pos_b) > MAX_HEDGE_MISMATCH_QTY:
                logger.critical("[%s] DELTA NEUTRALITY BREACHED after entry: A=%s B=%s net=%s. Flattening immediately!",
                                self.market, pos_a, pos_b, pos_a + pos_b)
                await bot.emergency_flatten(spec)
                return False

            bot.note_entry(self.market, res, self.entry_mid)
            self.opened_at = time.monotonic()
            logger.info("[%s] HEDGED A=%s B=%s net=%s dust=%s",
                        self.market, pos_a, pos_b, pos_a + pos_b, res.dust)

            # ---------- 4. HOLD ----------
            self.state = "HOLDING"
            hold_sec = random.uniform(HOLD_MIN_SEC, HOLD_MAX_SEC)
            logger.info("[%s] holding hedge for %.0fs (%.1f min)",
                        self.market, hold_sec, hold_sec / 60.0)
            hold_deadline = time.monotonic() + hold_sec
            while time.monotonic() < hold_deadline and not bot.stop_event.is_set() and not bot.draining:
                try:
                    remaining_hold = max(0.1, hold_deadline - time.monotonic())
                    await asyncio.wait_for(bot.drain_event.wait(), timeout=min(5.0, remaining_hold))
                    break
                except asyncio.TimeoutError:
                    await self._assert_still_neutral()

            if bot.draining and not bot.stop_event.is_set():
                logger.info("[%s] Ctrl+C received: aborting hold early (%.0fs remaining) to immediately execute strategy maker exit.",
                            self.market, max(0.0, hold_deadline - time.monotonic()))

            # ---------- 5. EXIT: same engine, reduce-only ----------
            self.state = "EXITING"
            if not await self.anchor_exit():
                logger.warning("[%s] anchored exit incomplete -- flattening.", self.market)
                if not await bot.emergency_flatten(spec):
                    logger.critical("[%s] FLATTEN FAILED -- manual intervention.", self.market)
                    return False

            pos_a, pos_b = await bot.rest_positions(self.market)
            if abs(pos_a) > MAX_HEDGE_MISMATCH_QTY or abs(pos_b) > MAX_HEDGE_MISMATCH_QTY:
                logger.warning("[%s] residual after exit A=%s B=%s -- flattening.",
                               self.market, pos_a, pos_b)
                await bot.emergency_flatten(spec)

            bot.completed_pairs += 1
            self.state = "DONE"
            await bot.report_ledger(self.market)
            return True

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("[%s] worker error: %s", self.market, exc)
            try:
                await self.bot.emergency_flatten(self.spec)
            except Exception:
                pass
            return False
        finally:
            try:
                await self.bot.cancel_market_orders(self.market)
            except Exception:
                pass
            if self.state != "DONE":
                self.state = "FAILED"

    # ------------------------------------------------------------------
    async def anchor_exit(self) -> bool:
        """[V11-8] Exit with the same engine, reduce-only, sized from REST."""
        return await self.bot.strategy_anchor_exit(self.spec, self.leverage)

    # ------------------------------------------------------------------
    def _qty_for_margin(self, margin: Decimal, mid: Decimal) -> Decimal:
        if mid <= _ZERO:
            return _ZERO
        notional = margin * self.leverage
        # Sizing entropy: jitter between 85% and 100% of maximum safe size
        # to break deterministic lot size fingerprints detected by exchange wash-trading heuristics
        jitter = Decimal(str(round(random.uniform(0.85, 1.00), 4)))
        qty = floor_step((notional * jitter) / mid, self.spec.qty_step)
        if qty < self.spec.min_qty and (notional / mid) >= self.spec.min_qty:
            qty = self.spec.min_qty
        return qty if qty >= self.spec.min_qty else _ZERO

    async def _wait_for_band(self, lo: Decimal, hi: Decimal, timeout: float):
        spec = self.spec
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and not self.bot.stop_event.is_set() and not self.bot.draining:
            try:
                book = await self.bot.market_stream.wait_for_fresh(
                    self.market, spec.quote_age_sec, timeout=3.0)
                if lo <= book.spread_bps <= hi:
                    return book
            except TimeoutError:
                pass
            try:
                await asyncio.wait_for(self.bot.drain_event.wait(), timeout=0.25)
                return None
            except asyncio.TimeoutError:
                pass
        return None

    async def _assert_still_neutral(self) -> None:
        """Drift check during the hold.

        [V11-1] positions_fast now returns None-safe values: a market missing
        from the WS payload falls through to REST instead of reading as flat,
        which is what produced the false 'DELTA DRIFT A=0.27 B=0' alarms in v10.
        """
        try:
            a, b = await self.bot.positions_fast(self.market,
                                                 max_stale=HOLD_DRIFT_RESYNC_SEC)
            if a is None or b is None:
                return
            net = a + b
            if abs(net) > MAX_HEDGE_MISMATCH_QTY:
                pa, pb = await self.bot.rest_positions(self.market)
                if abs(pa + pb) > MAX_HEDGE_MISMATCH_QTY:
                    logger.error("[%s] DELTA DRIFT during hold: A=%s B=%s net=%s. Re-trimming.",
                                 self.market, pa, pb, pa + pb)
                    await self.bot.reconcile_delta(self.spec)
        except Exception as exc:
            logger.warning("[%s] _assert_still_neutral check failed: %s", self.market, exc)


# ================================================================
# BOT / SCHEDULER
# ================================================================
class DeltaNeutralProductionBot:
    """Two-account delta-neutral maker engine with a concurrent market scheduler.

    Scheduling rules (user requirement):
      * At most MAX_CONCURRENT_MARKETS (2) markets hedged simultaneously.
      * Exactly ONE hedged pair per market visit -- never stack a second slice
        into a book we are already hedged in. Stacking is what produced the
        A=+0.12 / B=-0.24 imbalance in the v6 log.
      * After a pair completes, that market enters a cooldown before it can be
        selected again; the rotation prefers markets we are not currently in.
    """
    # Zero-Drift Controller class-level defaults (ensures safety when instantiated via __new__)
    last_equity_a: Decimal = _ZERO
    last_equity_b: Decimal = _ZERO
    last_anchor_wallet: Wallet | None = None
    last_side_a: OrderSide | None = None
    cumulative_directional_notional_a: Decimal = _ZERO

    def __init__(self) -> None:
        self.wallet_a = Wallet("WALLET_A", CONFIG)
        self.wallet_b = Wallet("WALLET_B", CONFIG)
        self.enabled_markets = [m for m, t in _MARKET_TUNING.items() if t["enabled"]]
        if not self.enabled_markets:
            raise RuntimeError("No markets enabled. Set ENABLE_HYPE=1 / ENABLE_BTC=1.")
        self.market_stream = MarketDataStream(CONFIG, self.enabled_markets)
        self.engine = AnchorEngine(self)
        self.specs: dict[str, MarketSpec] = {}
        self.markets_dict: dict = {}
        self.stop_event = asyncio.Event()
        self.drain_event = asyncio.Event()
        self.draining: bool = False
        self.setup_complete = False
        self.completed_pairs = 0
        self.pairs_unwound = 0
        self.rotation_count = 0
        self.executed_volume = _ZERO
        self.entry_volume = _ZERO
        # [V8-5] cost ledger
        self.gross_capture = _ZERO     # spread captured, in quote currency
        self.taker_cost = _ZERO        # fees actually paid
        self.taker_legs = 0
        self.maker_legs = 0

        # [V14-FIX-1] INSTITUTIONAL DUAL-WALLET LEDGER
        self.initial_equity_a: Decimal | None = None
        self.initial_equity_b: Decimal | None = None
        self.trade_volume_a = _ZERO
        self.trade_volume_b = _ZERO
        self.maker_volume_a = _ZERO
        self.maker_volume_b = _ZERO
        self.taker_volume_a = _ZERO
        self.taker_volume_b = _ZERO
        self.fees_a = _ZERO
        self.fees_b = _ZERO
        self.cash_flow_a = _ZERO
        self.cash_flow_b = _ZERO
        self.maker_legs_a = 0
        self.taker_legs_a = 0
        self.maker_legs_b = 0
        self.taker_legs_b = 0

        self._pos_resync: dict = {}    # market -> monotonic of last REST confirm
        self.rest_position_calls = 0
        self.ws_position_reads = 0
        self.active: dict[str, MarketWorker] = {}
        self.tasks: dict[str, asyncio.Task] = {}
        self.cooldown_until: dict[str, float] = {}
        self._balance_cache = None
        self._balance_cache_ts: float = 0.0

        # Inter-Wallet Zero-Drift Controller
        self.last_anchor_wallet: Wallet | None = None
        self.last_side_a: OrderSide | None = None
        self.cumulative_directional_notional_a: Decimal = _ZERO
        self.last_equity_a: Decimal = _ZERO
        self.last_equity_b: Decimal = _ZERO
        self.initial_equity_a: Decimal = _ZERO
        self.initial_equity_b: Decimal = _ZERO

    def request_stop(self, force: bool = False) -> None:
        """Two-tiered signal dispatcher:
        - 1st call (or force=False): triggers graceful strategy-defined maker exit (drain).
        - 2nd call (or force=True): forces immediate emergency cancel & IOC flatten.
        """
        if not self.draining and not force:
            self.draining = True
            logger.warning("=" * 75)
            logger.warning(">>> STOP SIGNAL (Ctrl+C) RECEIVED: INITIATING STRATEGY-DEFINED GRACEFUL EXIT <<<")
            logger.warning(">>> Halting new entries. Unwinding active positions via passive maker orders. <<<")
            logger.warning(">>> (Press Ctrl+C AGAIN at any time to FORCE immediate emergency flatten)    <<<")
            logger.warning("=" * 75)
            try:
                loop = asyncio.get_running_loop()
                loop.call_soon_threadsafe(self.drain_event.set)
            except RuntimeError:
                self.drain_event.set()
        else:
            logger.critical("!" * 75)
            logger.critical(">>> SECOND STOP SIGNAL (Ctrl+C) RECEIVED: FORCING IMMEDIATE ABORT!          <<<")
            logger.critical(">>> Cancelling all workers and executing aggressive taker IOC flatten!       <<<")
            logger.critical("!" * 75)
            self.stop_event.set()
            try:
                loop = asyncio.get_running_loop()
                loop.call_soon_threadsafe(self.stop_event.set)
            except RuntimeError:
                self.stop_event.set()

    # ---------------- setup ----------------
    async def setup(self) -> None:
        logger.info("Starting Extended %s | API=%s | WS=%s",
                    ENVIRONMENT, CONFIG.endpoints.api_base_url, CONFIG.endpoints.stream_rpc_url)
        logger.info("Enabled markets: %s", ", ".join(self.enabled_markets))
        for m in _MARKET_TUNING:
            if m not in self.enabled_markets:
                logger.warning("Market %s DISABLED -- %s", m, _MARKET_TUNING[m]["note"])

        self.markets_dict = await self.wallet_a.rest.info.get_markets_dict()

        await asyncio.gather(
            self.wallet_a.resolve_account_id(),
            self.wallet_b.resolve_account_id(),
        )

        fee_a, fee_b = await asyncio.gather(
            self.wallet_a.rest.account.get_fees(market_names=self.enabled_markets),
            self.wallet_b.rest.account.get_fees(market_names=self.enabled_markets),
        )

        for name in self.enabled_markets:
            market = self.markets_dict.get(name)
            if market is None:
                raise RuntimeError(f"Market {name!r} not found")
            if not market.active:
                raise RuntimeError(f"Market {name} is not active")
            if getattr(market, "is_rfq", False):
                raise RuntimeError(f"{name} is RFQ; this bot requires an order book")

            tuning = _MARKET_TUNING[name]
            max_lev = dec(market.trading_config.max_leverage)
            lev_choices = tuple(l for l in tuning["lev"] if Decimal(str(l)) <= max_lev)
            if not lev_choices:
                raise RuntimeError(f"{name}: no configured leverage <= market max {max_lev}")

            self.specs[name] = MarketSpec(
                name=name,
                price_step=dec(market.trading_config.min_price_change),
                qty_step=dec(market.trading_config.min_order_size_change),
                min_qty=dec(market.trading_config.min_order_size),
                min_bps=tuning["min_bps"],
                max_bps=tuning["max_bps"],
                lev_choices=lev_choices,
                quote_age_sec=tuning["quote_age_sec"],
                maker_fee=max(self._fee_from(fee_a, name, "maker"),
                              self._fee_from(fee_b, name, "maker")),
                taker_fee=max(self._fee_from(fee_a, name, "taker"),
                              self._fee_from(fee_b, name, "taker")),
                refresh_lo=tuning.get("refresh_lo", 0.0),   # [V12-3]
                refresh_hi=tuning.get("refresh_hi", 0.0),
            )
            s = self.specs[name]
            logger.info(
                "SPEC %s: tick=%s qtystep=%s minqty=%s band=[%s,%s]bps lev=%s "
                "quote_age=%.0fs maker=%s taker=%s | refresh=%s | cross_cap=%s ticks",
                name, s.price_step, s.qty_step, s.min_qty, s.min_bps, s.max_bps,
                s.lev_choices, s.quote_age_sec, s.maker_fee, s.taker_fee,
                (f"{s.refresh_lo:.0f}-{s.refresh_hi:.0f}s" if s.refresh_hi > 0 else "OFF"),
                MAX_CROSS_TICKS,
            )

        await asyncio.gather(
            self.market_stream.start(),
            self.wallet_a.stream.start(),
            self.wallet_b.stream.start(),
        )
        await asyncio.gather(
            self.wallet_a.stream.wait_ready(),
            self.wallet_b.stream.wait_ready(),
        )

        for name in self.enabled_markets:
            await self.cancel_market_orders(name)
            if not await self.ensure_flat(self.specs[name]):
                raise RuntimeError(f"Could not establish flat starting state for {name}")

        # [FIX-8] Reset session trading ledger after startup cleanup.
        # Startup flattening cleans up dirty debris from previous runs.
        # Resetting the ledger guarantees the live trading session starts with 0.00 ghost PnL/fees/volume.
        self.cash_flow_a = _ZERO
        self.cash_flow_b = _ZERO
        self.trade_volume_a = _ZERO
        self.trade_volume_b = _ZERO
        self.maker_volume_a = _ZERO
        self.maker_volume_b = _ZERO
        self.taker_volume_a = _ZERO
        self.taker_volume_b = _ZERO
        self.executed_volume = _ZERO
        self.fees_a = _ZERO
        self.fees_b = _ZERO
        self.maker_legs_a = 0
        self.taker_legs_a = 0
        self.maker_legs_b = 0
        self.taker_legs_b = 0
        self.maker_legs = 0
        self.taker_legs = 0
        self.taker_cost = _ZERO
        self.gross_capture = _ZERO
        logger.info("Startup state verified 100%% flat. Session trading ledger initialized to 0.00 baseline.")

        self.setup_complete = True
        try:
            bal_a, bal_b = await self.balances()
            self.initial_equity_a = dec(bal_a.data.equity)
            self.initial_equity_b = dec(bal_b.data.equity)
            logger.info("INITIAL CLEARING EQUITIES: Wallet A = %s | Wallet B = %s | Combined = %s",
                        fmt_money(self.initial_equity_a), fmt_money(self.initial_equity_b),
                        fmt_money(self.initial_equity_a + self.initial_equity_b))
        except Exception as exc:
            logger.warning("Could not record initial clearing equities: %s", exc)

        logger.info("BOT READY. max_concurrent=%s max_cross_ticks=%s",
                    MAX_CONCURRENT_MARKETS, MAX_CROSS_TICKS)
        if ENVIRONMENT == "MAINNET":
            logger.warning("MAINNET LIVE: margin cap %s per wallet per market",
                           fmt_money(MAX_MARGIN_USDC))

    @staticmethod
    def _fee_from(response, market_name: str, kind: str) -> Decimal:
        for row in response.data or []:
            if row.market == market_name:
                return dec(row.maker_fee_rate if kind == "maker" else row.taker_fee_rate)
        raise RuntimeError(f"No {kind} fee schedule for {market_name}")

    # ---------------- account helpers ----------------
    async def balances(self, max_age: float = 0.0):
        now = time.monotonic()
        if max_age > 0 and self._balance_cache is not None and (now - self._balance_cache_ts) < max_age:
            return self._balance_cache
        res = await asyncio.gather(
            self.wallet_a.rest.account.get_balance(),
            self.wallet_b.rest.account.get_balance(),
        )
        self._balance_cache = res
        self._balance_cache_ts = now
        return res

    @staticmethod
    def signed_position(response) -> Decimal:
        total = _ZERO
        for p in response.data or []:
            st = getattr(p, "status", None)
            st_str = st.value if hasattr(st, "value") else str(st)
            if st_str.upper() == "CLOSED":
                continue
            size = dec(p.size)
            side_attr = getattr(p, "side", "")
            side_str = side_attr.value if hasattr(side_attr, "value") else str(side_attr)
            total += size if side_str.upper() == "LONG" else -size
        return total

    @staticmethod
    def position_detail(response) -> tuple[Decimal, Decimal]:
        """Returns (signed_quantity, weighted_average_open_price) from REST position response."""
        total = _ZERO
        weighted_px = _ZERO
        for p in response.data or []:
            st = getattr(p, "status", None)
            st_str = st.value if hasattr(st, "value") else str(st)
            if st_str.upper() == "CLOSED":
                continue
            size = dec(p.size)
            side_attr = getattr(p, "side", "")
            side_str = side_attr.value if hasattr(side_attr, "value") else str(side_attr)
            signed_sz = size if side_str.upper() == "LONG" else -size
            op = dec(getattr(p, "open_price", "0") or "0")
            weighted_px += size * op
            total += signed_sz
        avg_px = (weighted_px / abs(total)) if abs(total) > _ZERO else _ZERO
        return total, avg_px

    async def rest_position_detail(self, wallet: Wallet, market: str) -> tuple[Decimal, Decimal]:
        """Authoritative position and open price read for a single wallet."""
        resp = await wallet.rest.account.get_positions(market_names=[market])
        return self.position_detail(resp)

    async def rest_positions_detail(self, market: str) -> tuple[tuple[Decimal, Decimal], tuple[Decimal, Decimal]]:
        """Authoritative positions and open prices for both wallets."""
        ra, rb = await asyncio.gather(
            self.wallet_a.rest.account.get_positions(market_names=[market]),
            self.wallet_b.rest.account.get_positions(market_names=[market]),
        )
        detail_a = self.position_detail(ra)
        detail_b = self.position_detail(rb)
        self.wallet_a.stream.update_position_from_rest(market, detail_a[0])
        self.wallet_b.stream.update_position_from_rest(market, detail_b[0])
        self._pos_resync[market] = time.monotonic()
        return detail_a, detail_b

    async def reconcile_orphan_position(self, spec: MarketSpec, pa: Decimal, pb: Decimal) -> None:
        """Ensure any unrecorded orphan position has its opening trade recorded in cash flow
        before emergency_flatten executes, eliminating ledger desync / phantom PnL."""
        for wallet, pos in ((self.wallet_a, pa), (self.wallet_b, pb)):
            if abs(pos) > MAX_HEDGE_MISMATCH_QTY:
                _, open_px = await self.rest_position_detail(wallet, spec.name)
                if open_px <= _ZERO:
                    book = self.market_stream.books.get(spec.name)
                    open_px = book.mid if book and book.mid > _ZERO else _ZERO
                opening_side = OrderSide.BUY if pos > _ZERO else OrderSide.SELL
                logger.warning(
                    "[%s] RECONCILING ORPHAN OPEN on %s: %s %s @ %s to preserve cash-flow integrity.",
                    spec.name, wallet.label, opening_side.value, abs(pos), open_px
                )
                self.record_trade(
                    wallet.label, spec.name, opening_side, abs(pos), open_px,
                    fee=_ZERO, is_maker=True, tag="ORPHAN-OPEN", record_cash_flow=True
                )

    async def rest_positions(self, market: str) -> tuple:
        """Authoritative position read, scoped to ONE market. [V7-7]"""
        self.rest_position_calls += 1
        ra, rb = await asyncio.gather(
            self.wallet_a.rest.account.get_positions(market_names=[market]),
            self.wallet_b.rest.account.get_positions(market_names=[market]),
        )
        pos_a, pos_b = self.signed_position(ra), self.signed_position(rb)
        self.wallet_a.stream.update_position_from_rest(market, pos_a)
        self.wallet_b.stream.update_position_from_rest(market, pos_b)
        self._pos_resync[market] = time.monotonic()
        return pos_a, pos_b

    async def rest_position_single(self, wallet: Wallet, market: str) -> Decimal:
        """Authoritative single wallet position read."""
        self.rest_position_calls += 1
        resp = await wallet.rest.account.get_positions(market_names=[market])
        pos = self.signed_position(resp)
        wallet.stream.update_position_from_rest(market, pos)
        return pos

    async def positions_fast(self, market: str,
                             max_stale: float = WS_POSITION_RESYNC_SEC) -> tuple:
        """[V10-1] WS-preferred position read, for use INSIDE polling loops.

        Why this exists: v10's reconcile_delta refresh and unwind
        loop both ran `rest_positions()` once per loop iteration on every book
        tick (~21 ms on HYPE), so that was ~95 REST calls
        per second per market, ~190 with two markets live -- enough to be rate
        limited by the exchange, which shows up as post-only placement failures.

        Safety model: WS positions are trusted only for max_stale seconds after
        a REST confirmation. Note we deliberately do NOT age-gate on
        last_position_monotonic -- the WS only emits when a position CHANGES, so
        silence means unchanged, not stale (the same mistake v6 made with book
        freshness). The periodic REST resync is what bounds a missed event.

        Authoritative flat/residual decisions still call rest_positions().
        """
        now = time.monotonic()
        last = self._pos_resync.get(market, 0.0)
        if now - last < max_stale:
            ws_a = self.wallet_a.stream.signed_position_ws(market)
            ws_b = self.wallet_b.stream.signed_position_ws(market)
            if ws_a is not None and ws_b is not None:
                self.ws_position_reads += 1
                return ws_a, ws_b
        pos = await self.rest_positions(market)
        self._pos_resync[market] = now
        return pos

    async def cancel_market_orders(self, market: str) -> None:
        """Scoped mass-cancel. NEVER global -- a global cancel would destroy the
        other concurrent worker's resting orders. [V7-7]"""
        await asyncio.gather(
            self.wallet_a.rest.orders.mass_cancel(markets=[market]),
            self.wallet_b.rest.orders.mass_cancel(markets=[market]),
            return_exceptions=True,
        )

    async def cancel_order_confirmed(self, wallet: Wallet, order_id: int | None,
                                     timeout: float = CANCEL_CONFIRM_TIMEOUT_SEC) -> Any:
        """[V7-11] Cancel and CONFIRM terminal status before the caller reposts.
        Returns the terminal order object (truthy) if confirmed, None if unresolved.
        Guarantees wallet.stream.orders cache is updated with REST ground truth.
        """
        if order_id is None:
            return None
        oid = int(order_id)
        cached = wallet.stream.orders.get(oid)
        if cached is not None and getattr(cached, "status", None) == OrderStatus.FILLED:
            return cached
        try:
            await wallet.rest.orders.cancel_order(oid)
        except Exception as exc:
            logger.debug("%s cancel %s raised (may already be terminal): %s",
                         wallet.label, order_id, exc)
        order = await wallet.stream.wait_order_terminal(oid, timeout)
        # Always confirm with REST ground truth if order is missing, unconfirmed, or reports zero fills
        if (order is None or getattr(order, "status", None) not in _TERMINAL_STATUSES
                or dec(getattr(order, "filled_qty", "0") or "0") <= _ZERO):
            try:
                resp = await wallet.rest.account.get_order_by_id(oid)
                rest_order = resp.data
                if rest_order is not None:
                    order = rest_order
                    wallet.stream.orders[oid] = order
                    wallet.stream.order_events.setdefault(oid, asyncio.Event()).set()
            except Exception as exc:
                logger.debug("REST order lookup failed %s/%s: %s", wallet.label, oid, exc)
        return order if (order is not None and getattr(order, "status", None) in _TERMINAL_STATUSES) else None

    async def place_limit(self, wallet: Wallet, spec: MarketSpec, qty: Decimal,
                          price: Decimal, side: OrderSide, *, post_only: bool,
                          reduce_only: bool, tif: TimeInForce, tag: str) -> int:
        # Sanitize external_id to prevent leaking strategy intent / account linkage to exchange surveillance
        external_id = f"x10_{uuid.uuid4().hex[:16]}"
        fee = spec.maker_fee if post_only else spec.taker_fee
        response = await wallet.rest.place_order(
            market_name=spec.name,             # [V7-7] explicit, never self.current_market
            amount_of_synthetic=qty,
            price=price,
            side=side,
            taker_fee=fee,
            post_only=post_only,
            time_in_force=tif,
            reduce_only=reduce_only,
            self_trade_protection_level=SelfTradeProtectionLevel.CLIENT,
            external_id=external_id,
        )
        if response.data is None:
            raise RuntimeError(f"{wallet.label} {tag}: exchange returned no order id")
        order_id = int(response.data.id)
        logger.info("%s [%s] %s id=%s side=%s qty=%s px=%s post_only=%s reduce_only=%s",
                    wallet.label, spec.name, tag, order_id, side.value, qty, price,
                    post_only, reduce_only)
        return order_id

    async def wait_filled(self, wallet: Wallet, order_id: int, timeout: float):
        oid = int(order_id)
        order = await wallet.stream.wait_order_terminal(oid, timeout)
        if order is not None and (getattr(order, "status", None) in _TERMINAL_STATUSES or (getattr(order, "qty", None) and dec(getattr(order, "filled_qty", "0") or "0") >= dec(order.qty))):
            return order
        try:
            resp = await wallet.rest.account.get_order_by_id(oid)
            order = resp.data
            if order is not None:
                wallet.stream.orders[oid] = order
                wallet.stream.order_events.setdefault(oid, asyncio.Event()).set()
            return order
        except Exception as exc:
            logger.debug("REST order reconciliation failed %s/%s: %s",
                         wallet.label, oid, exc)
            return None

    # ---------------- delta reconciliation [V7-9] ----------------
    async def reconcile_delta(self, spec: MarketSpec) -> bool:
        """Restore |pos_a + pos_b| ~= 0 by trimming the over-filled leg.
        Guarantees strict delta-neutrality without leaving unhedged exposure."""
        market = spec.name
        pos_a, pos_b = _ZERO, _ZERO
        for retry in range(1, 4):
            try:
                pos_a, pos_b = await self.rest_positions(market)
                break
            except Exception as exc:
                is_429 = "429" in str(exc) or "Rate limited" in str(exc)
                backoff = min(2.0 * retry, 5.0) if is_429 else (0.5 * retry)
                logger.warning("[%s] reconcile_delta rest_positions attempt %d failed: %s. Backing off %.2fs",
                               market, retry, exc, backoff)
                await asyncio.sleep(backoff)
        else:
            logger.error("[%s] reconcile_delta could not read positions after 3 attempts; invoking emergency_flatten.", market)
            return await self.emergency_flatten(spec)

        net = pos_a + pos_b
        if abs(net) <= MAX_HEDGE_MISMATCH_QTY:
            return True

        logger.warning("[%s] DELTA IMBALANCE A=%s B=%s net=%s. Reconciling.",
                       market, pos_a, pos_b, net)

        # If both legs have the same sign or one is zero while holding mismatch, cannot trim opposite leg: flatten directly
        if (pos_a * pos_b >= _ZERO) and (abs(pos_a) > MAX_HEDGE_MISMATCH_QTY or abs(pos_b) > MAX_HEDGE_MISMATCH_QTY):
            logger.warning("[%s] Both legs have same sign or one is zero (A=%s, B=%s) -- flattening directly.",
                           market, pos_a, pos_b)
            return await self.emergency_flatten(spec)

        # [FIX-1] If mismatch is < spec.min_qty, a standard IOC trim cannot be placed directly.
        # Call emergency_flatten which handles both standard flatten and 2-step dust cleanup.
        if abs(net) < spec.min_qty:
            logger.warning("[%s] residual %s below min order size %s -- invoking dust cleanup via emergency_flatten.",
                           market, net, spec.min_qty)
            return await self.emergency_flatten(spec)

        # Trim the leg whose absolute size is larger.
        wallet, pos = ((self.wallet_a, pos_a) if abs(pos_a) > abs(pos_b)
                       else (self.wallet_b, pos_b))
        # Bound trim_qty by both |net| and |pos| so reduce_only never exceeds actual position size
        trim_qty = min(floor_step(abs(net), spec.qty_step), floor_step(abs(pos), spec.qty_step))
        if trim_qty < spec.min_qty:
            logger.warning("[%s] residual trim_qty %s below min order size %s -- invoking emergency_flatten.",
                           market, trim_qty, spec.min_qty)
            return await self.emergency_flatten(spec)
        trim_side = side_of(pos)

        book = self.market_stream.books.get(market)
        if book is None or not book.is_fresh(spec.quote_age_sec):
            try:
                book = await self.market_stream.wait_for_fresh(market, spec.quote_age_sec, timeout=2.0)
            except TimeoutError:
                book = self.market_stream.books.get(market)
        if book is None:
            logger.error("[%s] no book for trim.", market)
            return await self.emergency_flatten(spec)

        try:
            # Escalate cross ticks (10 ticks) to ensure trim fills immediately
            px = spec.cross_price_escalated(book, trim_side, Decimal("2"))
            oid = await self.place_limit(
                wallet, spec, trim_qty, px, trim_side,
                post_only=False, reduce_only=True, tif=TimeInForce.IOC,
                tag=f"TRIM-{wallet.label}",
            )
            order = await self.wait_filled(wallet, oid, ORDER_EVENT_TIMEOUT_SEC)
            got = dec(order.filled_qty or "0") if order is not None else _ZERO
            if got > 0:
                avg_p = dec(getattr(order, "average_price", None) or "0") if order else _ZERO
                fill_px = avg_p if avg_p > _ZERO else px
                actual_fee = dec(getattr(order, "payed_fee", None) or "0") if order else _ZERO
                fee = actual_fee if actual_fee > _ZERO else (got * fill_px * spec.taker_fee)
                self.record_trade(wallet.label, market, trim_side, got, fill_px, fee, is_maker=False, tag="TRIM")
                logger.warning("TAKER-COST [%s] TRIM %s qty=%s fee=%s",
                               market, wallet.label, got, fmt_money(fee))
        except Exception as exc:
            logger.error("[%s] trim IOC failed: %s", market, exc)

        for retry in range(1, 4):
            try:
                pos_a, pos_b = await self.rest_positions(market)
                break
            except Exception:
                await asyncio.sleep(0.5 * retry)

        if abs(pos_a + pos_b) <= MAX_HEDGE_MISMATCH_QTY:
            logger.info("[%s] delta restored A=%s B=%s", market, pos_a, pos_b)
            return True

        logger.warning("[%s] trim incomplete. Emergency flatten.", market)
        return await self.emergency_flatten(spec)

    # ---------------- flatten ----------------
    async def ensure_flat(self, spec: MarketSpec) -> bool:
        pos_a, pos_b = await self.rest_positions(spec.name)
        if abs(pos_a) <= MAX_HEDGE_MISMATCH_QTY and abs(pos_b) <= MAX_HEDGE_MISMATCH_QTY:
            return True
        logger.warning("[%s] Non-flat positions detected A=%s B=%s; flattening.",
                       spec.name, pos_a, pos_b)
        return await self.emergency_flatten(spec)

    async def clean_sub_min_qty(self, wallet: Wallet, spec: MarketSpec, pos: Decimal, book: TopOfBook) -> bool:
        """Two-step roundtrip cleanup for sub-min_qty residual dust.

        Extended DEX rejects orders with size < min_qty.
        To legally and completely close a position of size 0 < |P| < min_qty:
          Step 1: Place an IOC order of size min_qty in the SAME direction (reduce_only=False).
                  This increases the position to |P| + min_qty, which is strictly >= min_qty.
          Step 2: Place an IOC order of size |P| + min_qty in the OPPOSITE direction (reduce_only=True).
                  This closes the entire position to exactly 0.00000.
        """
        market = spec.name
        abs_pos = abs(pos)
        if abs_pos <= MAX_HEDGE_MISMATCH_QTY:
            return True

        logger.warning("[%s] %s executing 2-step roundtrip dust cleanup for pos=%s (min_qty=%s)",
                       market, wallet.label, pos, spec.min_qty)

        # Step 1: Same direction as position, size = min_qty
        step1_side = OrderSide.BUY if pos > 0 else OrderSide.SELL
        step1_qty = spec.min_qty
        step1_px = spec.cross_price_escalated(book, step1_side, Decimal("3"))

        try:
            oid1 = await self.place_limit(
                wallet, spec, step1_qty, step1_px, step1_side,
                post_only=False, reduce_only=False, tif=TimeInForce.IOC,
                tag=f"DUST1-{wallet.label}",
            )
            order1 = await self.wait_filled(wallet, oid1, ORDER_EVENT_TIMEOUT_SEC)
            got1 = dec(order1.filled_qty or "0") if order1 else _ZERO
            if got1 <= _ZERO:
                logger.error("[%s] %s dust cleanup step 1 failed to fill.", market, wallet.label)
                return False
            avg_p1 = dec(getattr(order1, "average_price", None) or "0") if order1 else _ZERO
            fill_p1 = avg_p1 if avg_p1 > _ZERO else step1_px
            act_fee1 = dec(getattr(order1, "payed_fee", None) or "0") if order1 else _ZERO
            fee1 = act_fee1 if act_fee1 > _ZERO else (got1 * fill_p1 * spec.taker_fee)
            self.record_trade(wallet.label, market, step1_side, got1, fill_p1, fee1, is_maker=False, tag="DUST1", record_cash_flow=True)
        except Exception as exc:
            logger.error("[%s] %s dust cleanup step 1 exception: %s", market, wallet.label, exc)
            return False

        await asyncio.sleep(0.3)
        new_pos = await self.rest_position_single(wallet, market)
        new_abs = abs(new_pos)
        if new_abs < spec.min_qty:
            expected_pos = (abs(pos) + got1) if pos > 0 else -(abs(pos) + got1)
            if abs(expected_pos) >= spec.min_qty:
                logger.info("[%s] %s REST position lag detected (%s); using expected %s",
                            market, wallet.label, new_pos, expected_pos)
                new_pos = expected_pos
                new_abs = abs(new_pos)
            else:
                logger.error("[%s] %s position after step 1 is %s (< min_qty %s)",
                             market, wallet.label, new_pos, spec.min_qty)
                return False

        # Step 2: Opposite direction, size = entire accumulated position (retry up to 3 times)
        for s2_attempt in range(1, 4):
            if s2_attempt > 1:
                await asyncio.sleep(0.3)
                new_pos = await self.rest_position_single(wallet, market)
                new_abs = abs(new_pos)
                if new_abs <= MAX_HEDGE_MISMATCH_QTY:
                    break

            step2_side = side_of(new_pos)
            step2_qty = floor_step(new_abs, spec.qty_step)
            if step2_qty < spec.min_qty:
                break

            fresh_book = self.market_stream.books.get(market) or book
            step2_px = spec.cross_price_escalated(fresh_book, step2_side, Decimal(str(2 + s2_attempt)))

            try:
                oid2 = await self.place_limit(
                    wallet, spec, step2_qty, step2_px, step2_side,
                    post_only=False, reduce_only=True, tif=TimeInForce.IOC,
                    tag=f"DUST2-{wallet.label}",
                )
                order2 = await self.wait_filled(wallet, oid2, ORDER_EVENT_TIMEOUT_SEC)
                got2 = dec(order2.filled_qty or "0") if order2 else _ZERO
                if got2 > _ZERO:
                    avg_p2 = dec(getattr(order2, "average_price", None) or "0") if order2 else _ZERO
                    fill_p2 = avg_p2 if avg_p2 > _ZERO else step2_px
                    act_fee2 = dec(getattr(order2, "payed_fee", None) or "0") if order2 else _ZERO
                    fee2 = act_fee2 if act_fee2 > _ZERO else (got2 * fill_p2 * spec.taker_fee)
                    self.record_trade(wallet.label, market, step2_side, got2, fill_p2, fee2, is_maker=False, tag="DUST2", record_cash_flow=True)
                    logger.info("[%s] %s dust cleanup step 2 attempt %d filled %s.", market, wallet.label, s2_attempt, got2)
                    if got2 >= step2_qty:
                        break
            except Exception as exc:
                logger.warning("[%s] %s dust cleanup step 2 attempt %d failed: %s", market, wallet.label, s2_attempt, exc)

        await asyncio.sleep(0.3)
        final_pos = await self.rest_position_single(wallet, market)
        if abs(final_pos) <= MAX_HEDGE_MISMATCH_QTY:
            logger.info("[%s] %s dust cleanup SUCCESSFUL. Final pos=%s", market, wallet.label, final_pos)
            return True
        logger.warning("[%s] %s dust cleanup incomplete. Final pos=%s", market, wallet.label, final_pos)
        return False

    async def emergency_flatten(self, spec: MarketSpec) -> bool:
        """IOC flatten, scoped to ONE market. Uses authoritative REST positions and supports 2-step dust cleanup."""
        market = spec.name
        for attempt in range(1, FLATTEN_RETRIES + 1):
            try:
                await self.cancel_market_orders(market)
                try:
                    book = await self.market_stream.wait_for_fresh(
                        market, spec.quote_age_sec, timeout=10.0
                    )
                except TimeoutError:
                    book = self.market_stream.books.get(market)
                    if book is None:
                        logger.error("[%s] no book at all; cannot flatten.", market)
                        await asyncio.sleep(0.5)
                        continue
                    logger.warning("[%s] using aged book (%.1fs) for flatten.",
                                   market, book.age())

                # [FIX-5] Always use authoritative REST positions inside emergency_flatten.
                # Avoids error 1137 caused by stale WS position cache.
                pos_a, pos_b = await self.rest_positions(market)
                orders = []
                for wallet, pos in ((self.wallet_a, pos_a), (self.wallet_b, pos_b)):
                    if abs(pos) <= MAX_HEDGE_MISMATCH_QTY:
                        continue
                    side = side_of(pos)
                    qty = floor_step(abs(pos), spec.qty_step)
                    if qty >= spec.min_qty:
                        multiplier = Decimal(str(min(attempt, 3)))
                        px = spec.cross_price_escalated(book, side, multiplier)
                        oid = await self.place_limit(
                            wallet, spec, qty, px, side,
                            post_only=False, reduce_only=True, tif=TimeInForce.IOC,
                            tag=f"FLATTEN-{attempt}-{wallet.label}",
                        )
                        orders.append((wallet, oid, qty, px, side))
                    else:
                        # [FIX-1] Sub-min_qty dust cleanup via 2-step roundtrip
                        await self.clean_sub_min_qty(wallet, spec, pos, book)

                if orders:
                    for w, oid, q, p, s in orders:
                        ord_res = await self.wait_filled(w, oid, ORDER_EVENT_TIMEOUT_SEC)
                        got = dec(ord_res.filled_qty or "0") if ord_res else _ZERO
                        if got > _ZERO:
                            avg_p = dec(getattr(ord_res, "average_price", None) or "0") if ord_res else _ZERO
                            fill_p = avg_p if avg_p > _ZERO else p
                            act_fee = dec(getattr(ord_res, "payed_fee", None) or "0") if ord_res else _ZERO
                            fee = act_fee if act_fee > _ZERO else (got * fill_p * spec.taker_fee)
                            self.record_trade(w.label, market, s, got, fill_p, fee, is_maker=False, tag="FLATTEN", record_cash_flow=True)
                            logger.warning("TAKER-COST [%s] FLATTEN %s qty=%s fee=%s",
                                           market, w.label, got, fmt_money(fee))
                    await asyncio.sleep(0.25)

                pos_a, pos_b = await self.rest_positions(market)
                if abs(pos_a) <= MAX_HEDGE_MISMATCH_QTY and abs(pos_b) <= MAX_HEDGE_MISMATCH_QTY:
                    logger.info("[%s] emergency flatten succeeded on attempt %s", market, attempt)
                    return True
                logger.warning("[%s] flatten residual attempt %s: A=%s B=%s",
                               market, attempt, pos_a, pos_b)
            except Exception as exc:
                logger.error("[%s] flatten attempt %s failed: %s", market, attempt, exc)
            await asyncio.sleep(0.5)
        return False

    async def strategy_anchor_exit(self, spec: MarketSpec, leverage: Decimal | None = None) -> bool:
        """Exit with the defined strategy engine: reduce-only, sized from REST, maker anchor + hedge.

        Sizing from REST (not from the entry's intended qty) is what makes dust
        self-correcting: whatever is actually on each wallet gets closed.
        """
        market = spec.name
        if leverage is None:
            leverage = Decimal(str(spec.lev_choices[0])) if spec.lev_choices else Decimal("2")

        book = self.market_stream.books.get(market)
        entry_mid = book.mid if book else _ZERO

        for attempt in range(1, 3):
            pos_a, pos_b = await self.rest_positions(market)
            if abs(pos_a) <= MAX_HEDGE_MISMATCH_QTY and abs(pos_b) <= MAX_HEDGE_MISMATCH_QTY:
                logger.info("[%s] flat after strategy exit attempt %d.", market, attempt - 1)
                return True

            # If either side is below min_qty, or legs have same sign or one is zero:
            if abs(pos_a) < spec.min_qty or abs(pos_b) < spec.min_qty or (pos_a * pos_b >= _ZERO):
                logger.info("[%s] legs not opposite hedged pair (A=%s, B=%s) -- flattening directly.",
                            market, pos_a, pos_b)
                return await self.emergency_flatten(spec)

            # Anchor on whichever wallet holds the larger position; the hedge
            # leg then mirrors it on the other wallet.
            if abs(pos_a) > abs(pos_b):
                anchor_w, anchor_pos, hedge_w = self.wallet_a, pos_a, self.wallet_b
            elif abs(pos_b) > abs(pos_a):
                anchor_w, anchor_pos, hedge_w = self.wallet_b, pos_b, self.wallet_a
            else:
                init_a = getattr(self, "initial_equity_a", _ZERO)
                init_b = getattr(self, "initial_equity_b", _ZERO)
                delta_a = getattr(self, "last_equity_a", _ZERO) - init_a
                delta_b = getattr(self, "last_equity_b", _ZERO) - init_b
                if delta_a < delta_b:
                    anchor_w, anchor_pos, hedge_w = self.wallet_a, pos_a, self.wallet_b
                else:
                    anchor_w, anchor_pos, hedge_w = self.wallet_b, pos_b, self.wallet_a

            size = floor_step(min(abs(pos_a), abs(pos_b)), spec.qty_step)
            if size < spec.min_qty:
                # Legs differ by less than one lot: nothing hedgeable, cross out.
                logger.info("[%s] exit size %s below min -- flattening directly.",
                            market, size)
                return await self.emergency_flatten(spec)

            logger.info("[%s] STRATEGY MAKER EXIT: %s pos=%s -> placing reduce-only anchor %s qty=%s, hedge=%s",
                        market, anchor_w.label, anchor_pos, side_of(anchor_pos).value, size, hedge_w.label)

            res = await self.engine.execute(
                spec, anchor_w, hedge_w, side_of(anchor_pos), size,
                reduce_only=True, tag=f"EXIT-{market}",
                timeout=EXIT_TIMEOUT_SEC, leverage=leverage,
            )
            self.note_exit(market, res, entry_mid)
            if not res.traded:
                logger.warning("[%s] strategy exit anchor never filled (%s) on attempt %d.",
                               market, res.reason, attempt)
                continue

        pos_a, pos_b = await self.rest_positions(market)
        return (abs(pos_a) <= MAX_HEDGE_MISMATCH_QTY
                and abs(pos_b) <= MAX_HEDGE_MISMATCH_QTY)

    # ---------------- margin / gating / zero-drift controller ----------------
    def choose_entry_roles(self, market: str, notional: Decimal) -> tuple[Wallet, Wallet, OrderSide]:
        """[INSTITUTIONAL ZERO-DRIFT CONTROLLER]
        Dynamically assigns anchor (maker) and hedge (taker) roles, and trade direction,
        to guarantee:
        1. Inter-wallet equity difference (A - B) continuously mean-reverts to zero.
        2. Cumulative directional market exposure (Long vs Short) remains balanced at zero,
           preventing market trends from transferring wealth between accounts.
        """
        # 1. Maker / Taker Allocation based on clearing equity
        eq_a = self.last_equity_a
        eq_b = self.last_equity_b
        init_a = getattr(self, "initial_equity_a", _ZERO)
        init_b = getattr(self, "initial_equity_b", _ZERO)
        delta_a = eq_a - init_a if (init_a > _ZERO or init_b > _ZERO) else eq_a
        delta_b = eq_b - init_b if (init_a > _ZERO or init_b > _ZERO) else eq_b
        session_divergence = delta_a - delta_b

        tot_eq = eq_a + eq_b
        # Dynamic threshold: 20 cents for micro accounts, 0.05% of capital for institutional accounts
        threshold = max(Decimal("0.20"), tot_eq * Decimal("0.0005")) if tot_eq > _ZERO else Decimal("0.20")

        if session_divergence > threshold:
            anchor_w = self.wallet_b
            hedge_w = self.wallet_a
            logger.info("[%s] GOVERNOR: Session Delta_A ($%+.2f) > Delta_B ($%+.2f) + $%.2f. Assigning WALLET_B as Maker Anchor.",
                        market, delta_a, delta_b, threshold)
        elif session_divergence < -threshold:
            anchor_w = self.wallet_a
            hedge_w = self.wallet_b
            logger.info("[%s] GOVERNOR: Session Delta_B ($%+.2f) > Delta_A ($%+.2f) + $%.2f. Assigning WALLET_A as Maker Anchor.",
                        market, delta_b, delta_a, threshold)
        else:
            # Balanced: alternate round-robin
            anchor_w = self.wallet_b if self.last_anchor_wallet == self.wallet_a else self.wallet_a
            hedge_w = self.wallet_b if anchor_w == self.wallet_a else self.wallet_a

        self.last_anchor_wallet = anchor_w

        # 2. Directional Inversion Allocation (Wallet A side)
        # Enforce that Wallet A's cumulative signed notional exposure remains tightly bounded.
        exp_thresh = notional * Decimal("1.5")
        if self.cumulative_directional_notional_a > exp_thresh:
            side_a = OrderSide.SELL
        elif self.cumulative_directional_notional_a < -exp_thresh:
            side_a = OrderSide.BUY
        else:
            side_a = OrderSide.SELL if self.last_side_a == OrderSide.BUY else OrderSide.BUY

        self.last_side_a = side_a
        if side_a == OrderSide.BUY:
            self.cumulative_directional_notional_a += notional
        else:
            self.cumulative_directional_notional_a -= notional

        # 3. Derive anchor_side from anchor_w and side_a
        if anchor_w == self.wallet_a:
            anchor_side = side_a
        else:
            anchor_side = opposite(side_a)

        return anchor_w, hedge_w, anchor_side

    def funding_safe(self) -> bool:
        now = datetime.now(timezone.utc)
        return (3600 - (now.minute * 60 + now.second)) > FUNDING_BUFFER_SEC

    def choose_margin(self, bal_a, bal_b) -> Decimal:
        """[FIX-4] Margin for ONE market slot. Properly divided across concurrent slots so multiple
        workers cannot jointly exceed usable balance."""
        try:
            self.last_equity_a = dec(bal_a.data.equity)
            self.last_equity_b = dec(bal_b.data.equity)
            if self.initial_equity_a <= _ZERO and self.last_equity_a > _ZERO:
                self.initial_equity_a = self.last_equity_a
            if self.initial_equity_b <= _ZERO and self.last_equity_b > _ZERO:
                self.initial_equity_b = self.last_equity_b
        except Exception:
            pass
        a = dec(bal_a.data.available_for_trade)
        b = dec(bal_b.data.available_for_trade)
        usable = min(a, b) - RESERVE_USDC
        if usable <= 0:
            return _ZERO
        active_count = len(self.active)
        remaining_slots = max(1, MAX_CONCURRENT_MARKETS - active_count)
        per_slot = (usable * MARGIN_FRACTION) / Decimal(str(remaining_slots))
        return min(per_slot, MAX_MARGIN_USDC)

    def margin_required(self, spec: MarketSpec, qty: Decimal, price: Decimal, leverage: Decimal) -> Decimal:
        """Calculate required USDC margin to buy/hedge `qty` at `price` with `leverage`,
        including taker fee and a 5% safety buffer for price fluctuation."""
        if qty <= _ZERO or price <= _ZERO:
            return _ZERO
        lev = max(leverage, Decimal("1"))
        notional = qty * price
        base_margin = notional / lev
        # 5% safety buffer + taker fee to cover slippage and fees
        fee_rate = spec.taker_fee if spec.taker_fee > _ZERO else Decimal("0.0005")
        buffer = Decimal("1.05") + fee_rate
        return (base_margin * buffer).quantize(Decimal("0.0001"), rounding=ROUND_UP)

    async def can_afford_maker(
        self,
        spec: MarketSpec,
        anchor_wallet: Wallet,
        hedge_wallet: Wallet,
        order_qty: Decimal,
        price: Decimal,
        leverage: Decimal,
    ) -> bool:
        """[FIX-7] Pre-Trade Risk Control (PTRC):
        Ensures BOTH wallets have sufficient available capital to:
        1. Support the maker order / hedge quantity `order_qty` plus RESERVE_USDC buffer.
        2. Preserve sufficient free capital for dust cleanup if either wallet holds unclosable dust (0 < |P| < min_qty).
        If either wallet lacks sufficient capital, returns False to prevent firing the maker order.
        """
        try:
            bal_a, bal_b = await self.balances(max_age=2.5)
            avail_a = dec(bal_a.data.available_for_trade)
            avail_b = dec(bal_b.data.available_for_trade)
        except Exception as exc:
            logger.error("[%s] can_afford_maker: balances read failed: %s", spec.name, exc)
            return False

        pos_a, pos_b = await self.positions_fast(spec.name, max_stale=2.0)
        if pos_a is None or pos_b is None:
            pos_a, pos_b = await self.rest_positions(spec.name)

        # Capital required for the order itself (includes 5% buffer + fee)
        req_cap_order = self.margin_required(spec, order_qty, price, leverage)

        # 1. Check dust cleanup reserve ONLY if a wallet actually holds unclosable dust (0 < |P| < min_qty)
        if _ZERO < abs(pos_a) < spec.min_qty:
            req_dust_qty_a = abs(pos_a) + spec.min_qty
            req_cap_dust_a = self.margin_required(spec, req_dust_qty_a, price, leverage)
            if avail_a < req_cap_dust_a:
                logger.warning(
                    "[%s] PTRC REJECT: Wallet A capital %s < required %s to buy |P|+min_qty (%s) for dust cleanup.",
                    spec.name, fmt_money(avail_a), fmt_money(req_cap_dust_a), req_dust_qty_a
                )
                return False

        if _ZERO < abs(pos_b) < spec.min_qty:
            req_dust_qty_b = abs(pos_b) + spec.min_qty
            req_cap_dust_b = self.margin_required(spec, req_dust_qty_b, price, leverage)
            if avail_b < req_cap_dust_b:
                logger.warning(
                    "[%s] PTRC REJECT: Wallet B capital %s < required %s to buy |P|+min_qty (%s) for dust cleanup.",
                    spec.name, fmt_money(avail_b), fmt_money(req_cap_dust_b), req_dust_qty_b
                )
                return False

        # 2. Each wallet must afford the order margin plus reserve
        total_needed_a = req_cap_order + RESERVE_USDC
        total_needed_b = req_cap_order + RESERVE_USDC

        if avail_a < total_needed_a:
            logger.warning(
                "[%s] PTRC REJECT: Wallet A capital %s < total needed %s (order=%s, reserve=%s).",
                spec.name, fmt_money(avail_a), fmt_money(total_needed_a),
                fmt_money(req_cap_order), fmt_money(RESERVE_USDC)
            )
            return False

        if avail_b < total_needed_b:
            logger.warning(
                "[%s] PTRC REJECT: Wallet B capital %s < total needed %s (order=%s, reserve=%s).",
                spec.name, fmt_money(avail_b), fmt_money(total_needed_b),
                fmt_money(req_cap_order), fmt_money(RESERVE_USDC)
            )
            return False

        return True


    # ---------------- ledger ----------------
    # [V14-3] Deleted with this block: other_wallet(), register_resting(),
    # cancel_market_orders_for() and the self._resting dict. All were v11
    # self-match machinery. v12 removed the only caller when it stopped leaving
    # an anchor resting during the cross; they have been dead code since.

    def note_entry(self, market: str, res, mid: Decimal) -> None:
        self._note(market, "ENTRY", res, mid)

    def note_exit(self, market: str, res, mid: Decimal) -> None:
        self._note(market, "EXIT", res, mid)

    def _note(self, market: str, phase: str, res, mid: Decimal) -> None:
        """[V12] Record one anchor execution.

        [V12-F1] capture comes from ExecResult.capture, which now signs by
        anchor_side. v11 always used (hedge - anchor), backwards for a SELL
        anchor and therefore wrong on roughly half of all cycles.
        [V12-F7] maker_legs and taker_legs both count LEGS now. v11 counted
        executions on one side and orders on the other, so the ratio compared
        unlike things.
        """
        if not res.traded:
            return
        notional = (res.anchor_filled + res.hedged) * mid
        # Note: self.executed_volume, maker_legs, and fees are authoritatively tracked per-fill in record_trade().
        # Do not add notional or legs here to prevent double-counting!
        capture = res.capture
        self.gross_capture += capture
        bps = (capture / notional * _TEN_THOUSAND) if notional > 0 else _ZERO
        logger.info("[%s] %s %s anchor_vwap=%s hedge_vwap=%s filled=%s hedged=%s "
                    "capture=%s fee=%s on %s (%.3f bps)",
                    market, phase,
                    res.anchor_side.value if res.anchor_side else "?",
                    res.vwap_anchor, res.vwap_hedge, res.anchor_filled, res.hedged,
                    fmt_money(capture), fmt_money(res.taker_fee_paid),
                    fmt_money(notional), bps)

    def record_trade(
        self,
        wallet_label: str,
        market: str,
        side: OrderSide,
        qty: Decimal,
        price: Decimal,
        fee: Decimal,
        is_maker: bool,
        tag: str = "",
        record_cash_flow: bool = True,
    ) -> None:
        """Institutional trade recording engine with exact cash-flow, volume, and fee attribution."""
        if qty <= _ZERO or price <= _ZERO:
            return

        notional = qty * price
        wl = str(wallet_label).upper()
        label = "B" if wl.endswith("_B") or wl.endswith(" B") or wl == "B" or "WALLET_B" in wl else "A"

        if label == "A":
            self.trade_volume_a += notional
            self.fees_a += fee
            if is_maker:
                self.maker_volume_a += notional
                self.maker_legs_a += 1
            else:
                self.taker_volume_a += notional
                self.taker_legs_a += 1
            if record_cash_flow:
                if side == OrderSide.BUY:
                    self.cash_flow_a -= notional
                else:
                    self.cash_flow_a += notional
        else:
            self.trade_volume_b += notional
            self.fees_b += fee
            if is_maker:
                self.maker_volume_b += notional
                self.maker_legs_b += 1
            else:
                self.taker_volume_b += notional
                self.taker_legs_b += 1
            if record_cash_flow:
                if side == OrderSide.BUY:
                    self.cash_flow_b -= notional
                else:
                    self.cash_flow_b += notional

        self.executed_volume = self.trade_volume_a + self.trade_volume_b
        self.taker_cost = self.fees_a + self.fees_b
        self.maker_legs = self.maker_legs_a + self.maker_legs_b
        self.taker_legs = self.taker_legs_a + self.taker_legs_b

    def get_trade_pnl(self, pos_override: dict[str, tuple[Decimal, Decimal]] | None = None) -> tuple[Decimal, Decimal]:
        """Realized + Unrealized Trade PnL (Mark-to-Market).
        Trade PnL = Cash Flow (Sells - Buys) + Mark-to-Market on open positions.
        When flat, Trade PnL == Cash Flow exactly.
        """
        pnl_a = self.cash_flow_a
        pnl_b = self.cash_flow_b
        for market in self.enabled_markets:
            book = self.market_stream.books.get(market)
            mid = book.mid if book and book.mid > _ZERO else _ZERO
            if mid > _ZERO:
                if pos_override is not None and market in pos_override:
                    pos_a, pos_b = pos_override[market]
                else:
                    pos_a = self.wallet_a.stream.signed_position_ws(market)
                    pos_b = self.wallet_b.stream.signed_position_ws(market)
                if pos_a is not None and pos_a != _ZERO:
                    pnl_a += pos_a * mid
                if pos_b is not None and pos_b != _ZERO:
                    pnl_b += pos_b * mid
        return pnl_a, pnl_b

    async def report_ledger(self, market: str) -> None:
        pos_map = {}
        for m in self.enabled_markets:
            try:
                pa, pb = await self.rest_positions(m)
                pos_map[m] = (pa, pb)
            except Exception:
                pass

        pnl_a, pnl_b = self.get_trade_pnl(pos_override=pos_map if pos_map else None)
        pnl_diff = pnl_a - pnl_b
        combined_trade_pnl = pnl_a + pnl_b

        vol_a = self.trade_volume_a
        vol_b = self.trade_volume_b
        total_vol = self.executed_volume

        pnl_bps_a = (pnl_a / vol_a * _TEN_THOUSAND) if vol_a > _ZERO else _ZERO
        pnl_bps_b = (pnl_b / vol_b * _TEN_THOUSAND) if vol_b > _ZERO else _ZERO
        comb_trade_bps = (combined_trade_pnl / total_vol * _TEN_THOUSAND) if total_vol > _ZERO else _ZERO

        fee_a = self.fees_a
        fee_b = self.fees_b
        total_fees = fee_a + fee_b

        fee_bps_a = (fee_a / vol_a * _TEN_THOUSAND) if vol_a > _ZERO else _ZERO
        fee_bps_b = (fee_b / vol_b * _TEN_THOUSAND) if vol_b > _ZERO else _ZERO
        total_fee_bps = (total_fees / total_vol * _TEN_THOUSAND) if total_vol > _ZERO else _ZERO

        whole_net_pnl = combined_trade_pnl - total_fees
        whole_net_bps = (whole_net_pnl / total_vol * _TEN_THOUSAND) if total_vol > _ZERO else _ZERO

        tot_legs_a = self.maker_legs_a + self.taker_legs_a
        tot_legs_b = self.maker_legs_b + self.taker_legs_b
        tot_legs = tot_legs_a + tot_legs_b

        maker_pct_a = (Decimal(self.maker_legs_a) / Decimal(tot_legs_a) * Decimal("100")) if tot_legs_a > 0 else _ZERO
        maker_pct_b = (Decimal(self.maker_legs_b) / Decimal(tot_legs_b) * Decimal("100")) if tot_legs_b > 0 else _ZERO
        net_maker_pct = (Decimal(self.maker_legs) / Decimal(tot_legs) * Decimal("100")) if tot_legs > 0 else _ZERO

        maker_vol_pct_a = (self.maker_volume_a / vol_a * Decimal("100")) if vol_a > _ZERO else _ZERO
        maker_vol_pct_b = (self.maker_volume_b / vol_b * Decimal("100")) if vol_b > _ZERO else _ZERO
        total_maker_vol = self.maker_volume_a + self.maker_volume_b
        net_maker_vol_pct = (total_maker_vol / total_vol * Decimal("100")) if total_vol > _ZERO else _ZERO

        eq_lines = []
        try:
            bal_a, bal_b = await self.balances()
            curr_eq_a = dec(bal_a.data.equity)
            curr_eq_b = dec(bal_b.data.equity)
            self.last_equity_a = curr_eq_a
            self.last_equity_b = curr_eq_b
            init_a = self.initial_equity_a if self.initial_equity_a is not None else curr_eq_a
            init_b = self.initial_equity_b if self.initial_equity_b is not None else curr_eq_b
            delta_eq_a = curr_eq_a - init_a
            delta_eq_b = curr_eq_b - init_b
            tot_delta_eq = delta_eq_a + delta_eq_b
            discrepancy = tot_delta_eq - whole_net_pnl
            auth_gross_pnl = tot_delta_eq + total_fees
            auth_gross_bps = (auth_gross_pnl / total_vol * _TEN_THOUSAND) if total_vol > _ZERO else _ZERO
            auth_net_bps = (tot_delta_eq / total_vol * _TEN_THOUSAND) if total_vol > _ZERO else _ZERO
            status_str = "HEALTHY (MATCH)" if abs(discrepancy) < Decimal("0.05") else "DESYNC DETECTED"
            eq_lines = [
                f"   * Wallet A Clearing Equity: {fmt_money(curr_eq_a)} (Delta {fmt_signed_money(delta_eq_a)})",
                f"   * Wallet B Clearing Equity: {fmt_money(curr_eq_b)} (Delta {fmt_signed_money(delta_eq_b)})",
                f"   * Total Account Equity Delta: {fmt_signed_money(tot_delta_eq)} ({auth_net_bps:+.3f} bps)",
                f"   * Authoritative Gross Trade PnL: {fmt_signed_money(auth_gross_pnl)} ({auth_gross_bps:+.3f} bps) [Equation: Delta Eq + Total Fees]",
                f"   * Ledger Discrepancy vs Clearing: {fmt_signed_money(discrepancy)} [{status_str}]",
            ]
        except Exception as exc:
            eq_lines = [f"   * Clearing Equity Read Error: {exc}"]

        logger.info(
            "\n" + "=" * 85 +
            f"\nLEDGER REPORT [{market}] | Pairs: {self.completed_pairs} | Unwound: {self.pairs_unwound} | Total Volume: {fmt_money(total_vol)}" +
            "\n" + "-" * 85 +
            f"\n1. TRADE PNL (Gross Trading Results excluding fees):" +
            f"\n   * Wallet A Trade PnL      : {fmt_signed_money(pnl_a)} ({pnl_bps_a:+.3f} bps on {fmt_money(vol_a)} vol)" +
            f"\n   * Wallet B Trade PnL      : {fmt_signed_money(pnl_b)} ({pnl_bps_b:+.3f} bps on {fmt_money(vol_b)} vol)" +
            f"\n   * Net Trade PnL (A - B)   : {fmt_signed_money(pnl_diff)}" +
            f"\n   * Combined Net Trade PnL  : {fmt_signed_money(combined_trade_pnl)} ({comb_trade_bps:+.3f} bps)" +
            "\n" + "-" * 85 +
            f"\n2. MAKER FILL RATIO & EXECUTION QUALITY:" +
            f"\n   * Wallet A Maker Fills    : {maker_pct_a:.1f}% ({self.maker_legs_a}/{tot_legs_a} legs) | Volume: {maker_vol_pct_a:.1f}% ({fmt_money(self.maker_volume_a)}/{fmt_money(vol_a)})" +
            f"\n   * Wallet B Maker Fills    : {maker_pct_b:.1f}% ({self.maker_legs_b}/{tot_legs_b} legs) | Volume: {maker_vol_pct_b:.1f}% ({fmt_money(self.maker_volume_b)}/{fmt_money(vol_b)})" +
            f"\n   * Net Combined Maker Fills: {net_maker_pct:.1f}% ({self.maker_legs}/{tot_legs} legs) | Net Maker Vol: {net_maker_vol_pct:.1f}% ({fmt_money(total_maker_vol)}/{fmt_money(total_vol)})" +
            "\n" + "-" * 85 +
            f"\n3. BPS LOST TRACKER (Fees Paid):" +
            f"\n   * Wallet A Fees Lost      :  {fmt_money(fee_a)} ({fee_bps_a:.3f} bps) [Maker: {self.maker_legs_a} | Taker: {self.taker_legs_a}]" +
            f"\n   * Wallet B Fees Lost      :  {fmt_money(fee_b)} ({fee_bps_b:.3f} bps) [Maker: {self.maker_legs_b} | Taker: {self.taker_legs_b}]" +
            f"\n   * Total Net Fees Lost     :  {fmt_money(total_fees)} ({total_fee_bps:.3f} bps) [Total Taker Legs: {self.taker_legs}]" +
            "\n" + "-" * 85 +
            f"\n4. WHOLE NET PNL (Combined Trade PnL - Total Fees Lost):" +
            f"\n   * Whole Net PnL           : {fmt_signed_money(whole_net_pnl)} ({whole_net_bps:+.3f} bps of total volume)" +
            f"\n   * Equation Breakdown     : ({comb_trade_bps:+.3f} Trade bps) - ({total_fee_bps:.3f} Fee bps) = {whole_net_bps:+.3f} Whole Net bps" +
            "\n" + "-" * 85 +
            f"\n5. ON-CHAIN CLEARING RECONCILIATION:" +
            "\n" + "\n".join(eq_lines) +
            "\n" + "=" * 85
        )

    # ---------------- scheduler [V7-8] ----------------
    def _selectable_markets(self) -> list:
        now = time.monotonic()
        return [
            m for m in self.enabled_markets
            if m not in self.active                       # never a 2nd slice in the same book
            and self.cooldown_until.get(m, 0.0) <= now    # respect post-exit cooldown
        ]

    async def _launch(self, market: str) -> None:
        spec = self.specs[market]
        bal_a, bal_b = await self.balances()
        margin = self.choose_margin(bal_a, bal_b)
        if margin <= 0:
            logger.info("No usable margin; skipping launch of %s.", market)
            return

        book = self.market_stream.books.get(market)
        affordable_levs = list(spec.lev_choices)
        if book and book.mid > _ZERO:
            affordable_levs = [
                l for l in spec.lev_choices
                if margin >= self.margin_required(spec, spec.min_qty, book.mid, Decimal(str(l)))
            ]
            if not affordable_levs:
                max_lev = max(spec.lev_choices)
                min_req = self.margin_required(spec, spec.min_qty, book.mid, Decimal(str(max_lev)))
                logger.info("[%s] Sized margin %s < min_margin %s for min_qty %s even at max lev %sx. Skipping launch.",
                            market, fmt_money(margin), fmt_money(min_req), spec.min_qty, max_lev)
                self.cooldown_until[market] = time.monotonic() + 15.0
                return

        leverage = Decimal(str(random.choice(affordable_levs)))

        # Cancel market orders and ensure flat before updating leverage to eliminate error 1052
        await self.cancel_market_orders(market)
        if not await self.ensure_flat(spec):
            logger.warning("[%s] ensure_flat before leverage update failed. Skipping launch.", market)
            self.cooldown_until[market] = time.monotonic() + 15.0
            return

        try:
            await asyncio.gather(
                self.wallet_a.rest.account.update_leverage(market, leverage),
                self.wallet_b.rest.account.update_leverage(market, leverage),
            )
        except Exception as exc:
            logger.warning("[%s] update_leverage failed: %s", market, exc)
            self.cooldown_until[market] = time.monotonic() + 15.0
            return

        worker = MarketWorker(self, spec, leverage, margin)
        self.active[market] = worker
        self.tasks[market] = asyncio.create_task(worker.run(), name=f"worker-{market}")
        self.rotation_count += 1
        logger.info("LAUNCH [%s] lev=%sx margin=%s | active=%s slot %s/%s",
                    market, leverage, fmt_money(margin),
                    list(self.active), len(self.active), MAX_CONCURRENT_MARKETS)

    async def _reap(self) -> None:
        for market, task in list(self.tasks.items()):
            if not task.done():
                continue
            try:
                task.result()
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.error("[%s] worker task raised: %s", market, exc)
            self.tasks.pop(market, None)
            self.active.pop(market, None)
            # Stochastic cooldown: jitter between 80% and 150% of MARKET_COOLDOWN_SEC
            # to break deterministic periodicity across cycles
            cd = random.uniform(MARKET_COOLDOWN_SEC * 0.8, MARKET_COOLDOWN_SEC * 1.5)
            self.cooldown_until[market] = time.monotonic() + cd
            logger.info("REAPED [%s]. active=%s cooldown %.1fs",
                        market, list(self.active), cd)

    async def scheduler_loop(self) -> None:
        while not self.draining and not self.stop_event.is_set():
            try:
                await self._reap()
                if len(self.active) < MAX_CONCURRENT_MARKETS:
                    if not self.funding_safe():
                        logger.debug("Funding guard active; not launching.")
                    else:
                        candidates = self._selectable_markets()
                        if candidates:
                            await self._launch(random.choice(candidates))
                        elif not self.active:
                            logger.debug("All markets in cooldown; waiting.")
            except Exception as exc:
                logger.exception("Scheduler error: %s", exc)
            try:
                await asyncio.wait_for(self.drain_event.wait(), SCHEDULER_TICK_SEC)
            except asyncio.TimeoutError:
                pass

    async def status_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                parts = []
                for market in self.enabled_markets:
                    book = self.market_stream.books.get(market)
                    worker = self.active.get(market)
                    st = worker.state if worker else "idle"
                    if book:
                        parts.append(f"{market}[{st} {book.spread_bps:.3f}bps age{book.age():.1f}s]")
                    else:
                        parts.append(f"{market}[{st} nobook]")
                pnl_a, pnl_b = self.get_trade_pnl()
                pnl_diff = pnl_a - pnl_b
                comb_trade = pnl_a + pnl_b
                total_fees = self.fees_a + self.fees_b
                whole_net = comb_trade - total_fees
                net_bps = (whole_net / self.executed_volume * Decimal("10000")) if self.executed_volume > 0 else _ZERO
                tot_legs = self.maker_legs + self.taker_legs
                net_maker_pct = (Decimal(self.maker_legs) / Decimal(tot_legs) * Decimal("100")) if tot_legs > 0 else _ZERO
                tot_a = self.maker_legs_a + self.taker_legs_a
                tot_b = self.maker_legs_b + self.taker_legs_b
                logger.info("STATUS %s | vol=%s (A=%s B=%s) | Maker: %.1f%% (A=%d/%d B=%d/%d) | TradePnL: A=%s B=%s Net=%s (Diff=%s) | Fees: A=%s B=%s Net=%s | WHOLE NET: %s (%.3f bps)",
                            " ".join(parts), fmt_money(self.executed_volume), fmt_money(self.trade_volume_a), fmt_money(self.trade_volume_b),
                            net_maker_pct, self.maker_legs_a, tot_a, self.maker_legs_b, tot_b,
                            fmt_signed_money(pnl_a), fmt_signed_money(pnl_b), fmt_signed_money(comb_trade), fmt_signed_money(pnl_diff),
                            fmt_money(self.fees_a), fmt_money(self.fees_b), fmt_money(total_fees),
                            fmt_signed_money(whole_net), net_bps)
            except Exception as exc:
                logger.debug("status error: %s", exc)
            try:
                await asyncio.wait_for(self.stop_event.wait(), STATUS_INTERVAL_SEC)
            except asyncio.TimeoutError:
                pass

    # ---------------- lifecycle ----------------
    async def run(self) -> None:
        status_task = None
        sched_task = None
        try:
            await self.setup()
            status_task = asyncio.create_task(self.status_loop(), name="status")
            sched_task = asyncio.create_task(self.scheduler_loop(), name="scheduler")

            while not self.draining and not self.stop_event.is_set():
                try:
                    await asyncio.wait_for(self.drain_event.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
                except KeyboardInterrupt:
                    self.request_stop()
                    break

            # If graceful drain was initiated (and not forced)
            if self.draining and not self.stop_event.is_set():
                logger.warning("CTRL+C DETECTED: Halting scheduler and immediately exiting all open positions via defined strategy...")
                if sched_task is not None and not sched_task.done():
                    sched_task.cancel()
                    try:
                        await sched_task
                    except asyncio.CancelledError:
                        pass
                    sched_task = None

                drain_timeout = float(EXIT_TIMEOUT_SEC * 2 + 30)
                drain_deadline = time.monotonic() + drain_timeout

                while self.tasks and time.monotonic() < drain_deadline and not self.stop_event.is_set():
                    try:
                        await self._reap()
                        if not self.tasks:
                            break
                        active_markets = list(self.tasks.keys())
                        logger.info("Strategy exit in progress: %d market(s) exiting (%s)... %.0fs until forced fallback",
                                    len(self.tasks), active_markets, max(0.0, drain_deadline - time.monotonic()))
                        try:
                            await asyncio.wait_for(self.stop_event.wait(), 1.0)
                        except asyncio.TimeoutError:
                            pass
                    except KeyboardInterrupt:
                        self.request_stop(force=True)
                        break

                if self.tasks:
                    if self.stop_event.is_set():
                        logger.warning("Force abort triggered. Cancelling remaining %d active worker(s).", len(self.tasks))
                    else:
                        logger.warning("Drain timeout (%.0fs) reached. Cancelling remaining %d active worker(s).",
                                       drain_timeout, len(self.tasks))
                    for market, task in list(self.tasks.items()):
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*self.tasks.values(), return_exceptions=True)
                else:
                    logger.info("All active worker positions exited cleanly via strategy.")

                # Authoritative sweep: ensure ANY open positions across all enabled markets exit via strategy
                for name in self.enabled_markets:
                    try:
                        spec = self.specs.get(name)
                        if not spec:
                            continue
                        pos_a, pos_b = await self.rest_positions(name)
                        if abs(pos_a) > MAX_HEDGE_MISMATCH_QTY or abs(pos_b) > MAX_HEDGE_MISMATCH_QTY:
                            if not self.stop_event.is_set():
                                logger.info("[%s] Open position detected on shutdown (A=%s, B=%s) -> executing strategy maker exit...",
                                            name, pos_a, pos_b)
                                if not await self.strategy_anchor_exit(spec):
                                    logger.warning("[%s] Strategy exit could not clear position -- running emergency flatten.", name)
                                    await self.emergency_flatten(spec)
                            else:
                                await self.emergency_flatten(spec)
                    except Exception as exc:
                        logger.error("[%s] Shutdown exit error: %s", name, exc)

        except KeyboardInterrupt:
            self.request_stop(force=True)
        finally:
            for t in (sched_task, status_task):
                if t is not None and not t.done():
                    t.cancel()
            for market, task in list(self.tasks.items()):
                if not task.done():
                    task.cancel()
            pending = [t for t in (sched_task, status_task) if t is not None and not t.done()]
            pending += [t for t in self.tasks.values() if not t.done()]
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            await self.shutdown()

    async def shutdown(self) -> None:
        if self.setup_complete:
            logger.warning("Final shutdown cleanup: checking final position states across all enabled markets")
            for name in self.enabled_markets:
                try:
                    await self.cancel_market_orders(name)
                except Exception:
                    pass
            for name in self.enabled_markets:
                try:
                    spec = self.specs.get(name)
                    if spec:
                        pos_a, pos_b = await self.rest_positions(name)
                        if abs(pos_a) > MAX_HEDGE_MISMATCH_QTY or abs(pos_b) > MAX_HEDGE_MISMATCH_QTY:
                            logger.warning("[%s] Residual position detected at shutdown (A=%s B=%s). Running emergency flatten.",
                                           name, pos_a, pos_b)
                            if not await self.emergency_flatten(spec):
                                logger.critical("Could not guarantee flat state for %s", name)
                        else:
                            logger.info("[%s] Position verified flat at shutdown (A=%s B=%s). No flatten needed.",
                                        name, pos_a, pos_b)
                except Exception as exc:
                    logger.critical("Shutdown flatten exception for %s: %s", name, exc)
        else:
            logger.warning("Shutting down: setup incomplete, skipping cancel/flatten")

        if self.executed_volume > 0:
            await self.report_ledger("FINAL-SHUTDOWN")

        async def _safe_close(resource, name: str) -> None:
            if resource is None:
                return
            try:
                await resource.close()
            except Exception as exc:
                logger.debug("Error closing %s: %s", name, exc)

        await asyncio.gather(
            _safe_close(self.wallet_a.rest, "wallet_a.rest"),
            _safe_close(self.wallet_b.rest, "wallet_b.rest"),
            _safe_close(self.market_stream, "market_stream"),
            _safe_close(self.wallet_a.stream, "wallet_a.stream"),
            _safe_close(self.wallet_b.stream, "wallet_b.stream"),
        )


async def main() -> None:
    bot = DeltaNeutralProductionBot()

    def signal_handler(sig=None, frame=None) -> None:
        bot.request_stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, signal_handler)
        except (ValueError, AttributeError):
            pass

    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, signal_handler)
            except NotImplementedError:
                pass
    except Exception:
        pass

    try:
        await bot.run()
    except (KeyboardInterrupt, asyncio.CancelledError):
        if not bot.draining:
            bot.request_stop()
        else:
            bot.request_stop(force=True)


if __name__ == "__main__":
    asyncio.run(main())