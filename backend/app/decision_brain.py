"""Timeframe-aware decision brain for the read-only trader terminal.

The brain is deliberately evidence-first: scores are point counts, never
probabilities.  Fifteen-minute context, five-minute direction/location and a
one-minute setup must exist before one-second execution evidence can confirm a
setup.  Missing evidence is represented as N/A/WAITING rather than inferred.
"""

from __future__ import annotations

from collections import deque
from math import isfinite
from typing import Any


def number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if isfinite(result) else None


def last_value(rows: list[dict[str, Any]], key: str) -> float | None:
    for row in reversed(rows):
        value = number(row.get(key))
        if value is not None:
            return value
    return None


def structure(bars: list[dict[str, Any]]) -> dict[str, Any]:
    """Return closed-candle HH/HL/LH/LL, BOS and CHOCH evidence."""
    rows = [row for row in bars if row.get("closed", True)]
    if len(rows) < 7:
        return {"status": "INSUFFICIENT DATA", "label": "N/A", "events": [], "last_bos": "N/A", "last_choch": "N/A"}
    highs: list[tuple[int, float]] = []
    lows: list[tuple[int, float]] = []
    for index in range(2, len(rows) - 2):
        high = number(rows[index].get("high")); low = number(rows[index].get("low"))
        if high is not None and high >= max(number(rows[c].get("high")) or high for c in range(index - 2, index + 3)):
            highs.append((index, high))
        if low is not None and low <= min(number(rows[c].get("low")) or low for c in range(index - 2, index + 3)):
            lows.append((index, low))
    events: list[dict[str, Any]] = []
    high_label = low_label = None
    if len(highs) >= 2:
        high_label = "HH" if highs[-1][1] > highs[-2][1] else "LH"
        events.append({"type": high_label, "price": highs[-1][1], "time": rows[highs[-1][0]].get("time")})
    if len(lows) >= 2:
        low_label = "HL" if lows[-1][1] > lows[-2][1] else "LL"
        events.append({"type": low_label, "price": lows[-1][1], "time": rows[lows[-1][0]].get("time")})
    label = "BULLISH" if high_label == "HH" and low_label == "HL" else "BEARISH" if high_label == "LH" and low_label == "LL" else "RANGE"
    close = number(rows[-1].get("close"))
    bos_direction = None
    if close is not None and highs and close > highs[-1][1]:
        bos_direction = "UP"
    elif close is not None and lows and close < lows[-1][1]:
        bos_direction = "DOWN"
    last_bos = "N/A"; last_choch = "N/A"
    if bos_direction:
        change = (label == "BEARISH" and bos_direction == "UP") or (label == "BULLISH" and bos_direction == "DOWN")
        event_type = "CHOCH" if change else "BOS"
        event = {"type": event_type, "direction": bos_direction, "price": highs[-1][1] if bos_direction == "UP" else lows[-1][1], "time": rows[-1].get("time")}
        events.append(event)
        if event_type == "BOS":
            last_bos = f"{event_type}_{bos_direction}"
        else:
            last_choch = f"{event_type}_{bos_direction}"
    range_high = highs[-1][1] if highs else max(number(row.get("high")) or 0 for row in rows[-20:])
    range_low = lows[-1][1] if lows else min(number(row.get("low")) or 0 for row in rows[-20:])
    return {
        "status": "OBSERVED_DERIVED", "label": label, "high_label": high_label or "N/A", "low_label": low_label or "N/A",
        "events": events[-12:], "last_bos": last_bos, "last_choch": last_choch,
        "swing_high": highs[-1][1] if highs else None, "swing_low": lows[-1][1] if lows else None,
        "previous_high": max((item[1] for item in highs[:-1]), default=None), "previous_low": min((item[1] for item in lows[:-1]), default=None),
        "range": {"low": range_low, "high": range_high, "equilibrium": (range_low + range_high) / 2},
    }


def evidence(name: str, value: Any, source: str, contribution: int = 1) -> dict[str, Any]:
    return {"name": name, "value": value, "source": source, "contribution": contribution}


class DecisionBrain:
    def __init__(self, min_rr: float = 1.5) -> None:
        self.min_rr = min_rr
        self.active_position: dict[str, Any] | None = None
        self.last_decision: str | None = None

    @staticmethod
    def _location(price: float | None, five: dict[str, Any], indicator: dict[str, Any], profiles: dict[str, Any]) -> dict[str, Any]:
        if price is None or five.get("status") != "OBSERVED_DERIVED":
            return {"status": "INSUFFICIENT DATA", "label": "N/A", "long_ok": False, "short_ok": False, "evidence": []}
        dealing = five.get("range") or {}; low = number(dealing.get("low")); high = number(dealing.get("high")); eq = number(dealing.get("equilibrium"))
        atr_value = number(indicator.get("atr"))
        if atr_value is None:
            atr_value = abs(high - low) * 0.1 if high is not None and low is not None else price * 0.001
        tolerance = max(atr_value * 0.6, price * 0.001)
        vwap = number(indicator.get("vwap")); val = number(profiles.get("val")); vah = number(profiles.get("vah"))
        hvn = [number(row.get("price")) for row in profiles.get("hvn", []) if number(row.get("price")) is not None]
        near_val = val is not None and abs(price - val) <= tolerance
        near_vah = vah is not None and abs(price - vah) <= tolerance
        near_vwap = vwap is not None and abs(price - vwap) <= tolerance
        support_hvn = any(item <= price and abs(price - item) <= tolerance for item in hvn)
        resistance_hvn = any(item >= price and abs(price - item) <= tolerance for item in hvn)
        discount = eq is not None and price <= eq
        premium = eq is not None and price >= eq
        long_labels = []
        short_labels = []
        if discount: long_labels.append("DISCOUNT")
        if premium: short_labels.append("PREMIUM")
        if near_val: long_labels.append("VAL")
        if near_vah: short_labels.append("VAH")
        if near_vwap and price >= vwap: long_labels.append("VWAP_SUPPORT/RECLAIM")
        if near_vwap and price <= vwap: short_labels.append("VWAP_RESISTANCE/REJECTION")
        if support_hvn: long_labels.append("HVN_SUPPORT")
        if resistance_hvn: short_labels.append("HVN_RESISTANCE")
        return {
            "status": "OBSERVED_DERIVED", "label": "/".join(long_labels if len(long_labels) >= len(short_labels) else short_labels) or "MID-RANGE",
            "long_ok": bool(long_labels), "short_ok": bool(short_labels), "long_reasons": long_labels, "short_reasons": short_labels,
            "value_area": {"val": val, "vah": vah}, "vwap": vwap, "range": dealing,
            "evidence": [evidence("5M dealing range", dealing, "5M closed candles"), evidence("1M value area", {"VAL": val, "VAH": vah}, "Executed-flow volume profile"), evidence("VWAP", vwap, "Futures trade/candle volume")],
        }

    @staticmethod
    def _micro(state: Any, intel: dict[str, Any], price: float | None, timestamp: int) -> dict[str, Any]:
        engine = state.intelligence
        aggression = engine.aggression(timestamp) if engine else {}
        one = aggression.get("1000ms", {})
        recent = [item for item in list(getattr(state, "trades", [])) if timestamp - int(item.get("received_time", timestamp)) <= 250]
        recent_buy = sum(number(item.get("notional")) or 0 for item in recent if item.get("side") == "BUY")
        recent_sell = sum(number(item.get("notional")) or 0 for item in recent if item.get("side") == "SELL")
        trades_1s = [item for item in list(getattr(state, "trades", [])) if timestamp - int(item.get("received_time", timestamp)) <= 1000]
        price_start = number(trades_1s[-1].get("price")) if trades_1s else price
        price_end = number(trades_1s[0].get("price")) if trades_1s else price
        price_change = (price_end - price_start) if price_start is not None and price_end is not None else None
        book = state.orderbook.metrics() if state.orderbook else {"valid": False}
        microprice = number(book.get("microprice")); mid = number(book.get("mid")) or price
        micro_bias = microprice - mid if microprice is not None and mid is not None else None
        detectors = intel.get("detectors") or {}
        absorption = detectors.get("absorption") or {}
        replenishment = detectors.get("replenishment") or {}
        long_absorption = number(absorption.get("buy_absorption_score")) or 0
        short_absorption = number(absorption.get("sell_absorption_score")) or 0
        bid_replenish = number(replenishment.get("bid_score")) or 0
        ask_replenish = number(replenishment.get("ask_score")) or 0
        long_sell_absorption = bool(one) and one.get("sell_volume", 0) > one.get("buy_volume", 0) and (long_absorption >= 45 or bid_replenish >= 45) and (price_change is None or price_change >= 0)
        short_buy_absorption = bool(one) and one.get("buy_volume", 0) > one.get("sell_volume", 0) and (short_absorption >= 45 or ask_replenish >= 45) and (price_change is None or price_change <= 0)
        long_confirmed = long_sell_absorption and (recent_buy > recent_sell or (micro_bias is not None and micro_bias > 0)) and (number(book.get("spread_bps")) is None or number(book.get("spread_bps")) <= 10)
        short_confirmed = short_buy_absorption and (recent_sell > recent_buy or (micro_bias is not None and micro_bias < 0)) and (number(book.get("spread_bps")) is None or number(book.get("spread_bps")) <= 10)
        if not trades_1s:
            status = "WAITING FOR DATA"; side = None
        elif long_confirmed and not short_confirmed:
            status = "CONFIRMED"; side = "LONG"
        elif short_confirmed and not long_confirmed:
            status = "CONFIRMED"; side = "SHORT"
        else:
            status = "NOT CONFIRMED"; side = None
        return {
            "status": status, "side": side, "window": "1S", "buy_aggression": one.get("buy_notional"), "sell_aggression": one.get("sell_notional"),
            "delta": one.get("net_notional"), "recent_delta": recent_buy - recent_sell, "price_change": price_change,
            "microprice": microprice, "microprice_bias": micro_bias, "spread_bps": book.get("spread_bps"),
            "bid_replenishment": bid_replenish, "ask_replenishment": ask_replenish, "bid_absorption": long_absorption, "ask_absorption": short_absorption,
            "sell_absorption": long_sell_absorption, "buy_absorption": short_buy_absorption,
            "delta_flip": recent_buy > recent_sell if long_sell_absorption else recent_sell > recent_buy if short_buy_absorption else False,
            "evidence": [evidence("SELL absorption / bid replenishment", long_sell_absorption, "1S trades + bid lifecycle"), evidence("BUY absorption / ask replenishment", short_buy_absorption, "1S trades + ask lifecycle"), evidence("Delta flip", recent_buy - recent_sell, "1S trade executions"), evidence("Micro-price", microprice, "Top-of-book"), evidence("Spread", book.get("spread_bps"), "Best bid/ask")],
        }

    @staticmethod
    def _liquidations(state: Any, price: float | None, structures: dict[str, dict[str, Any]], indicators: dict[str, Any], location: dict[str, Any]) -> dict[str, Any]:
        if price is None or number(getattr(state, "open_interest", None)) is None:
            return {"status": "INSUFFICIENT DATA", "observed": list(getattr(state, "liquidations", []))[:30], "estimated": [], "methodology": "OI unavailable"}
        oi = number(state.open_interest) or 0; oi_delta = number(getattr(state, "oi_delta", None)); funding = number(state.funding_rate)
        ratio = state.ratio or {}; volume = number(indicators.get("volume_total"))
        observed = list(getattr(state, "liquidations", []))[:50]
        estimate_inputs = ["open_interest"]
        if oi_delta is not None: estimate_inputs.append("open_interest_change")
        if volume is not None: estimate_inputs.append("futures_volume")
        if funding is not None: estimate_inputs.append("funding")
        if ratio: estimate_inputs.append("long_short_positioning")
        rows: list[dict[str, Any]] = []
        for side, sign, label in (("ABOVE", 1, "SHORT_LIQUIDATION_CLUSTER"), ("BELOW", -1, "LONG_LIQUIDATION_CLUSTER")):
            struct = structures.get("5m", {})
            levels = [struct.get("swing_high") if sign > 0 else struct.get("swing_low"), struct.get("previous_high") if sign > 0 else struct.get("previous_low"), location.get("value_area", {}).get("vah") if sign > 0 else location.get("value_area", {}).get("val")]
            levels = [level for level in levels if level is not None and (level > price if sign > 0 else level < price)]
            if not levels:
                distance = price * 0.008
                levels = [price + sign * distance]
                estimate_inputs.append("leverage_distribution_prior")
            level = min(levels) if sign > 0 else max(levels)
            near_observed = sum((number(item.get("notional")) or 0) for item in observed if (number(item.get("price")) or 0) * sign > price * sign and abs((number(item.get("price")) or price) - level) <= price * 0.003)
            base_density = oi * 0.00035
            if oi_delta is not None and oi_delta * sign > 0: base_density *= 1.15
            if near_observed: base_density += near_observed / max(price, 1)
            if volume is not None: base_density *= 1 + min(volume / max(oi * price, 1), 0.25)
            positioning = number(ratio.get("longAccount"))
            if positioning is not None and ((sign < 0 and positioning > 0.5) or (sign > 0 and positioning < 0.5)): base_density *= 1.1
            confidence = min(0.95, 0.25 + 0.1 * len(set(estimate_inputs)) + (0.15 if near_observed else 0))
            cascade = min(100.0, 20 + confidence * 35 + (20 if near_observed else 0) + (10 if oi_delta is not None and oi_delta * sign > 0 else 0))
            rows.append({"price": level, "side": side, "type": label, "estimated_liquidation_density": base_density, "confidence": round(confidence, 2), "cascade_probability": round(cascade, 1), "probability_status": "HEURISTIC_ESTIMATE_NOT_PROBABILITY", "status": "ESTIMATED", "inputs": sorted(set(estimate_inputs)), "observed_notional_nearby": near_observed})
        return {"status": "ESTIMATED", "observed": observed, "estimated": rows, "methodology": "OI change + futures volume + funding + public positioning + structure + observed forceOrder proximity + leverage distribution prior; no account liquidation prices are available"}

    @staticmethod
    def _path(state: Any, price: float | None, target: dict[str, Any] | None, side: str | None) -> dict[str, Any]:
        book = state.orderbook.metrics() if state.orderbook else {"valid": False}
        if not target or price is None or side not in ("LONG", "SHORT"):
            return {"status": "INSUFFICIENT DATA", "label": "N/A", "obstacles": [], "evidence": []}
        if not book.get("valid"):
            return {"status": "WAITING FOR DATA", "label": "N/A", "obstacles": [], "evidence": []}
        rows = book.get("asks" if side == "LONG" else "bids", [])
        rows = [row for row in rows if (price < row["price"] <= target["price"] if side == "LONG" else target["price"] <= row["price"] < price)]
        rows = sorted(rows, key=lambda row: abs(row["price"] - price))
        obstacles = [{"price": row["price"], "quantity": row["quantity"], "notional": row["price"] * row["quantity"], "source": "OBSERVED_AGGREGATED_L2"} for row in rows[:12] if row["price"] * row["quantity"] > 0]
        strongest = max(obstacles, key=lambda row: row["notional"], default=None)
        label = "CLEAR" if not obstacles else "HARD" if strongest and strongest["notional"] > target.get("notional", 0) * 0.35 else "PASSABLE"
        return {"status": "OBSERVED_DERIVED", "label": label, "obstacles": obstacles, "strongest_obstacle": strongest, "evidence": [evidence("Path levels", len(obstacles), "Validated aggregated order book"), evidence("Strongest opposing liquidity", strongest, "Observed L2; order-book liquidity is not liquidation liquidity")]}

    def _targets(self, state: Any, price: float | None, side: str | None, structures: dict[str, dict[str, Any]], indicators: dict[str, Any], intel: dict[str, Any], liquidations: dict[str, Any]) -> list[dict[str, Any]]:
        if price is None or side not in ("LONG", "SHORT"):
            return []
        target_side = "ASK" if side == "LONG" else "BID"
        candidates: list[dict[str, Any]] = []
        for item in intel.get("targets", []):
            if item.get("side") == target_side and ((number(item.get("price")) or 0) > price if side == "LONG" else (number(item.get("price")) or 0) < price):
                candidates.append({"price": number(item.get("price")), "type": "ORDER_BOOK_LIQUIDITY", "source": "Lifecycle intelligence / observed aggregated L2", "notional": number(item.get("features", {}).get("notional")) or 0, "score": number(item.get("touch_score")) or 0, "path_hint": item.get("liquidity_path")})
        struct = structures.get("5m", {})
        levels = [(struct.get("swing_high") if side == "LONG" else struct.get("swing_low"), "PREVIOUS_STRUCTURE"), (struct.get("previous_high") if side == "LONG" else struct.get("previous_low"), "PREVIOUS_HIGH/LOW")]
        profile = intel.get("profiles") or {}
        levels += [(number(indicators.get("vwap")), "VWAP"), (number(profile.get("vah")) if side == "LONG" else number(profile.get("val")), "VALUE_AREA")]
        for level, kind in levels:
            if level is not None and ((level > price) if side == "LONG" else (level < price)):
                candidates.append({"price": level, "type": kind, "source": "5M structure / 1M profile", "notional": 0, "score": 35, "path_hint": None})
        for item in liquidations.get("estimated", []):
            level = number(item.get("price"))
            if level is not None and ((level > price) if side == "LONG" else level < price):
                candidates.append({"price": level, "type": item.get("type", "ESTIMATED_LIQUIDATION_CLUSTER"), "source": "Estimated liquidation density", "notional": item.get("estimated_liquidation_density", 0) * level, "score": item.get("confidence", 0) * 100, "path_hint": None, "liquidation": item})
        unique: dict[tuple[str, int], dict[str, Any]] = {}
        for item in candidates:
            if item.get("price") is not None:
                unique[(item["type"], round(item["price"], 2))] = item
        return sorted(unique.values(), key=lambda item: (-item["score"], abs(item["price"] - price)))[:12]

    def evaluate(self, state: Any, timestamp: int) -> dict[str, Any]:
        price = number(state.mark_price) or number(state.price)
        bars = {tf: list(getattr(state, "timeframe_klines", {}).get(tf, [])) for tf in ("1m", "5m", "15m")}
        structures = {tf: structure(rows) for tf, rows in bars.items()}
        intel = state.intelligence.snapshot(state.orderbook, bars["1m"], list(state.buckets.values()), price, timestamp, "1m", data_integrity_ok=bool(state.orderbook.valid and state.last_trade_at and timestamp - state.last_trade_at <= 3000)) if state.intelligence else {}
        indicators5 = state.intelligence.indicators(bars["1m"], list(state.buckets.values()), "5m") if state.intelligence else {}
        indicators1 = intel.get("indicators") or {}
        profiles = intel.get("profiles") or (state.intelligence._profiles(bars["1m"]) if state.intelligence else {})
        location = self._location(price, structures["5m"], indicators5, profiles)
        micro = self._micro(state, intel, price, timestamp)
        liquidations = self._liquidations(state, price, structures, indicators5, location)
        long_score = 0; short_score = 0; long_evidence: list[dict[str, Any]] = []; short_evidence: list[dict[str, Any]] = []
        def add(target: str, name: str, value: Any, source: str) -> None:
            nonlocal long_score, short_score
            item = evidence(name, value, source)
            if target == "LONG": long_score += 1; long_evidence.append(item)
            else: short_score += 1; short_evidence.append(item)
        one = structures["1m"]
        if one.get("label") == "BULLISH": add("LONG", "HH + HL", "BULLISH", "1M closed-candle structure")
        if one.get("label") == "BEARISH": add("SHORT", "LH + LL", "BEARISH", "1M closed-candle structure")
        if one.get("last_bos") == "BOS_UP": add("LONG", "Last BOS up", "BOS_UP", "1M closed-candle structure")
        if one.get("last_bos") == "BOS_DOWN": add("SHORT", "Last BOS down", "BOS_DOWN", "1M closed-candle structure")
        if one.get("last_choch") == "CHOCH_UP": add("LONG", "Bullish CHOCH", "CHOCH_UP", "1M closed-candle structure")
        if one.get("last_choch") == "CHOCH_DOWN": add("SHORT", "Bearish CHOCH", "CHOCH_DOWN", "1M closed-candle structure")
        current_flow = [row for row in state.buckets.values() if timestamp - int(row.get("time", timestamp)) <= 60_000]
        buy_1m = sum(number(row.get("buy")) or 0 for row in current_flow); sell_1m = sum(number(row.get("sell")) or 0 for row in current_flow); delta_1m = buy_1m - sell_1m
        one_minute_bars = bars["1m"]
        price_change_1m = None
        if len(one_minute_bars) >= 2:
            price_change_1m = (number(one_minute_bars[-1].get("close")) or 0) - (number(one_minute_bars[-2].get("close")) or 0)
        replenishment = (intel.get("detectors") or {}).get("replenishment") or {}
        bid_replenishment = number(replenishment.get("bid_score")) or 0; ask_replenishment = number(replenishment.get("ask_score")) or 0
        bid_absorption_1m = sell_1m > buy_1m and (price_change_1m is None or price_change_1m >= 0) and bid_replenishment >= 45
        ask_absorption_1m = buy_1m > sell_1m and (price_change_1m is None or price_change_1m <= 0) and ask_replenishment >= 45
        if bid_absorption_1m: add("LONG", "Bid absorption", {"sell_1m": sell_1m, "bid_replenishment": bid_replenishment}, "1M executed flow + L2 lifecycle")
        if ask_absorption_1m: add("SHORT", "Ask absorption", {"buy_1m": buy_1m, "ask_replenishment": ask_replenishment}, "1M executed flow + L2 lifecycle")
        if sell_1m > buy_1m and (price_change_1m is None or price_change_1m >= 0): add("LONG", "Seller exhaustion", price_change_1m, "1M sell aggression vs price response")
        if buy_1m > sell_1m and (price_change_1m is None or price_change_1m <= 0): add("SHORT", "Buyer exhaustion", price_change_1m, "1M buy aggression vs price response")
        if delta_1m > 0: add("LONG", "Positive Delta", delta_1m, "1M executed trade buckets")
        if delta_1m < 0: add("SHORT", "Negative Delta", delta_1m, "1M executed trade buckets")
        if delta_1m > 0 and (price_change_1m or 0) > 0: add("LONG", "Bullish initiative", {"delta": delta_1m, "price_change": price_change_1m}, "1M flow + closed-candle response")
        if delta_1m < 0 and (price_change_1m or 0) < 0: add("SHORT", "Bearish initiative", {"delta": delta_1m, "price_change": price_change_1m}, "1M flow + closed-candle response")
        if delta_1m > 0 and (price_change_1m or 0) < 0: add("SHORT", "Trapped buyer", {"delta": delta_1m, "price_change": price_change_1m}, "1M aggression/price divergence")
        if delta_1m < 0 and (price_change_1m or 0) > 0: add("LONG", "Trapped seller", {"delta": delta_1m, "price_change": price_change_1m}, "1M aggression/price divergence")
        cvd_history = [(at, value) for at, value in list(getattr(state, "cvd_history", [])) if timestamp - at <= 60_000]
        cvd_rising = len(cvd_history) >= 2 and cvd_history[-1][1] > cvd_history[0][1]; cvd_falling = len(cvd_history) >= 2 and cvd_history[-1][1] < cvd_history[0][1]
        if cvd_rising: add("LONG", "CVD rising", True, "Cumulative executed trade delta · 60S window")
        if cvd_falling: add("SHORT", "CVD falling", True, "Cumulative executed trade delta · 60S window")
        funding = number(state.funding_rate)
        if funding is not None and funding > 0: add("SHORT", "Positive funding", funding, "Binance Futures funding")
        if funding is not None and funding < 0: add("LONG", "Negative funding", funding, "Binance Futures funding")
        oi_delta = number(getattr(state, "oi_delta", None))
        if oi_delta is not None and oi_delta > 0: add("LONG", "Open Interest rising", oi_delta, "Binance Futures OI change")
        if oi_delta is not None and oi_delta < 0: add("SHORT", "Open Interest falling", oi_delta, "Binance Futures OI change")
        macd = indicators1.get("macd") or {}; macd_line = number(macd.get("line")); signal = number(macd.get("signal"))
        if macd_line is not None and signal is not None and macd_line > signal: add("LONG", "MACD bullish", {"line": macd_line, "signal": signal}, "1M derived MACD")
        if macd_line is not None and signal is not None and macd_line < signal: add("SHORT", "MACD bearish", {"line": macd_line, "signal": signal}, "1M derived MACD")
        if location.get("long_ok"): add("LONG", "Location", location.get("long_reasons"), "5M location + 1M profile/VWAP")
        if location.get("short_ok"): add("SHORT", "Location", location.get("short_reasons"), "5M location + 1M profile/VWAP")
        max_pain = {"status": "N/A", "reason": "Options max-pain feed is not connected; no score contribution"}
        regime = structures["15m"].get("label", "N/A"); direction = structures["5m"].get("label", "N/A")
        bias = "LONG" if long_score > short_score else "SHORT" if short_score > long_score else "BALANCED"
        context_side = "LONG" if regime == "BULLISH" and direction == "BULLISH" and location.get("long_ok") else "SHORT" if regime == "BEARISH" and direction == "BEARISH" and location.get("short_ok") else None
        setup_side = "LONG" if one.get("label") == "BULLISH" and (one.get("last_bos") == "BOS_UP" or one.get("last_choch") == "CHOCH_UP") and context_side == "LONG" else "SHORT" if one.get("label") == "BEARISH" and (one.get("last_bos") == "BOS_DOWN" or one.get("last_choch") == "CHOCH_DOWN") and context_side == "SHORT" else None
        target_candidates = self._targets(state, price, setup_side or bias if bias != "BALANCED" else None, structures, indicators1, intel, liquidations)
        target = target_candidates[0] if target_candidates else None
        path = self._path(state, price, target, setup_side or bias if bias != "BALANCED" else None)
        trade_side = setup_side or ("LONG" if bias == "LONG" else "SHORT" if bias == "SHORT" else None)
        invalidation = structures["1m"].get("swing_low" if trade_side == "LONG" else "swing_high" if trade_side == "SHORT" else "swing_low")
        if invalidation is None: invalidation = structures["5m"].get("swing_low" if trade_side == "LONG" else "swing_high" if trade_side == "SHORT" else "swing_low")
        if invalidation is None: invalidation = location.get("value_area", {}).get("val" if trade_side == "LONG" else "vah")
        entry = price if price is not None else None
        sl = invalidation
        tp = target.get("price") if target else None
        risk = abs(entry - sl) if entry is not None and sl is not None else None
        reward = abs(tp - entry) if tp is not None and entry is not None else None
        rr = reward / risk if reward is not None and risk and risk > 0 else None
        rr_ok = rr is not None and rr >= self.min_rr
        confirmation = micro if setup_side else {"status": "NOT CONFIRMED", "side": None, "window": "1S", "evidence": [evidence("1M setup required", False, "Decision hierarchy") ]}
        if not state.orderbook.valid or not state.last_trade_at or timestamp - state.last_trade_at > 3000:
            final = "NO TRADE"; final_reason = "WAITING FOR VALIDATED TRADE + ORDER BOOK DATA"
        elif not setup_side or not context_side:
            final = f"WAIT {context_side}" if context_side else "NO TRADE"; final_reason = "15M/5M context, 5M location and 1M setup are not aligned"
        elif target is None or not rr_ok or path.get("label") == "HARD":
            final = "NO TRADE"; final_reason = "Natural target, order-book path or structural RR is insufficient"
        elif confirmation.get("status") == "CONFIRMED" and confirmation.get("side") == setup_side:
            final = f"{setup_side} ENTRY CONFIRMED"; final_reason = "15M → 5M → 1M aligned and 1S confirmation received"
        else:
            final = f"{setup_side} SETUP"; final_reason = "Setup is ready; waiting for 1S confirmation before entry"
        if final.endswith("ENTRY CONFIRMED") and self.active_position is None and entry is not None and sl is not None and tp is not None:
            self.active_position = {"side": setup_side, "entry": entry, "stop_loss": sl, "take_profit": tp, "opened_at": timestamp, "status": "PAPER_MONITOR"}
        monitor = self._monitor(state, timestamp, self.active_position, micro, one, price)
        if monitor.get("status") == "EXIT" and self.active_position:
            self.active_position = None
        return {
            "status": "LIVE" if state.orderbook.valid and state.last_trade_at else "WAITING FOR DATA", "generated_at": timestamp,
            "market_regime": regime, "market_regime_evidence": structures["15m"], "five_minute_direction": direction, "five_minute_evidence": structures["5m"],
            "one_minute_structure": one, "location": location, "long_score": long_score, "short_score": short_score,
            "score_basis": {"long": long_evidence, "short": short_evidence, "unavailable": [evidence("Max Pain", "N/A", "Options surface not connected", 0)], "unit": "POINTS; not probability"}, "max_pain": max_pain, "one_second_confirmation": confirmation,
            "final_bias": bias, "entry_status": "CONFIRMED" if final.endswith("ENTRY CONFIRMED") else "WAITING FOR CONFIRMATION" if setup_side else "NOT READY",
            "entry": entry, "stop_loss": sl, "take_profit": tp, "primary_target": target, "target_type": target.get("type") if target else "N/A",
            "order_book_path": path, "risk_reward": rr, "minimum_rr": self.min_rr, "invalidation": {"price": sl, "source": "1M/5M swing invalidation or VAL/VAH", "rule": "acceptance beyond structural level invalidates thesis"},
            "final_decision": final, "final_reason": final_reason, "liquidation_heatmap": liquidations, "position_monitor": monitor,
            "data_lineage": {"15M": "closed Futures candles", "5M": "closed Futures candles + 1M profile/VWAP", "1M": "closed Futures candles + executed flow", "1S": "received trade executions + L2 lifecycle; confirmation only"},
        }

    @staticmethod
    def _monitor(state: Any, timestamp: int, position: dict[str, Any] | None, micro: dict[str, Any], one: dict[str, Any], price: float | None) -> dict[str, Any]:
        if not position:
            return {"status": "NO ACTIVE POSITION", "mode": "PAPER MONITOR", "evidence": []}
        side = position["side"]; healthy = (one.get("net_notional", 0) > 0) if side == "LONG" else (one.get("net_notional", 0) < 0)
        absorption = micro.get("buy_absorption") if side == "LONG" else micro.get("sell_absorption")
        opposing_absorption = bool(absorption)
        stop = position["stop_loss"]; target = position["take_profit"]
        invalid = price is not None and ((side == "LONG" and price <= stop) or (side == "SHORT" and price >= stop))
        target_hit = price is not None and ((side == "LONG" and price >= target) or (side == "SHORT" and price <= target))
        status = "EXIT" if invalid or target_hit else "WARNING" if opposing_absorption else "HOLD" if healthy else "TRAIL"
        return {"status": status, "mode": "PAPER MONITOR", "side": side, "entry": position["entry"], "stop_loss": stop, "take_profit": target, "evidence": [evidence("Aggression aligned", healthy, "1S executed trades"), evidence("Opposing absorption", opposing_absorption, "1S + L2 lifecycle"), evidence("Structural invalidation", invalid, "Price vs stop-loss"), evidence("Target reached", target_hit, "Price vs primary target")], "warning": "BUY ABSORPTION / CVD divergence / micro break" if opposing_absorption else None, "generated_at": timestamp}
