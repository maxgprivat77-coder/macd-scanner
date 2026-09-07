#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD-Divergenz-Scanner  v1.0.0
================================
Scannt die Top-N USDT-Paare (nach 24h-Umsatz) auf MACD-Divergenzen im
Daily- und Weekly-Chart und meldet Treffer per Telegram.

Datenquelle: Bybit v5 (USDT-Perpetuals). Ist Bybit vom Laufort aus nicht
erreichbar (Geo-Block, z.B. US-Rechenzentrum), fällt der Scanner
automatisch auf Binance Spot (data-api.binance.vision) zurück.

Läuft komplett stateless (GitHub Actions, 1x täglich nach Kerzenschluss).
Doppel-Alerts werden vermieden, indem nur Divergenzen gemeldet werden,
deren letzter Pivot auf der gerade geschlossenen Kerze bestätigt wurde
(ALERT_WINDOW=1).

Changelog
  1.0.0  Erstfassung: Bybit/Binance-Adapter, MACD 12/26/9, reguläre +
         versteckte Divergenz, Telegram-Versand, Dry-Run, Symbol-Filter.
"""
import argparse
import os
import sys
import time
from datetime import datetime, timezone

import requests

VERSION = "1.0.0"


# ------------------------------------------------------------ Konfiguration
def _env_int(key, default):
    return int(os.getenv(key, str(default)))


def _env_bool(key, default):
    return os.getenv(key, str(default)).strip().lower() in ("1", "true", "yes", "ja")


EXCHANGE       = os.getenv("EXCHANGE", "auto").strip().lower()   # auto | bybit | binance
TOP_N          = _env_int("TOP_N", 100)
MACD_FAST      = _env_int("MACD_FAST", 12)
MACD_SLOW      = _env_int("MACD_SLOW", 26)
MACD_SIGNAL    = _env_int("MACD_SIGNAL", 9)
MACD_SOURCE    = os.getenv("MACD_SOURCE", "line").strip().lower()  # line | hist
PIVOT_LEFT     = _env_int("PIVOT_LEFT", 5)
PIVOT_RIGHT    = _env_int("PIVOT_RIGHT", 3)
MIN_PIVOT_DIST = _env_int("MIN_PIVOT_DIST", 5)
MAX_PIVOT_DIST = _env_int("MAX_PIVOT_DIST", 60)
ALERT_WINDOW   = _env_int("ALERT_WINDOW", 1)
DETECT_HIDDEN  = _env_bool("DETECT_HIDDEN", False)
SEND_SUMMARY   = _env_bool("SEND_SUMMARY", True)
MIN_CANDLES    = _env_int("MIN_CANDLES", 60)
KLINE_LIMIT    = _env_int("KLINE_LIMIT", 300)
REQUEST_PAUSE  = float(os.getenv("REQUEST_PAUSE", "0.15"))

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TG_LIMIT = 3900  # Telegram-Hardlimit ist 4096 Zeichen

INTERVAL_MS = {"D": 86_400_000, "W": 7 * 86_400_000}
TF_LABEL    = {"D": "1D", "W": "1W"}

SESSION = requests.Session()
SESSION.headers["User-Agent"] = f"macd-div-scanner/{VERSION}"


def log(msg):
    print(msg, flush=True)


# ------------------------------------------------------------ HTTP / Börsen
class GeoBlocked(Exception):
    """Börse blockt den Laufort (HTTP 403/451 oder Compliance-retCode)."""


def _get_json(url, params):
    last_err = None
    for attempt in range(3):
        try:
            r = SESSION.get(url, params=params, timeout=20)
            if r.status_code in (403, 451):
                raise GeoBlocked(f"HTTP {r.status_code} von {url.split('/')[2]}")
            r.raise_for_status()
            return r.json()
        except GeoBlocked:
            raise
        except Exception as e:  # Timeout, 5xx, JSON-Fehler
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"Request fehlgeschlagen ({url}): {last_err}")


class Bybit:
    name = "Bybit (USDT-Perps)"
    BASE = "https://api.bybit.com"
    INTERVAL = {"D": "D", "W": "W"}

    def _call(self, path, params):
        data = _get_json(self.BASE + path, params)
        if data.get("retCode") != 0:
            if data.get("retCode") == 10024:
                raise GeoBlocked(f"Bybit retCode 10024: {data.get('retMsg')}")
            raise RuntimeError(f"Bybit retCode {data.get('retCode')}: {data.get('retMsg')}")
        return data["result"]

    def top_symbols(self, n):
        rows = self._call("/v5/market/tickers", {"category": "linear"})["list"]
        rows = [t for t in rows if t["symbol"].endswith("USDT") and "-" not in t["symbol"]]
        rows.sort(key=lambda t: float(t.get("turnover24h") or 0), reverse=True)
        return [t["symbol"] for t in rows[:n]]

    def klines(self, symbol, tf, limit):
        # Antwort: newest-first, Felder [startTime, open, high, low, close, volume, turnover]
        rows = self._call("/v5/market/kline", {
            "category": "linear", "symbol": symbol,
            "interval": self.INTERVAL[tf], "limit": limit,
        })["list"]
        out = [(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4])) for r in rows]
        out.sort(key=lambda r: r[0])
        return out


class BinanceVision:
    name = "Binance (Spot)"
    BASE = "https://data-api.binance.vision"
    INTERVAL = {"D": "1d", "W": "1w"}
    STABLES = {"USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDP", "EUR", "TRY", "BRL",
               "GBP", "AUD", "ARS", "COP", "UAH", "PLN", "RON", "CZK", "ZAR", "JPY",
               "MXN", "XUSD", "USD1", "USDE", "PYUSD", "AEUR", "EURI", "PAXG"}

    def top_symbols(self, n):
        rows = _get_json(self.BASE + "/api/v3/ticker/24hr", {})

        def ok(sym):
            if not sym.endswith("USDT"):
                return False
            base = sym[:-4]
            if base in self.STABLES:
                return False
            if base.endswith(("UP", "DOWN", "BULL", "BEAR")):  # Hebel-Tokens
                return False
            return True

        rows = [t for t in rows if ok(t["symbol"])]
        rows.sort(key=lambda t: float(t.get("quoteVolume") or 0), reverse=True)
        return [t["symbol"] for t in rows[:n]]

    def klines(self, symbol, tf, limit):
        # Antwort: oldest-first, Felder [openTime, open, high, low, close, ...]
        rows = _get_json(self.BASE + "/api/v3/klines", {
            "symbol": symbol, "interval": self.INTERVAL[tf], "limit": limit,
        })
        return [(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4])) for r in rows]


def pick_exchange():
    if EXCHANGE == "bybit":
        return Bybit()
    if EXCHANGE == "binance":
        return BinanceVision()
    ex = Bybit()
    try:
        ex.top_symbols(1)  # Erreichbarkeits-Probe
        return ex
    except GeoBlocked as e:
        log(f"Bybit nicht erreichbar ({e}) -> Fallback auf Binance Spot")
        return BinanceVision()


def closed_only(rows, tf, now_ms=None):
    """Verwirft die noch laufende Kerze am Ende der Liste."""
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    if rows and rows[-1][0] + INTERVAL_MS[tf] > now_ms:
        return rows[:-1]
    return rows


# ------------------------------------------------------------ Indikatoren
def ema(values, period):
    k = 2.0 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1.0 - k))
    return out


def macd(closes, fast=None, slow=None, signal=None):
    fast = fast or MACD_FAST
    slow = slow or MACD_SLOW
    signal = signal or MACD_SIGNAL
    line = [f - s for f, s in zip(ema(closes, fast), ema(closes, slow))]
    sig = ema(line, signal)
    hist = [m - s for m, s in zip(line, sig)]
    return line, sig, hist


def pivot_lows(vals, left, right):
    idx = []
    for i in range(left, len(vals) - right):
        v = vals[i]
        if all(v < vals[j] for j in range(i - left, i)) and \
           all(v <= vals[j] for j in range(i + 1, i + right + 1)):
            idx.append(i)
    return idx


def pivot_highs(vals, left, right):
    idx = []
    for i in range(left, len(vals) - right):
        v = vals[i]
        if all(v > vals[j] for j in range(i - left, i)) and \
           all(v >= vals[j] for j in range(i + 1, i + right + 1)):
            idx.append(i)
    return idx


def find_divergences(highs, lows, osc, left=None, right=None, min_dist=None,
                     max_dist=None, alert_window=None, hidden=None):
    """
    Vergleicht den zuletzt bestätigten Preis-Pivot mit früheren Pivots
    (Abstand min..max Kerzen). Gemeldet wird nur, wenn der letzte Pivot
    innerhalb der letzten `alert_window` Kerzen bestätigt wurde.
    """
    left = PIVOT_LEFT if left is None else left
    right = PIVOT_RIGHT if right is None else right
    min_dist = MIN_PIVOT_DIST if min_dist is None else min_dist
    max_dist = MAX_PIVOT_DIST if max_dist is None else max_dist
    alert_window = ALERT_WINDOW if alert_window is None else alert_window
    hidden = DETECT_HIDDEN if hidden is None else hidden

    n = len(osc)
    fresh_from = (n - 1 - right) - (alert_window - 1)
    found = []

    def compare(pivots, price, is_low):
        if not pivots or pivots[-1] < fresh_from:
            return
        i2 = pivots[-1]
        for i1 in reversed(pivots[:-1]):
            d = i2 - i1
            if d < min_dist:
                continue
            if d > max_dist:
                break
            p1, p2, o1, o2 = price[i1], price[i2], osc[i1], osc[i2]
            kind = None
            if is_low:
                if p2 < p1 and o2 > o1:
                    kind = "bull_reg"
                elif hidden and p2 > p1 and o2 < o1:
                    kind = "bull_hid"
            else:
                if p2 > p1 and o2 < o1:
                    kind = "bear_reg"
                elif hidden and p2 < p1 and o2 > o1:
                    kind = "bear_hid"
            if kind:
                found.append({"kind": kind, "i1": i1, "i2": i2,
                              "p1": p1, "p2": p2, "o1": o1, "o2": o2, "dist": d})
                break

    compare(pivot_lows(lows, left, right), lows, True)
    compare(pivot_highs(highs, left, right), highs, False)
    return found


# ------------------------------------------------------------ Scan
def scan_timeframe(exchange, tf, symbols):
    hits, skipped, errors = [], 0, []
    for i, sym in enumerate(symbols, 1):
        try:
            rows = closed_only(exchange.klines(sym, tf, KLINE_LIMIT), tf)
        except Exception as e:
            errors.append(f"{sym}: {e}")
            log(f"  [{i}/{len(symbols)}] {sym} {tf}: FEHLER {e}")
            time.sleep(REQUEST_PAUSE)
            continue
        if len(rows) < MIN_CANDLES:
            skipped += 1
            time.sleep(REQUEST_PAUSE)
            continue
        highs = [r[2] for r in rows]
        lows = [r[3] for r in rows]
        closes = [r[4] for r in rows]
        line, _sig, hist = macd(closes)
        osc = line if MACD_SOURCE == "line" else hist
        for d in find_divergences(highs, lows, osc):
            d.update({"symbol": sym, "tf": tf, "close": closes[-1]})
            hits.append(d)
            log(f"  [{i}/{len(symbols)}] {sym} {tf}: {d['kind']} "
                f"p {d['p1']:.6g}->{d['p2']:.6g} osc {d['o1']:.4g}->{d['o2']:.4g} ({d['dist']} Kerzen)")
        time.sleep(REQUEST_PAUSE)
    return hits, skipped, errors


# ------------------------------------------------------------ Telegram
KIND_LABEL = {
    "bull_reg": ("🟢", "Bullish (regulär)"),
    "bull_hid": ("🟢", "Bullish (versteckt)"),
    "bear_reg": ("🔴", "Bearish (regulär)"),
    "bear_hid": ("🔴", "Bearish (versteckt)"),
}


def html_escape(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def fmt_price(p):
    if p >= 1000:
        return f"{p:,.0f}"
    if p >= 1:
        return f"{p:.2f}"
    if p >= 0.01:
        return f"{p:.4f}"
    return f"{p:.6g}"


def build_lines(tf, hits, n_symbols, skipped, errors, exchange_name, when):
    lines = [
        f"<b>📊 MACD-Divergenz · {TF_LABEL.get(tf, tf)}</b>",
        f"{when} · {html_escape(exchange_name)}",
        f"{n_symbols} Coins gescannt · {len(hits)} Treffer"
        + (f" · {skipped} übersprungen (zu wenig Historie)" if skipped else "")
        + (f" · {len(errors)} Fehler" if errors else ""),
        "",
    ]
    if not hits:
        lines.append("✅ Keine Divergenz gefunden.")
        return lines
    order = {"bull_reg": 0, "bull_hid": 1, "bear_reg": 2, "bear_hid": 3}
    for h in sorted(hits, key=lambda x: (order[x["kind"]], x["symbol"])):
        icon, label = KIND_LABEL[h["kind"]]
        side = "Tief" if h["kind"].startswith("bull") else "Hoch"
        lines.append(f"{icon} <b>{html_escape(h['symbol'])}</b> — {label}")
        lines.append(
            f"{side} {fmt_price(h['p1'])} → {fmt_price(h['p2'])} | "
            f"MACD {h['o1']:.4g} → {h['o2']:.4g} | {h['dist']} Kerzen | "
            f"Close {fmt_price(h['close'])}"
        )
        lines.append("")
    return lines


def chunk_lines(lines, limit=TG_LIMIT):
    chunks, cur = [], ""
    for ln in lines:
        if cur and len(cur) + len(ln) + 1 > limit:
            chunks.append(cur.rstrip("\n"))
            cur = ""
        cur += ln + "\n"
    if cur.strip():
        chunks.append(cur.rstrip("\n"))
    return chunks


def tg_send(text):
    if not TG_TOKEN or not TG_CHAT:
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID fehlen (Secrets prüfen)")
    r = SESSION.post(
        f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
        json={"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML",
              "disable_web_page_preview": True},
        timeout=20,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Telegram HTTP {r.status_code}: {r.text[:200]}")


# ------------------------------------------------------------ Main
def resolve_timeframes(arg):
    if arg and arg.strip().lower() != "auto":
        return [t.strip().upper() for t in arg.split(",") if t.strip()]
    tfs = ["D"]
    if datetime.now(timezone.utc).weekday() == 0:  # Montag: Weekly-Kerze ist frisch zu
        tfs.append("W")
    return tfs


def main(argv=None):
    ap = argparse.ArgumentParser(description="MACD-Divergenz-Scanner")
    ap.add_argument("--timeframes", default=os.getenv("TIMEFRAMES", "auto"),
                    help="auto | D | W | D,W  (auto = D täglich, W nur montags)")
    ap.add_argument("--symbols", default=os.getenv("SYMBOLS", ""),
                    help="Komma-Liste, überschreibt Top-N (zum Testen)")
    ap.add_argument("--dry-run", action="store_true",
                    help="nichts an Telegram senden, nur ausgeben")
    args = ap.parse_args(argv)

    now = datetime.now(timezone.utc)
    when = now.strftime("%d.%m.%Y %H:%M UTC")
    tfs = resolve_timeframes(args.timeframes)
    log(f"MACD-Div-Scanner v{VERSION} start | {when} | tfs={tfs} | top={TOP_N} | "
        f"exchange={EXCHANGE} | source={MACD_SOURCE} | pivots {PIVOT_LEFT}/{PIVOT_RIGHT} | "
        f"dist {MIN_PIVOT_DIST}-{MAX_PIVOT_DIST} | window={ALERT_WINDOW} | hidden={DETECT_HIDDEN}")

    bad = [t for t in tfs if t not in INTERVAL_MS]
    if bad:
        log(f"FATAL: unbekannte Timeframes {bad} (erlaubt: D, W)")
        return 2

    try:
        exchange = pick_exchange()
        if args.symbols.strip():
            symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        else:
            symbols = exchange.top_symbols(TOP_N)
    except Exception as e:
        log(f"FATAL: Symbolliste nicht ladbar: {e}")
        return 1
    log(f"Quelle: {exchange.name} | {len(symbols)} Symbole")

    exit_code = 0
    for tf in tfs:
        log(f"--- Scan {TF_LABEL[tf]} ---")
        hits, skipped, errors = scan_timeframe(exchange, tf, symbols)
        log(f"{TF_LABEL[tf]}: {len(hits)} Treffer, {skipped} übersprungen, {len(errors)} Fehler")
        if not hits and not SEND_SUMMARY:
            continue
        lines = build_lines(tf, hits, len(symbols), skipped, errors, exchange.name, when)
        for chunk in chunk_lines(lines):
            if args.dry_run:
                log("---- Telegram (dry-run) ----\n" + chunk + "\n----")
            else:
                try:
                    tg_send(chunk)
                except Exception as e:
                    log(f"FEHLER Telegram: {e}")
                    exit_code = 1
        if errors and len(errors) > len(symbols) // 2:
            exit_code = 1  # mehr als die Hälfte gescheitert -> Run als fehlgeschlagen markieren

    log(f"MACD-Div-Scanner v{VERSION} fertig | exit={exit_code}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
