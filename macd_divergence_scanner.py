#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD-Divergenz-Scanner  v1.1.0
================================
Scannt die Top-N USDT-Paare (nach 24h-Umsatz) auf MACD-Divergenzen im
Daily- und Weekly-Chart und meldet Treffer per Telegram.

Datenquelle: PERPETUAL FUTURES, in dieser Reihenfolge probiert:
  BingX -> Bybit -> Binance Futures -> (Notnagel) Binance Spot
Die erste erreichbare Quelle gewinnt; welche es war, steht in der Nachricht.
Grund fuer die Kette: GitHub-Runner stehen in US-Rechenzentren, und
Bybit (403) sowie Binance-Futures (451) blocken die haeufig.

Laeuft komplett stateless (GitHub Actions, 1x taeglich nach Kerzenschluss).
Doppel-Alerts werden vermieden, indem nur Divergenzen gemeldet werden,
deren letzter Pivot auf der gerade geschlossenen Kerze bestaetigt wurde
(ALERT_WINDOW=1).

Changelog
  1.1.0  Futures-Kette BingX/Bybit/Binance-Futures statt Spot-only;
         Signalstaerke-Filter MIN_PRICE_DIFF_PCT + MIN_OSC_DIFF_PCT gegen
         Rausch-Treffer (ORCA 1.01->1.01, PYTH-MACD 1.5%); getrennte
         Pivot-Bestaetigung je Timeframe (PIVOT_RIGHT_W=1, Weekly-Signale
         kamen 3 Wochen zu spaet); Gold-/Wrapped-Token gefiltert (XAUT);
         Schub-Hinweis bei marktweiten Weekly-Clustern.
  1.0.0  Erstfassung: Bybit/Binance-Adapter, MACD 12/26/9, regulaere +
         versteckte Divergenz, Telegram-Versand, Dry-Run, Symbol-Filter.
"""
import argparse
import hashlib
import hmac
import os
import sys
import time
import urllib.parse
from datetime import datetime, timezone

import requests

VERSION = "1.1.0"


# ------------------------------------------------------------ Konfiguration
def _env_int(key, default):
    return int(os.getenv(key, str(default)))


def _env_float(key, default):
    return float(os.getenv(key, str(default)))


def _env_bool(key, default):
    return os.getenv(key, str(default)).strip().lower() in ("1", "true", "yes", "ja")


EXCHANGE       = os.getenv("EXCHANGE", "auto").strip().lower()
TOP_N          = _env_int("TOP_N", 100)
MACD_FAST      = _env_int("MACD_FAST", 12)
MACD_SLOW      = _env_int("MACD_SLOW", 26)
MACD_SIGNAL    = _env_int("MACD_SIGNAL", 9)
MACD_SOURCE    = os.getenv("MACD_SOURCE", "line").strip().lower()  # line | hist
PIVOT_LEFT     = _env_int("PIVOT_LEFT", 5)
PIVOT_RIGHT    = _env_int("PIVOT_RIGHT", 3)          # Daily
PIVOT_RIGHT_W  = _env_int("PIVOT_RIGHT_W", 1)        # Weekly (3 Wochen Verzug waeren zu viel)
MIN_PIVOT_DIST = _env_int("MIN_PIVOT_DIST", 5)
MAX_PIVOT_DIST = _env_int("MAX_PIVOT_DIST", 60)
MIN_PRICE_DIFF_PCT = _env_float("MIN_PRICE_DIFF_PCT", 1.0)   # 0 = aus
MIN_OSC_DIFF_PCT   = _env_float("MIN_OSC_DIFF_PCT", 10.0)    # 0 = aus
ALERT_WINDOW   = _env_int("ALERT_WINDOW", 1)
DETECT_HIDDEN  = _env_bool("DETECT_HIDDEN", False)
SEND_SUMMARY   = _env_bool("SEND_SUMMARY", True)
CLUSTER_HINT   = _env_int("CLUSTER_HINT", 8)         # ab so vielen gleichgerichteten Treffern: Hinweis
MIN_CANDLES    = _env_int("MIN_CANDLES", 60)
KLINE_LIMIT    = _env_int("KLINE_LIMIT", 300)
REQUEST_PAUSE  = float(os.getenv("REQUEST_PAUSE", "0.15"))

BINGX_KEY    = os.getenv("BINGX_API_KEY", "").strip()
BINGX_SECRET = os.getenv("BINGX_API_SECRET", "").strip()

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TG_LIMIT = 3900  # Telegram-Hardlimit ist 4096 Zeichen

INTERVAL_MS = {"D": 86_400_000, "W": 7 * 86_400_000}
TF_LABEL    = {"D": "1D", "W": "1W"}

# Kein Coin im eigentlichen Sinn: Stablecoins, Fiat, tokenisiertes Gold,
# Wrapped-/Staking-Derivate von BTC und ETH (laufen 1:1 mit dem Basiswert).
NON_COINS = {
    "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDP", "USDE", "USD1", "PYUSD",
    "XUSD", "AEUR", "EURI", "EURC", "USDD", "USDS", "SUSD", "FRAX", "LUSD",
    "EUR", "TRY", "BRL", "GBP", "AUD", "ARS", "COP", "UAH", "PLN", "RON",
    "CZK", "ZAR", "JPY", "MXN", "IDR", "NGN", "VAI",
    "PAXG", "XAUT", "XAU", "TGOLD",
    "WBTC", "BTCB", "WBETH", "BETH", "STETH", "WSTETH", "WETH", "CBETH",
    "RETH", "METH", "EZETH", "WEETH", "SOLVBTC", "LBTC",
}

SESSION = requests.Session()
SESSION.headers["User-Agent"] = f"macd-div-scanner/{VERSION}"


def log(msg):
    print(msg, flush=True)


def pivot_right_for(tf):
    return PIVOT_RIGHT_W if tf == "W" else PIVOT_RIGHT


def is_real_coin(base):
    if base in NON_COINS:
        return False
    if base.endswith(("UP", "DOWN", "BULL", "BEAR")):  # Hebel-Tokens
        return False
    return True


# ------------------------------------------------------------ HTTP / Boersen
class Unreachable(Exception):
    """Boerse ist von hier aus nicht nutzbar (Geo-Block, Auth-Zwang, kaputte Antwort)."""


def _get_json(url, params=None, headers=None, tries=3):
    last_err = None
    for attempt in range(tries):
        try:
            r = SESSION.get(url, params=params, headers=headers, timeout=20)
            if r.status_code in (401, 403, 451):
                raise Unreachable(f"HTTP {r.status_code} von {url.split('/')[2]}")
            r.raise_for_status()
            return r.json()
        except Unreachable:
            raise
        except Exception as e:  # Timeout, 5xx, JSON-Fehler
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"Request fehlgeschlagen ({url}): {last_err}")


def _num(d, *keys, default=None):
    """Erste vorhandene, in float wandelbare Zahl aus einem Dict holen."""
    for k in keys:
        if k in d and d[k] not in (None, ""):
            try:
                return float(d[k])
            except (TypeError, ValueError):
                continue
    return default


class BingX:
    """USDT-M Perpetual Futures. Symbole im Format BTC-USDT.

    Die BingX-Doku ist widerspruechlich, ob die quote-Endpunkte eine
    HMAC-Signatur brauchen. Deshalb: ohne Key versuchen, mit Key signieren,
    falls welcher hinterlegt ist. Verlangt die API doch Auth, wirft der
    Adapter Unreachable und die Kette geht zur naechsten Boerse.
    """
    name = "BingX (USDT-M Perps)"
    BASE = "https://open-api.bingx.com"
    INTERVAL = {"D": "1d", "W": "1w"}

    def _call(self, path, params):
        params = dict(params or {})
        params["timestamp"] = int(time.time() * 1000)
        headers = {"X-SOURCE-KEY": "BX-AI-SKILL"}
        if BINGX_KEY and BINGX_SECRET:
            qs = urllib.parse.urlencode(sorted(params.items()))
            params["signature"] = hmac.new(
                BINGX_SECRET.encode(), qs.encode(), hashlib.sha256
            ).hexdigest()
            headers["X-BX-APIKEY"] = BINGX_KEY
        data = _get_json(self.BASE + path, params, headers)
        code = data.get("code", 0)
        if str(code) not in ("0", "None"):
            msg = f"BingX code {code}: {data.get('msg')}"
            # Signatur-/Key-/Permission-Fehler -> Boerse hier nicht nutzbar
            if str(code) in ("100001", "100202", "100413", "100414", "100421", "80014"):
                raise Unreachable(msg)
            raise RuntimeError(msg)
        payload = data.get("data")
        if payload is None:
            raise Unreachable(f"BingX: leeres data-Feld ({str(data)[:120]})")
        return payload

    def top_symbols(self, n):
        rows = self._call("/openApi/swap/v2/quote/ticker", {})
        if isinstance(rows, dict):
            rows = rows.get("list") or rows.get("tickers") or []
        out = []
        for t in rows:
            sym = t.get("symbol", "")
            if not sym.endswith("-USDT") or not is_real_coin(sym.split("-")[0]):
                continue
            turnover = _num(t, "quoteVolume", "turnover", "amount")
            if turnover is None:
                vol = _num(t, "volume", default=0.0)
                last = _num(t, "lastPrice", "close", "price", default=0.0)
                turnover = vol * last
            out.append((turnover, sym))
        out.sort(reverse=True)
        return [s for _, s in out[:n]]

    def _parse_klines(self, rows):
        out = []
        for r in rows:
            if isinstance(r, dict):
                ts = _num(r, "time", "openTime", "t")
                o = _num(r, "open", "o")
                h = _num(r, "high", "h")
                lo = _num(r, "low", "l")
                c = _num(r, "close", "c")
            else:  # Array-Form [time, open, high, low, close, volume]
                ts, o, h, lo, c = (float(r[0]), float(r[1]), float(r[2]),
                                   float(r[3]), float(r[4]))
            if None in (ts, o, h, lo, c):
                continue
            out.append((int(ts), o, h, lo, c))
        out.sort(key=lambda r: r[0])
        return out

    def klines(self, symbol, tf, limit):
        try:
            rows = self._call("/openApi/swap/v3/quote/klines", {
                "symbol": symbol, "interval": self.INTERVAL[tf], "limit": limit})
        except RuntimeError:
            rows = self._call("/openApi/swap/v2/quote/klines", {
                "symbol": symbol, "interval": self.INTERVAL[tf], "limit": limit})
        if isinstance(rows, dict):
            rows = rows.get("klines") or rows.get("list") or []
        return self._parse_klines(rows)


class Bybit:
    name = "Bybit (USDT-Perps)"
    BASE = "https://api.bybit.com"
    INTERVAL = {"D": "D", "W": "W"}

    def _call(self, path, params):
        data = _get_json(self.BASE + path, params)
        if data.get("retCode") != 0:
            if data.get("retCode") == 10024:
                raise Unreachable(f"Bybit retCode 10024: {data.get('retMsg')}")
            raise RuntimeError(f"Bybit retCode {data.get('retCode')}: {data.get('retMsg')}")
        return data["result"]

    def top_symbols(self, n):
        rows = self._call("/v5/market/tickers", {"category": "linear"})["list"]
        rows = [t for t in rows
                if t["symbol"].endswith("USDT") and "-" not in t["symbol"]
                and is_real_coin(t["symbol"][:-4])]
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


class BinanceFutures:
    name = "Binance (USDT-M Futures)"
    BASE = "https://fapi.binance.com"
    INTERVAL = {"D": "1d", "W": "1w"}

    def top_symbols(self, n):
        rows = _get_json(self.BASE + "/fapi/v1/ticker/24hr")
        rows = [t for t in rows
                if t["symbol"].endswith("USDT") and is_real_coin(t["symbol"][:-4])]
        rows.sort(key=lambda t: float(t.get("quoteVolume") or 0), reverse=True)
        return [t["symbol"] for t in rows[:n]]

    def klines(self, symbol, tf, limit):
        rows = _get_json(self.BASE + "/fapi/v1/klines", {
            "symbol": symbol, "interval": self.INTERVAL[tf], "limit": limit})
        return [(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4])) for r in rows]


class BinanceSpot:
    """Notnagel: geo-offener Datenhost, aber nur Spot."""
    name = "Binance (Spot — Futures nicht erreichbar)"
    BASE = "https://data-api.binance.vision"
    INTERVAL = {"D": "1d", "W": "1w"}

    def top_symbols(self, n):
        rows = _get_json(self.BASE + "/api/v3/ticker/24hr")
        rows = [t for t in rows
                if t["symbol"].endswith("USDT") and is_real_coin(t["symbol"][:-4])]
        rows.sort(key=lambda t: float(t.get("quoteVolume") or 0), reverse=True)
        return [t["symbol"] for t in rows[:n]]

    def klines(self, symbol, tf, limit):
        rows = _get_json(self.BASE + "/api/v3/klines", {
            "symbol": symbol, "interval": self.INTERVAL[tf], "limit": limit})
        return [(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4])) for r in rows]


EXCHANGES = {"bingx": BingX, "bybit": Bybit,
             "binance-futures": BinanceFutures, "binance-spot": BinanceSpot}
CHAIN = ["bingx", "bybit", "binance-futures", "binance-spot"]


def pick_exchange():
    """Erste Boerse der Kette, die eine Symbolliste UND eine Testkerze liefert."""
    if EXCHANGE in EXCHANGES:
        ex = EXCHANGES[EXCHANGE]()
        return ex, ex.top_symbols(TOP_N)
    for key in CHAIN:
        ex = EXCHANGES[key]()
        try:
            syms = ex.top_symbols(TOP_N)
            if not syms:
                raise Unreachable("leere Symbolliste")
            ex.klines(syms[0], "D", 5)  # Klines koennen anders blocken als Ticker
            log(f"Quelle: {ex.name}")
            return ex, syms
        except Unreachable as e:
            log(f"  {ex.name} nicht nutzbar ({e}) -> naechste Quelle")
        except Exception as e:
            log(f"  {ex.name} Fehler ({type(e).__name__}: {e}) -> naechste Quelle")
    raise RuntimeError("Keine Boerse der Kette erreichbar")


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


def pct_diff(a, b):
    """Abstand von a nach b in Prozent, bezogen auf den groesseren Betrag.
    Bezug auf max(|a|,|b|) statt auf a, damit Vorzeichenwechsel und Werte
    nahe null keine Scheinriesen erzeugen (der MACD schwingt um 0)."""
    ref = max(abs(a), abs(b))
    if ref == 0:
        return 0.0
    return abs(b - a) / ref * 100.0


def find_divergences(highs, lows, osc, left=None, right=None, min_dist=None,
                     max_dist=None, alert_window=None, hidden=None,
                     min_price_pct=None, min_osc_pct=None):
    """
    Vergleicht den zuletzt bestaetigten Preis-Pivot mit frueheren Pivots
    (Abstand min..max Kerzen). Gemeldet wird nur, wenn der letzte Pivot
    innerhalb der letzten `alert_window` Kerzen bestaetigt wurde UND beide
    Seiten (Preis, Oszillator) die Mindestdifferenz ueberschreiten.
    """
    left = PIVOT_LEFT if left is None else left
    right = PIVOT_RIGHT if right is None else right
    min_dist = MIN_PIVOT_DIST if min_dist is None else min_dist
    max_dist = MAX_PIVOT_DIST if max_dist is None else max_dist
    alert_window = ALERT_WINDOW if alert_window is None else alert_window
    hidden = DETECT_HIDDEN if hidden is None else hidden
    min_price_pct = MIN_PRICE_DIFF_PCT if min_price_pct is None else min_price_pct
    min_osc_pct = MIN_OSC_DIFF_PCT if min_osc_pct is None else min_osc_pct

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
            if not kind:
                continue
            # Signalstaerke: marginale Unterschiede sind Rauschen, kein Signal
            dp, do = pct_diff(p1, p2), pct_diff(o1, o2)
            if dp < min_price_pct or do < min_osc_pct:
                break  # Muster erkannt, aber zu schwach -> nicht weitersuchen
            found.append({"kind": kind, "i1": i1, "i2": i2,
                          "p1": p1, "p2": p2, "o1": o1, "o2": o2,
                          "dist": d, "dp": dp, "do": do})
            break

    compare(pivot_lows(lows, left, right), lows, True)
    compare(pivot_highs(highs, left, right), highs, False)
    return found


# ------------------------------------------------------------ Scan
def scan_timeframe(exchange, tf, symbols):
    hits, skipped, errors = [], 0, []
    right = pivot_right_for(tf)
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
        for d in find_divergences(highs, lows, osc, right=right):
            d.update({"symbol": sym, "tf": tf, "close": closes[-1]})
            hits.append(d)
            log(f"  [{i}/{len(symbols)}] {sym} {tf}: {d['kind']} "
                f"p {d['p1']:.6g}->{d['p2']:.6g} ({d['dp']:.1f}%) "
                f"osc {d['o1']:.4g}->{d['o2']:.4g} ({d['do']:.1f}%) [{d['dist']} Kerzen]")
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


def cluster_note(hits, threshold=None):
    """Marktweite Schuebe sind EIN Ereignis, nicht N unabhaengige Signale."""
    threshold = CLUSTER_HINT if threshold is None else threshold
    if threshold <= 0 or not hits:
        return None
    bull = sum(1 for h in hits if h["kind"].startswith("bull"))
    bear = len(hits) - bull
    if bull >= threshold and bear == 0:
        return (f"ℹ️ {bull} gleichgerichtete Bullish-Signale — sieht nach marktweitem "
                f"Boden aus, nicht nach {bull} unabhängigen Setups.")
    if bear >= threshold and bull == 0:
        return (f"ℹ️ {bear} gleichgerichtete Bearish-Signale — sieht nach marktweitem "
                f"Top aus, nicht nach {bear} unabhängigen Setups.")
    return None


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
    note = cluster_note(hits)
    if note:
        lines += [note, ""]
    order = {"bull_reg": 0, "bull_hid": 1, "bear_reg": 2, "bear_hid": 3}
    for h in sorted(hits, key=lambda x: (order[x["kind"]], -x.get("do", 0))):
        icon, label = KIND_LABEL[h["kind"]]
        side = "Tief" if h["kind"].startswith("bull") else "Hoch"
        lines.append(f"{icon} <b>{html_escape(h['symbol'])}</b> — {label}")
        lines.append(
            f"{side} {fmt_price(h['p1'])} → {fmt_price(h['p2'])} ({h.get('dp', 0):.1f}%) | "
            f"MACD {h['o1']:.4g} → {h['o2']:.4g} ({h.get('do', 0):.0f}%) | "
            f"{h['dist']} Kerzen | Close {fmt_price(h['close'])}"
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
        f"exchange={EXCHANGE} | source={MACD_SOURCE} | pivots {PIVOT_LEFT}/{PIVOT_RIGHT}"
        f"(W:{PIVOT_RIGHT_W}) | dist {MIN_PIVOT_DIST}-{MAX_PIVOT_DIST} | "
        f"min-diff {MIN_PRICE_DIFF_PCT}%/{MIN_OSC_DIFF_PCT}% | "
        f"window={ALERT_WINDOW} | hidden={DETECT_HIDDEN}")

    bad = [t for t in tfs if t not in INTERVAL_MS]
    if bad:
        log(f"FATAL: unbekannte Timeframes {bad} (erlaubt: D, W)")
        return 2

    try:
        exchange, symbols = pick_exchange()
        if args.symbols.strip():
            symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    except Exception as e:
        log(f"FATAL: keine Datenquelle nutzbar: {e}")
        return 1
    log(f"Verwende: {exchange.name} | {len(symbols)} Symbole")

    exit_code = 0
    for tf in tfs:
        log(f"--- Scan {TF_LABEL[tf]} (Pivot-Bestätigung: {pivot_right_for(tf)}) ---")
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
            exit_code = 1  # mehr als die Haelfte gescheitert -> Run als fehlgeschlagen markieren

    log(f"MACD-Div-Scanner v{VERSION} fertig | exit={exit_code}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
