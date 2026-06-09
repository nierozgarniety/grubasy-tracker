"""
Śledzenie portfeli cwaniaków v0.2.0
====================================
Nowa logika: szukamy smart money przez heurystykę czasową.

KROK 1: Historia ceny BTC (Kraken) → znajdź 10 największych pompów (+15% w 72h)
KROK 2: Dla każdego pompa → bloki z 24-72h PRZED nim (mempool.space)
KROK 3: Z tych bloków → adresy które otrzymały duży wpływ BTC
KROK 4: Zbierz unikalne adresy → policz win rate / smart score dla każdego
KROK 5: Ranking → grubasy_ranking.csv
"""

import requests, pandas as pd, numpy as np, time, json, os, sys
from datetime import datetime, timezone, timedelta
from tqdm import tqdm

# ════════════════════════════════════════════════
TOP_PUMPS           = 15     # ile pompów szukamy
MIN_PUMP_PCT        = 8      # minimalny % wzrostu w 72h
WINDOW_BEFORE_H     = 72     # godziny przed pompem (max)
WINDOW_AFTER_H      = 6      # godziny przed pompem (min)
MIN_BTC_INFLOW      = 0.5    # minimalny wpływ BTC żeby uznać za kandydata
MAX_CANDIDATES      = 150    # max adresów do analizy
MAX_TX_PER_ADDR     = 60     # max transakcji na adres
MIN_BTC_FLOW        = 0.05   # min BTC per transakcja (filtr szumu)
MIN_TRADES          = 2      # min zamkniętych tradów żeby wejść do rankingu
MIN_PNL_PCT         = 3.0    # min % różnicy ceny kupno→sprzedaż żeby liczyć jako trade
SLEEP_API           = 0.3
OUTPUT_CSV          = "grubasy_ranking.csv"
CACHE_FILE          = "grubasy_price_cache_v2.json"
# ════════════════════════════════════════════════

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

def log(msg): print(f"  {msg}", flush=True)

# ── Cache ─────────────────────────────────────────────────────────────────────

def load_cache():
    try:
        with open(CACHE_FILE) as f:
            return json.load(f)
    except:
        return {}

def save_cache(cache):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)

# ── KROK 1: Historia ceny BTC + znajdź pompy ─────────────────────────────────

def get_btc_price_history():
    """Pobiera dzienną historię BTC/USD z ostatnich 2 lat (Kraken OHLC)."""
    log("Pobieram historię ceny BTC (Kraken)...")
    prices = []  # lista (timestamp, close_price)
    try:
        # Kraken daje max 720 świec — bierzemy dzienne (1440 min)
        r = requests.get(
            "https://api.kraken.com/0/public/OHLC",
            params={"pair": "XBTUSD", "interval": 1440},
            headers=HEADERS, timeout=20
        )
        if r.status_code == 200:
            data = r.json()
            ohlc = data.get("result", {}).get("XXBTZUSD", [])
            prices = [(int(row[0]), float(row[4])) for row in ohlc]  # (ts, close)
            log(f"Pobrano {len(prices)} dni historii ceny")
    except Exception as e:
        log(f"Błąd historii ceny: {e}")
    return prices

def find_pumps(prices, min_pump_pct=MIN_PUMP_PCT, window_days=3, top_n=TOP_PUMPS):
    """
    Znajdź momenty gdzie cena wzrosła o min_pump_pct% w ciągu window_days dni.
    Zwraca listę (ts_start, ts_end, pct_change) posortowaną po wielkości pompa.
    """
    pumps = []
    for i in range(len(prices) - window_days):
        ts_start, p_start = prices[i]
        ts_end,   p_end   = prices[i + window_days]
        pct = (p_end - p_start) / p_start * 100
        if pct >= min_pump_pct:
            pumps.append((ts_start, ts_end, round(pct, 1), p_start, p_end))

    # Deduplicate — usuń nakładające się okna, zostaw największe
    pumps.sort(key=lambda x: -x[2])
    deduped = []
    used_ts = set()
    for pump in pumps:
        ts_s, ts_e = pump[0], pump[1]
        overlap = any(abs(ts_s - u) < 86400 * window_days for u in used_ts)
        if not overlap:
            deduped.append(pump)
            used_ts.add(ts_s)
        if len(deduped) >= top_n:
            break

    log(f"Znaleziono {len(deduped)} pompów ≥{min_pump_pct}%:")
    for p in deduped:
        dt = datetime.fromtimestamp(p[0], tz=timezone.utc).strftime("%Y-%m-%d")
        log(f"  {dt}  +{p[2]}%  (${int(p[3])} → ${int(p[4])})")

    return deduped

# ── KROK 2: Bloki przed pompem ────────────────────────────────────────────────

def get_blocks_in_window(ts_pump_start, hours_before_min, hours_before_max):
    """
    Znajdź bloki BTC z okna czasowego przed pompem.
    Używa blockchain.info/blocks/{ts_ms} który zwraca bloki z danego dnia.
    """
    ts_window_end   = ts_pump_start - hours_before_min * 3600
    ts_window_start = ts_pump_start - hours_before_max * 3600

    blocks = []
    seen_hashes = set()

    # Pobierz bloki dla każdego dnia w oknie
    day_ts = ts_window_start
    while day_ts <= ts_window_end:
        try:
            ts_ms = day_ts * 1000
            r = requests.get(
                f"https://blockchain.info/blocks/{ts_ms}?format=json",
                headers=HEADERS, timeout=20
            )
            if r.status_code == 200:
                day_blocks = r.json()
                for b in day_blocks:
                    bt   = b.get("time", 0)
                    bhash = b.get("hash", "")
                    if bhash in seen_hashes:
                        continue
                    if ts_window_start <= bt <= ts_window_end:
                        blocks.append({"id": bhash, "timestamp": bt, "height": b.get("height")})
                        seen_hashes.add(bhash)
            time.sleep(SLEEP_API)
        except Exception:
            pass
        day_ts += 86400  # następny dzień

    return blocks

# ── KROK 3: Adresy z wpływem BTC z bloków ────────────────────────────────────

def get_top_receivers_from_block(block_hash, min_btc=MIN_BTC_INFLOW, max_addrs=20):
    """
    Pobierz transakcje z bloku, znajdź adresy które otrzymały ≥ min_btc BTC.
    """
    receivers = {}
    try:
        r = requests.get(
            f"https://mempool.space/api/block/{block_hash}/txs/0",
            headers=HEADERS, timeout=20
        )
        if r.status_code == 200:
            txs = r.json()
            for tx in txs:
                # Sumuj outputy per adres
                for out in tx.get("vout", []):
                    addr = out.get("scriptpubkey_address")
                    val  = out.get("value", 0) / 1e8
                    if addr and val >= 0.01:
                        receivers[addr] = receivers.get(addr, 0) + val

        time.sleep(SLEEP_API)
    except Exception:
        pass

    # Filtruj i sortuj
    filtered = {a: v for a, v in receivers.items() if v >= min_btc}
    sorted_r  = sorted(filtered.items(), key=lambda x: -x[1])
    return sorted_r[:max_addrs]

# ── KROK 4: Cena BTC dla timestamp ───────────────────────────────────────────

def get_price(timestamp_ms, cache):
    """
    Pobiera cenę BTC/USD dla danego timestamp.
    Używa Krakena OHLC dziennego — stabilne, sprawdzone.
    Cache key = timestamp zaokrąglony do dnia.
    """
    day_ts = (timestamp_ms // 86_400_000) * 86_400_000
    key = "d_" + str(day_ts)
    if key in cache:
        return cache[key]

    day_s = day_ts // 1000

    try:
        since = day_s - 86400 * 2  # 2 dni wcześniej żeby na pewno złapać świecę
        r = requests.get(
            "https://api.kraken.com/0/public/OHLC",
            params={"pair": "XBTUSD", "interval": 1440, "since": since},
            headers=HEADERS, timeout=15
        )
        if r.status_code == 200:
            ohlc = r.json().get("result", {}).get("XXBTZUSD", [])
            if ohlc:
                exact = [row for row in ohlc if int(row[0]) == day_s]
                if exact:
                    price = float(exact[0][4])
                else:
                    closest = min(ohlc, key=lambda x: abs(int(x[0]) - day_s))
                    price = float(closest[4])
                cache[key] = price
                save_cache(cache)
                return price
    except:
        pass

    return None


def get_address_txs(address):
    for attempt in range(3):
        try:
            r = requests.get(
                f"https://mempool.space/api/address/{address}/txs",
                headers=HEADERS, timeout=20
            )
            if r.status_code == 200:
                time.sleep(SLEEP_API)
                return r.json()[:MAX_TX_PER_ADDR]
            elif r.status_code == 429:
                time.sleep(10)
        except:
            time.sleep(2)
    time.sleep(SLEEP_API)
    return []

def parse_flows(address, txs):
    flows = []
    for tx in txs:
        if not tx.get("status", {}).get("confirmed"):
            continue
        block_time = tx.get("status", {}).get("block_time")
        if not block_time:
            continue
        spent = sum(
            inp.get("prevout", {}).get("value", 0)
            for inp in tx.get("vin", [])
            if inp and inp.get("prevout") and inp.get("prevout", {}).get("scriptpubkey_address") == address
        )
        received = sum(
            out.get("value", 0)
            for out in tx.get("vout", [])
            if out.get("scriptpubkey_address") == address
        )
        net_btc = (received - spent) / 1e8
        if abs(net_btc) < MIN_BTC_FLOW:
            continue
        flows.append({
            "timestamp":    block_time,
            "timestamp_ms": block_time * 1000,
            "date":         datetime.fromtimestamp(block_time, tz=timezone.utc).strftime("%Y-%m-%d"),
            "net_btc":      net_btc,
            "direction":    "IN" if net_btc > 0 else "OUT"
        })
    return flows

def analyze(address, flows, cache):
    if not flows:
        return None
    flows = sorted(flows, key=lambda x: x["timestamp"])
    for f in flows:
        f["price"] = get_price(f["timestamp_ms"], cache)
    flows = [f for f in flows if f["price"]]
    if not flows:
        return None

    buy_stack, trades = [], []
    for f in flows:
        if f["direction"] == "IN":
            buy_stack.append({"btc": abs(f["net_btc"]), "price": f["price"], "date": f["date"]})
        elif f["direction"] == "OUT" and buy_stack:
            remaining = abs(f["net_btc"])
            while remaining > 0.0001 and buy_stack:
                buy = buy_stack[0]
                matched = min(remaining, buy["btc"])
                pnl_pct = (f["price"] - buy["price"]) / buy["price"] * 100
                # Liczymy tylko ruchy ≥ MIN_PNL_PCT% (w górę lub w dół)
                if abs(pnl_pct) >= MIN_PNL_PCT:
                    trades.append({
                        "buy_date": buy["date"], "sell_date": f["date"],
                        "btc": matched, "buy_price": buy["price"],
                        "sell_price": f["price"], "pnl_pct": pnl_pct,
                        "profit_usd": matched * (f["price"] - buy["price"]),
                        "win": pnl_pct > 0
                    })
                buy["btc"] -= matched
                remaining -= matched
                if buy["btc"] < 0.0001:
                    buy_stack.pop(0)

    total_in  = sum(f["net_btc"] for f in flows if f["direction"] == "IN")
    total_out = sum(abs(f["net_btc"]) for f in flows if f["direction"] == "OUT")
    base = {"address": address, "total_txs": len(flows),
            "total_btc_in": round(total_in, 4), "total_btc_out": round(total_out, 4)}

    if len(trades) < MIN_TRADES:
        return None  # Za mało danych — pomijamy

    win_rate    = len([t for t in trades if t["win"]]) / len(trades) * 100
    avg_pnl     = np.mean([t["pnl_pct"] for t in trades])
    tot_profit  = sum(t["profit_usd"] for t in trades)
    confidence  = min(len(trades) / 8, 1.0)
    smart_score = (win_rate * 0.5 + np.clip(avg_pnl, -100, 200) * 0.3) * confidence

    return {**base, "status": "TRADER", "closed_trades": len(trades),
            "win_rate": round(win_rate, 1), "avg_pnl_pct": round(avg_pnl, 1),
            "total_profit_usd": round(tot_profit, 0),
            "avg_buy_price": round(np.mean([t["buy_price"] for t in trades]), 0),
            "smart_score": round(smart_score, 2)}

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 62)
    print("  ŚLEDZENIE PORTFELI CWANIAKÓW v0.2.0")
    print("  Tryb: heurystyka czasowa (smart money przed pompami)")
    print("=" * 62 + "\n")

    # Test API
    for name, url in [
        ("mempool.space", "https://mempool.space/api/v1/difficulty-adjustment"),
        ("Binance",       "https://api.binance.com/api/v3/ping"),
        ("Kraken",        "https://api.kraken.com/0/public/Time"),
    ]:
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            log(f"{name}: {'OK ✓' if r.status_code == 200 else 'BŁĄD ' + str(r.status_code)}")
        except Exception as e:
            log(f"{name}: BŁĄD ({e})")

    cache = load_cache()

    # KROK 1: Pompy
    print()
    prices = get_btc_price_history()
    if not prices:
        log("Brak historii ceny — przerywam.")
        pd.DataFrame().to_csv(OUTPUT_CSV, index=False)
        return

    pumps = find_pumps(prices)
    if not pumps:
        log("Nie znaleziono pompów — zmień parametry.")
        pd.DataFrame().to_csv(OUTPUT_CSV, index=False)
        return

    # KROK 2 & 3: Zbierz kandydatów
    print()
    log("Szukam adresów aktywnych przed pompami...")
    candidates = {}  # addr → max_inflow

    for pump in pumps:
        ts_pump, _, pct, _, _ = pump
        dt = datetime.fromtimestamp(ts_pump, tz=timezone.utc).strftime("%Y-%m-%d")
        log(f"Analizuję pompa {dt} +{pct}%...")

        blocks = get_blocks_in_window(ts_pump, WINDOW_AFTER_H, WINDOW_BEFORE_H)
        log(f"  Znaleziono {len(blocks)} bloków w oknie")

        for block in blocks[:8]:  # max 8 bloków na pompa
            block_hash = block.get("id")
            if not block_hash:
                continue
            receivers = get_top_receivers_from_block(block_hash, MIN_BTC_INFLOW)
            for addr, btc_val in receivers:
                if addr not in candidates or candidates[addr] < btc_val:
                    candidates[addr] = btc_val

        log(f"  Kandydaci łącznie: {len(candidates)}")

    # Sortuj po inflow, weź top MAX_CANDIDATES
    top_candidates = sorted(candidates.items(), key=lambda x: -x[1])[:MAX_CANDIDATES]
    log(f"\nŁącznie unikalnych kandydatów: {len(candidates)}")
    log(f"Analizuję top {len(top_candidates)} po wielkości wpływu BTC\n")

    # KROK 4 & 5: Analiza win rate
    results = []
    for i, (addr, inflow) in enumerate(tqdm(top_candidates, desc="Analiza")):
        txs   = get_address_txs(addr)
        flows = parse_flows(addr, txs)
        res   = analyze(addr, flows, cache)
        if res:
            res["discovered_inflow_btc"] = round(inflow, 4)
            results.append(res)
        if i % 15 == 0:
            save_cache(cache)

    save_cache(cache)

    if not results:
        log("Brak wyników z wystarczającą liczbą tradów.")
        pd.DataFrame(columns=["rank","address","total_txs","total_btc_in","total_btc_out",
                               "status","closed_trades","win_rate","avg_pnl_pct",
                               "total_profit_usd","avg_buy_price","smart_score",
                               "discovered_inflow_btc"]).to_csv(OUTPUT_CSV, index=False)
        return

    df    = pd.DataFrame(results).sort_values("smart_score", ascending=False)
    df.insert(0, "rank", range(1, len(df) + 1))
    df.to_csv(OUTPUT_CSV, index=False)

    print("\n" + "=" * 62)
    log(f"Kandydaci zbadani: {len(top_candidates)}")
    log(f"Z wystarczającą historią: {len(results)}")
    log(f"\nTOP 10 SMART MONEY:\n")
    print(f"  {'#':<4} {'Adres':<36} {'Win%':<8} {'AvgPnL%':<10} {'Trades':<8} {'Score'}")
    print(f"  {'─' * 68}")
    for i, (_, r) in enumerate(df.head(10).iterrows(), 1):
        print(f"  {i:<4} {r['address']:<36} {str(r['win_rate']):<8} {str(r['avg_pnl_pct']):<10} {int(r['closed_trades']):<8} {r['smart_score']}")
    log(f"\nWyniki: {OUTPUT_CSV}")
    print("=" * 62)

if __name__ == "__main__":
    main()
