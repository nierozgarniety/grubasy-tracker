"""
Śledzenie portfeli grubasów v0.1.2
- Ceny BTC: Binance API (darmowe, bez limitu, ceny godzinowe)
- Transakcje: mempool.space API
- Wynik: grubasy_ranking.csv
"""

import requests, pandas as pd, numpy as np, time, json, os, sys
from datetime import datetime, timezone
from tqdm import tqdm

# ════════════════════════════════════════════════
TOP_N            = 80
MAX_TX_PER_ADDR  = 50
MIN_BTC_FLOW     = 0.01
SLEEP_MEMPOOL    = 0.35
OUTPUT_CSV       = "grubasy_ranking.csv"
CACHE_FILE       = "grubasy_price_cache.json"
# ════════════════════════════════════════════════

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

WHALE_ADDRESSES = [
    "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo",
    "bc1qgdjqv0av3q56jvd82tkdjpy7gdp9ut8tlqmgrpmv24sq90ecnvqqjwvw97",
    "1FeexV6bAHb8ybZjqQMjJrcCrHGW9sb6uF",
    "1LdRcdxfbSnmCYYNdeYpUnztiYzVfBEQeC",
    "1AC4fMwgY8j9onSbXEWeH6Zan8QGMSdmtA",
    "1PnMfRF2enSZnR6JSexxBHuQnxG8Vo5FVX",
    "1KYiKJEfdJtap9QX2v9BXJMpz2SfU4pgZw",
    "1LruNZjwamWJXThX2Y8C2d47QqhAkkc5os",
    "12tkqA9xSoowkzoERHMWNKsTey55YEBqkv",
    "1P5ZEDWTKTFGxQjZphgWPQUpe554WKDfHQ",
    "bc1qa5wkgaew2dkv56kfvj49j0av5nml45x9ek9hz6",
    "1Ay8aZnAMPTCRJmBsHNSRnFMmEq3RR5qAZ",
    "1FzWLkAahHooV3kzTgyx6qsswXJ6sCXkSR",
    "1CXyk3bBpihTtqCaFkZFSLmoNbHFGBYzAG",
    "1GR9qNz7zgtaW5HwwVpEJWMnGWhsbsieCG",
    "1L07f8s9kS1c9qyFz9hJBPbFi7xFKEfMKr",
    "1FBPzxps6gNPpPBFkKbdMBiK4mDNZxVEFq",
    "1EHNa6Q4Jz2uvNExL497mE43ikXhwF6kZm",
    "1HQ3Go3ggs8pFnXuHVHRytPCq5fGG8Hbhx",
    "1FWQiwK27EnGXb6BiBMRLJvunJQZZPMcGd",
    "17A16QmavnUfCW11DAApiJxp7ARnxN5pGX",
    "15ubicBBWFnvoZLT7GiU2qxjRaKJPdkDMG",
    "1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s",
    "3D2oetdNuZUqQHPJmcMDDHYoqkyNVsFk9r",
    "1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v",
    "12ib7dApVFvg82TXKycWBNpN8kFyiAN1dr",
    "14NWDXkQD89g9DHFDRrYicoqSxhFZ7BFoF",
    "1MgpzbpFNF8yXkzfmMNfkTM7Fk3ESPH5bH",
    "1CWmK9dMJK4S5RFcr1EUzFAaGCNuLSRuAC",
    "1LcnbkmTWs9nJ8hEnRtVSXV3tXkBe4CTBS",
    "17rmTnbGFE2ZZKRB95RNDCfCcaMGfbf7qS",
    "3LYJfcfHkxYkNQu31nGaU1MKXS4Z3ZGML",
    "3FHNBLobJnbCPGo3nevw5UD5jQjSgRiDHm",
    "bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h",
    "1GdK9UzpHBzqL7CAY2QGAfPr6ZqwKfTP8",
    "1CCqfPToSHMDSS7FHPcjGjfJv5HVHAfKFY",
    "12t9YDPgwueZ9NyMgw519p7AA8isjr6SMw",
    "1HckjUpRGcrrRAtFaaCAUaGjsPx9oYmLaZ",
    "1MXNsZJp5RBkFQZXRxyMfNNd5235ioFy3e",
    "3Kzh9qAqVWQhEsfQz7zEQL1EuSx5tyNLNS",
    "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
    "1FoWyxwPXuj4C6abqwhjDWdz6D4PZgYRjA",
    "35hK24tcLEWcgNA4JxpvbkNkoAcDGqQPsP",
    "3E5bEQAFqoQjbbW4waBhBpFEzCsQBRBMCi",
    "1LQoWist8KkaUXSPKZHNvEyfrEkPHzSsCd",
    "1P5ZEDWTKTFGxQjZphgWPQUpe554WKDfHQ",
    "1EM4e8eu2S93MKTMDj7EKZB5q6Kej6eDe7",
    "385cR5DM96n1HvBDMzLHPYcw89fZAXULJP",
    "1CounterpartyXXXXXXXXXXXXXXXUWLpVr",
    "1DkyBEKt5S2GDtv7aQw6rQepAvnsRyHoYM",
    "3MbYQ55HL8WRoGKM5BaGBVjCHf4dHbKXyg",
    "bc1q4s8yps9my6hun2tpd5ke5nbqmznoeazorlhqhq",
    "1L2GM8eE7mJWLdo3HZS6su1832NX2txaac",
    "3LQUu4v9NvKmESWA2G2UBGy94MEsHE2U4K",
    "3E35SFZkfLMGo4qX5aVs1oCKRRuLKRCZiS",
    "17Vu7st1U1KwymZKtuSiMLfDoCxG6oAFTm",
    "1BpEi6DfDAUFd153wiGrvkiubFb4PmKbkx",
    "16ftSEQ4ctQFDtVZiUBusQUjRrGhM3JYwe",
    "3AAzK4Xbu8PTM8AD7gMLargefmFFHLyVGX",
    "bc1qhm6697d9d2224vfyt8mj4kw03ncec7a7fdafgt",
    "1AJbsFZ64EpEfS5UAjAfcUG8pH8Jn3rn1F",
    "1Bh9JwSMF9eoQM3bJwExCBbCjJFyNr6hiZ",
    "bc1qjasf9z3h7ex55vynatne4s0a6qe7keq0sxfhnm",
    "1dice8EMZmqKvrGE4Qc9bUFKqEsnKGRNRR",
    "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
    "1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v",
    "3LCGsSmfr24demGvriN4e3ft8wEcDuHFqh",
    "bc1qd4ysezhmypwty5dnyvari9t3hu3xp2s9mtg2dm",
    "1LoVGDgRs9hTfTNJNuXKSpywcbdvwRXpmK",
    "3QW9oHJqvUwnSKMGlBGDdKNTVoFtPSSJe8",
    "1LdRcdxfbSnmCYYNdeYpUnztiYzVfBEQeC",
    "bc1qrxqjp6zvcs3vkqhzs7ywzs2t53swlqnhpn0fxq",
    "1Jd3ktZFCNMDKBXDuQHEGpSHM9D8aDanmz",
    "3NukJ6fYZJ5Kk8bPjycAnruZkE5Q7UW7i8",
    "1Q7RBzFDzP8G2GQSgJbkuHMtwJ9khJ7aF9",
    "bc1qcu4xhsvgjzwxl9rj2rp9a4djqjqvq6g3vqjkhx",
    "3Nxwenay9Z8Lc9JBiywExpnEFiLp6Afp8v",
    "1JCe8z4jJVNXSjohjFcFtkfuQYd7AxBhTm",
]

# Deduplicate
seen, WHALE_ADDRESSES_CLEAN = set(), []
for a in WHALE_ADDRESSES:
    a = a.strip()
    if a not in seen and len(a) >= 26 and ' ' not in a:
        seen.add(a)
        WHALE_ADDRESSES_CLEAN.append(a)

def ts_to_date(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")

def btc(sats):
    return sats / 1e8

# ── Ceny BTC z Binance (godzinowe klines) ────────────────────────────────────

def load_cache():
    try:
        with open(CACHE_FILE) as f:
            return json.load(f)
    except:
        return {}

def save_cache(cache):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)

def get_price_binance(timestamp_ms, cache):
    """
    Pobiera cenę BTC/USDT z Binance dla danego timestamp (ms).
    Używa klines 1h — zwraca cenę zamknięcia świecy.
    Cache key = timestamp zaokrąglony do godziny.
    """
    # Zaokrąglij do godziny
    hour_ts = (timestamp_ms // 3_600_000) * 3_600_000
    key = str(hour_ts)

    if key in cache:
        return cache[key]

    try:
        url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol":    "BTCUSDT",
            "interval":  "1h",
            "startTime": hour_ts,
            "limit":     1
        }
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if data:
                price = float(data[0][4])  # close price
                cache[key] = price
                save_cache(cache)
                return price
    except Exception:
        pass
    return None

# ── Transakcje mempool.space ─────────────────────────────────────────────────

def get_address_txs(address):
    url = f"https://mempool.space/api/address/{address}/txs"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 200:
            time.sleep(SLEEP_MEMPOOL)
            return r.json()[:MAX_TX_PER_ADDR]
    except Exception:
        pass
    time.sleep(SLEEP_MEMPOOL)
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
            if inp.get("prevout", {}).get("scriptpubkey_address") == address
        )
        received = sum(
            out.get("value", 0)
            for out in tx.get("vout", [])
            if out.get("scriptpubkey_address") == address
        )
        net_btc = btc(received - spent)
        if abs(net_btc) < MIN_BTC_FLOW:
            continue
        flows.append({
            "timestamp":    block_time,
            "timestamp_ms": block_time * 1000,
            "date":         ts_to_date(block_time),
            "net_btc":      net_btc,
            "direction":    "IN" if net_btc > 0 else "OUT"
        })
    return flows

# ── Analiza portfela ─────────────────────────────────────────────────────────

def analyze(address, flows, cache):
    if not flows:
        return None

    flows = sorted(flows, key=lambda x: x["timestamp"])

    # Pobierz ceny (Binance, godzinowe)
    for f in flows:
        f["price"] = get_price_binance(f["timestamp_ms"], cache)

    flows = [f for f in flows if f["price"]]
    if not flows:
        return None

    # FIFO matching IN → OUT
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
                trades.append({
                    "buy_date":   buy["date"],
                    "sell_date":  f["date"],
                    "btc":        matched,
                    "buy_price":  buy["price"],
                    "sell_price": f["price"],
                    "pnl_pct":    pnl_pct,
                    "profit_usd": matched * (f["price"] - buy["price"]),
                    "win":        pnl_pct > 0
                })
                buy["btc"] -= matched
                remaining  -= matched
                if buy["btc"] < 0.0001:
                    buy_stack.pop(0)

    total_in  = sum(f["net_btc"] for f in flows if f["direction"] == "IN")
    total_out = sum(abs(f["net_btc"]) for f in flows if f["direction"] == "OUT")
    base = {
        "address":       address,
        "total_txs":     len(flows),
        "total_btc_in":  round(total_in,  4),
        "total_btc_out": round(total_out, 4),
    }

    if not trades:
        avg_buy = np.mean([f["price"] for f in flows if f["direction"] == "IN"]) if any(f["direction"] == "IN" for f in flows) else None
        return {**base, "status": "HODLER", "closed_trades": 0,
                "win_rate": None, "avg_pnl_pct": None, "total_profit_usd": None,
                "avg_buy_price": round(avg_buy, 0) if avg_buy else None, "smart_score": 0}

    win_rate    = len([t for t in trades if t["win"]]) / len(trades) * 100
    avg_pnl     = np.mean([t["pnl_pct"] for t in trades])
    tot_profit  = sum(t["profit_usd"] for t in trades)
    confidence  = min(len(trades) / 8, 1.0)
    smart_score = (win_rate * 0.5 + np.clip(avg_pnl, -100, 200) * 0.3) * confidence

    return {**base,
        "status":           "TRADER",
        "closed_trades":    len(trades),
        "win_rate":         round(win_rate, 1),
        "avg_pnl_pct":      round(avg_pnl, 1),
        "total_profit_usd": round(tot_profit, 0),
        "avg_buy_price":    round(np.mean([t["buy_price"] for t in trades]), 0),
        "smart_score":      round(smart_score, 2)
    }

# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 62)
    print("  ŚLEDZENIE PORTFELI GRUBASÓW v0.1.2")
    print("  Ceny: Binance API (godzinowe, bez limitu)")
    print("=" * 62)

    addresses = WHALE_ADDRESSES_CLEAN[:TOP_N]
    print(f"\n  Załadowano {len(addresses)} adresów wielorybów")
    print(f"  Pobieram transakcje i ceny...\n")

    cache = load_cache()
    cached_prices = len(cache)
    results = []

    for i, addr in enumerate(tqdm(addresses, desc="Analiza")):
        txs   = get_address_txs(addr)
        flows = parse_flows(addr, txs)
        res   = analyze(addr, flows, cache)
        if res:
            results.append(res)
        if i % 10 == 0:
            save_cache(cache)

    save_cache(cache)

    if not results:
        print("\nBrak wyników — sprawdź połączenie z internetem.")
        return

    df      = pd.DataFrame(results)
    traders = df[df["status"] == "TRADER"].sort_values("smart_score", ascending=False).copy()
    hodlers = df[df["status"] == "HODLER"].copy()
    final   = pd.concat([traders, hodlers], ignore_index=True)
    final.insert(0, "rank", range(1, len(final) + 1))
    final.to_csv(OUTPUT_CSV, index=False)

    new_cached = len(cache) - cached_prices

    print("\n" + "=" * 62)
    print(f"  Przeanalizowano:  {len(results)} adresów")
    print(f"  Traderzy:         {len(traders)}")
    print(f"  Hodlerzy:         {len(hodlers)}")
    print(f"  Nowych cen w cache: {new_cached}")

    if not traders.empty:
        print(f"\n  TOP 10 SMART MONEY:\n")
        print(f"  {'#':<4} {'Adres':<36} {'Win%':<8} {'AvgPnL%':<10} {'Trades':<8} {'Score'}")
        print(f"  {'─' * 68}")
        for i, (_, r) in enumerate(traders.head(10).iterrows(), 1):
            print(f"  {i:<4} {r['address']:<36} {str(r['win_rate']):<8} {str(r['avg_pnl_pct']):<10} {int(r['closed_trades']):<8} {r['smart_score']}")

    print(f"\n  Wyniki: {OUTPUT_CSV}")
    print("=" * 62)

if __name__ == "__main__":
    main()
