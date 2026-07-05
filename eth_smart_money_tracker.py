"""
Śledzenie portfeli grubasów v2.0 (ETH)
========================================
Nowa metodologia — koniec zgadywania:

KROK 1: Kraken ETHUSD → pompy (+8% w 72h)
KROK 2: Etherscan getblocknobytime → zakres bloków 4-28h przed pompą
KROK 3: Etherscan getlogs na puli Uniswap V3 WETH/USDC → PRAWDZIWE swapy.
        Kupno ETH za USDC widać wprost w evencie — zero interpretacji.
KROK 4: eth_getTransactionByHash → kto faktycznie zlecił swap (EOA)
KROK 5: tokentx (USDC+USDT) per kandydat → rotacje stable↔crypto
KROK 6: Win rate rotacji: ucieczka do stable przed spadkiem ≥3% w 7 dni = WIN,
        wejście z stable przed wzrostem ≥3% w 7 dni = WIN.

Wymaga: ETHERSCAN_API_KEY w zmiennych środowiskowych (GitHub secret).
"""

import requests, pandas as pd, numpy as np, time, json, os, sys
from datetime import datetime, timezone, timedelta
from tqdm import tqdm

# ════════════════════════════════════════════════
TOP_PUMPS          = 15      # ile pompów analizujemy
MIN_PUMP_PCT       = 8       # min % wzrostu w 72h
WINDOW_BEFORE_H    = 28      # okno przed pompą: od -28h...
WINDOW_AFTER_H     = 4       # ...do -4h
MIN_SWAP_USD       = 10_000  # min wielkość swapa (kupno ETH) żeby zostać kandydatem
TOP_BUYERS_PER_PUMP = 30     # ilu największych kupujących per pompa
MAX_CANDIDATES     = 100     # max adresów do pełnej analizy
MIN_ROTATION_USD   = 1_000   # min wielkość rotacji stable
ROTATION_HORIZON_D = 7       # horyzont oceny rotacji (dni)
MIN_MOVE_PCT       = 3.0     # min |ruch ceny| żeby rotacja była "rozstrzygnięta"
MIN_DECISIVE       = 3       # min rozstrzygniętych rotacji żeby wejść do rankingu
SLEEP_ETHERSCAN    = 0.38    # 3 req/s limit → ~2.6 req/s bezpiecznie
OUTPUT_CSV         = "grubasy_ranking.csv"
PRICE_CACHE_FILE   = "eth_price_cache.json"
PUMPS_CACHE_FILE   = "pumps_cache.json"
# ════════════════════════════════════════════════

API_KEY  = os.environ.get("ETHERSCAN_API_KEY", "").strip()
BASE_URL = "https://api.etherscan.io/v2/api"
CHAIN_ID = 1

HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) grubasy-tracker/2.0"}

# Uniswap V3 WETH/USDC 0.05% — największa płynność ETH/stable na mainnecie
POOL_WETH_USDC = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"

USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
USDT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"

# Adresy do wykluczenia: routery, znane hot wallety giełd, znane boty MEV
EXCLUDED = {a.lower() for a in [
    "0xE592427A0AEce92De3Edee1F18E0157C05861564",  # Uniswap V3 Router
    "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",  # Uniswap Router 2
    "0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD",  # Uniswap Universal Router
    "0x1111111254EEB25477B68fb85Ed929f73A960582",  # 1inch v5
    "0xDef1C0ded9bec7F1a1670819833240f027b25EfF",  # 0x Exchange Proxy
    "0x28C6c06298d514Db089934071355E5743bf21d60",  # Binance 14
    "0xDFd5293D8e347dFe59E90eFd55b2956a1343963d",  # Binance 15
    "0x9696f59E4d72E237BE84fFD425DCaD154Bf96976",  # Binance 16
    "0x21a31Ee1afC51d94C2eFcCAa2092aD1028285549",  # Binance 17
    "0x56Eddb7aa87536c09CCc2793473599fD21A8b17F",  # Binance 18
    "0xae2Fc483527B8EF99EB5D9B44875F005ba1FaE13",  # jaredfromsubway (MEV)
    "0x74de5d4FCbf63E00296fd95d33236B9794016631",  # MetaMask router
]}

def log(msg): print(f"  {msg}", flush=True)

# ── Etherscan helper ─────────────────────────────────────────────────────────

def etherscan(params, retries=3):
    """Jedno wywołanie Etherscan V2 z rate limitem i retry."""
    params = {**params, "chainid": CHAIN_ID, "apikey": API_KEY}
    for attempt in range(retries):
        try:
            r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=25)
            time.sleep(SLEEP_ETHERSCAN)
            if r.status_code == 200:
                data = r.json()
                # Etherscan zwraca status "0" też przy pustych wynikach — to nie błąd
                if data.get("status") == "0" and "rate limit" in str(data.get("result", "")).lower():
                    time.sleep(2)
                    continue
                return data
        except Exception:
            time.sleep(1.5)
    return None

def to_int256(hex_word):
    v = int(hex_word, 16)
    if v >= 2**255:
        v -= 2**256
    return v

# ── Cache ─────────────────────────────────────────────────────────────────────

def load_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except:
        return default

def save_json(path, obj):
    with open(path, "w") as f:
        json.dump(obj, f)

# ── KROK 1: Pompy ETH (Kraken) ───────────────────────────────────────────────

def get_eth_price_history():
    log("Pobieram historię ceny ETH (Kraken, dzienna)...")
    try:
        r = requests.get(
            "https://api.kraken.com/0/public/OHLC",
            params={"pair": "ETHUSD", "interval": 1440},
            headers=HEADERS, timeout=20
        )
        if r.status_code == 200:
            result = r.json().get("result", {})
            key = next((k for k in result if k != "last"), None)
            if key:
                prices = [(int(row[0]), float(row[4])) for row in result[key]]
                log(f"Pobrano {len(prices)} dni historii")
                return prices
    except Exception as e:
        log(f"Błąd: {e}")
    return []

def find_pumps(prices, min_pct=MIN_PUMP_PCT, window_days=3, top_n=TOP_PUMPS):
    pumps = []
    for i in range(len(prices) - window_days):
        ts_s, p_s = prices[i]
        ts_e, p_e = prices[i + window_days]
        pct = (p_e - p_s) / p_s * 100
        if pct >= min_pct:
            pumps.append((ts_s, ts_e, round(pct, 1), p_s, p_e))
    pumps.sort(key=lambda x: -x[2])
    deduped, used = [], set()
    for p in pumps:
        if not any(abs(p[0] - u) < 86400 * window_days for u in used):
            deduped.append(p)
            used.add(p[0])
        if len(deduped) >= top_n:
            break
    log(f"Znaleziono {len(deduped)} pompów ETH ≥{min_pct}%:")
    for p in deduped:
        dt = datetime.fromtimestamp(p[0], tz=timezone.utc).strftime("%Y-%m-%d")
        log(f"  {dt}  +{p[2]}%  (${int(p[3])} → ${int(p[4])})")
    return deduped

# ── KROK 2+3+4: Kandydaci z prawdziwych swapów ───────────────────────────────

def get_block_by_time(ts, closest="before"):
    d = etherscan({"module": "block", "action": "getblocknobytime",
                   "timestamp": ts, "closest": closest})
    try:
        return int(d["result"])
    except:
        return None

def get_pump_buyers(ts_pump):
    """
    Zwraca {address: max_usd_buy} — adresy które kupiły ETH za ≥MIN_SWAP_USD
    w oknie przed pompą, na podstawie PRAWDZIWYCH eventów Swap.
    """
    ts_from = ts_pump - WINDOW_BEFORE_H * 3600
    ts_to   = ts_pump - WINDOW_AFTER_H  * 3600

    b_from = get_block_by_time(ts_from, "after")
    b_to   = get_block_by_time(ts_to,   "before")
    if not b_from or not b_to or b_from >= b_to:
        return {}

    # Zbierz swapy z puli (paginacja getlogs)
    big_buys = {}  # txhash → usd_size
    for page in range(1, 11):  # max 10 stron × 1000 logów
        d = etherscan({"module": "logs", "action": "getLogs",
                       "address": POOL_WETH_USDC, "topic0": SWAP_TOPIC,
                       "fromBlock": b_from, "toBlock": b_to,
                       "page": page, "offset": 1000})
        logs = d.get("result", []) if d else []
        if not isinstance(logs, list) or not logs:
            break
        for lg in logs:
            data_hex = lg.get("data", "0x")[2:]
            if len(data_hex) < 128:
                continue
            amount0 = to_int256(data_hex[0:64])    # USDC (6 dec)
            amount1 = to_int256(data_hex[64:128])  # WETH (18 dec)
            # Kupno ETH: pula dostaje USDC (amount0>0), wysyła WETH (amount1<0)
            if amount0 > 0 and amount1 < 0:
                usd = amount0 / 1e6
                if usd >= MIN_SWAP_USD:
                    txh = lg.get("transactionHash")
                    if txh and (txh not in big_buys or big_buys[txh] < usd):
                        big_buys[txh] = usd
        if len(logs) < 1000:
            break

    # Największe kupna → kto zlecił (tx.from)
    top_txs = sorted(big_buys.items(), key=lambda x: -x[1])[:TOP_BUYERS_PER_PUMP]
    buyers = {}
    for txh, usd in top_txs:
        d = etherscan({"module": "proxy", "action": "eth_getTransactionByHash",
                       "txhash": txh})
        try:
            frm = d["result"]["from"].lower()
            if frm not in EXCLUDED:
                if frm not in buyers or buyers[frm] < usd:
                    buyers[frm] = usd
        except:
            pass
    return buyers

# ── KROK 5: Rotacje stable per adres ─────────────────────────────────────────

def get_stable_transfers(address):
    """Wszystkie transfery USDC+USDT adresu ≥ MIN_ROTATION_USD."""
    transfers = []
    for contract, decimals in [(USDC, 6), (USDT, 6)]:
        d = etherscan({"module": "account", "action": "tokentx",
                       "address": address, "contractaddress": contract,
                       "startblock": 0, "endblock": 99999999,
                       "page": 1, "offset": 1000, "sort": "asc"})
        rows = d.get("result", []) if d else []
        if not isinstance(rows, list):
            continue
        for t in rows:
            try:
                usd = int(t["value"]) / 10**decimals
                if usd < MIN_ROTATION_USD:
                    continue
                direction = "IN" if t["to"].lower() == address.lower() else "OUT"
                transfers.append({
                    "ts": int(t["timeStamp"]),
                    "usd": usd,
                    "direction": direction,  # IN = dostał stable (risk-off), OUT = wydał stable (risk-on)
                })
            except:
                continue
    transfers.sort(key=lambda x: x["ts"])
    return transfers

# ── KROK 6: Ocena rotacji ────────────────────────────────────────────────────

def build_price_index(prices):
    """dict: date_str → close oraz posortowana lista ts."""
    idx = {}
    for ts, p in prices:
        d = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        idx[d] = p
    return idx

def price_on(ts, price_idx, offset_days=0):
    d = (datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(days=offset_days)).strftime("%Y-%m-%d")
    return price_idx.get(d)

def analyze_address(address, transfers, price_idx, discovered_usd):
    if not transfers:
        return None

    decisive = []   # rozstrzygnięte rotacje
    last_rotation, last_rotation_ts = None, None

    for t in transfers:
        p0 = price_on(t["ts"], price_idx)
        p7 = price_on(t["ts"], price_idx, ROTATION_HORIZON_D)
        last_rotation    = "risk-off" if t["direction"] == "IN" else "risk-on"
        last_rotation_ts = t["ts"]
        if not p0 or not p7:
            continue
        move = (p7 - p0) / p0 * 100
        if abs(move) < MIN_MOVE_PCT:
            continue  # rotacja nierozstrzygnięta — rynek stał w miejscu

        if t["direction"] == "IN":   # risk-off: uciekł do stable
            win, captured = (move < 0), -move   # uniknięty spadek
        else:                         # risk-on: wszedł z stable
            win, captured = (move > 0), move    # złapany wzrost

        decisive.append({"win": win, "captured": captured if win else -abs(move),
                         "usd": t["usd"], "entry_price": p0,
                         "direction": t["direction"]})

    if len(decisive) < MIN_DECISIVE:
        return None

    wins        = [r for r in decisive if r["win"]]
    win_rate    = len(wins) / len(decisive) * 100
    avg_captured = float(np.mean([r["captured"] for r in decisive]))
    risk_on_entries = [r["entry_price"] for r in decisive if r["direction"] == "OUT"]
    avg_entry   = float(np.mean(risk_on_entries)) if risk_on_entries else None
    confidence  = min(len(decisive) / 8, 1.0)
    smart_score = (win_rate * 0.5 + float(np.clip(avg_captured, -100, 200)) * 0.3) * confidence

    return {
        "address":            address,
        "total_txs":          len(transfers),
        "closed_trades":      len(decisive),          # = rozstrzygnięte rotacje
        "win_rate":           round(win_rate, 1),
        "avg_pnl_pct":        round(avg_captured, 1), # śr. złapany ruch
        "total_profit_usd":   round(sum(r["usd"] for r in wins), 0),
        "avg_buy_price":      round(avg_entry, 0) if avg_entry else None,
        "smart_score":        round(smart_score, 2),
        "discovered_inflow_btc": round(discovered_usd, 0),  # nazwa kolumny bez zmian (kompatybilność), wartość = USD
        "last_rotation":      last_rotation,
        "last_rotation_date": datetime.fromtimestamp(last_rotation_ts, tz=timezone.utc).strftime("%Y-%m-%d") if last_rotation_ts else None,
    }

# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 62)
    print("  ŚLEDZENIE PORTFELI GRUBASÓW v2.0 (ETH)")
    print("  Metodologia: prawdziwe swapy DEX + timing rotacji stable")
    print("=" * 62 + "\n")

    if not API_KEY:
        log("BŁĄD: brak ETHERSCAN_API_KEY w zmiennych środowiskowych.")
        log("GitHub: Settings → Secrets → Actions → ETHERSCAN_API_KEY")
        sys.exit(1)

    # Test API
    d = etherscan({"module": "stats", "action": "ethprice"})
    ok = d and d.get("status") == "1"
    log(f"Etherscan API: {'OK ✓' if ok else 'BŁĄD — sprawdź klucz'}")
    if not ok:
        sys.exit(1)

    prices = get_eth_price_history()
    if not prices:
        log("Brak historii ceny — przerywam.")
        sys.exit(1)
    price_idx = build_price_index(prices)

    print()
    pumps = find_pumps(prices)
    if not pumps:
        log("Brak pompów.")
        pd.DataFrame().to_csv(OUTPUT_CSV, index=False)
        return

    # Kandydaci (z cache — okna historyczne się nie zmieniają)
    pumps_cache = load_json(PUMPS_CACHE_FILE, {})
    candidates = {}  # addr → max discovered buy USD

    print()
    log("Szukam kupujących przed pompami (prawdziwe swapy Uniswap V3)...")
    for pump in pumps:
        ts_pump, _, pct, _, _ = pump
        key = str(ts_pump)
        dt = datetime.fromtimestamp(ts_pump, tz=timezone.utc).strftime("%Y-%m-%d")

        if key in pumps_cache:
            buyers = pumps_cache[key]
            log(f"Pompa {dt} +{pct}% — z cache ({len(buyers)} kupujących)")
        else:
            log(f"Pompa {dt} +{pct}% — skanuję swapy...")
            buyers = get_pump_buyers(ts_pump)
            pumps_cache[key] = buyers
            save_json(PUMPS_CACHE_FILE, pumps_cache)
            log(f"  Znaleziono {len(buyers)} dużych kupujących (≥${MIN_SWAP_USD:,})")

        for addr, usd in buyers.items():
            if addr not in candidates or candidates[addr] < usd:
                candidates[addr] = usd

    top = sorted(candidates.items(), key=lambda x: -x[1])[:MAX_CANDIDATES]
    log(f"\nUnikalnych kandydatów: {len(candidates)}, analizuję top {len(top)}\n")

    results = []
    for addr, disc_usd in tqdm(top, desc="Analiza rotacji"):
        transfers = get_stable_transfers(addr)
        res = analyze_address(addr, transfers, price_idx, disc_usd)
        if res:
            results.append(res)

    if not results:
        log("Brak adresów z wystarczającą liczbą rozstrzygniętych rotacji.")
        pd.DataFrame(columns=["rank","address","total_txs","closed_trades","win_rate",
                               "avg_pnl_pct","total_profit_usd","avg_buy_price",
                               "smart_score","discovered_inflow_btc",
                               "last_rotation","last_rotation_date"]).to_csv(OUTPUT_CSV, index=False)
        return

    df = pd.DataFrame(results).sort_values("smart_score", ascending=False)
    df.insert(0, "rank", range(1, len(df) + 1))
    df.to_csv(OUTPUT_CSV, index=False)

    print("\n" + "=" * 62)
    log(f"Kandydaci zbadani: {len(top)}")
    log(f"W rankingu (≥{MIN_DECISIVE} rozstrzygniętych rotacji): {len(results)}")
    log(f"\nTOP 10 GRUBASÓW:\n")
    print(f"  {'#':<4} {'Adres':<44} {'Win%':<8} {'Śr.ruch%':<10} {'Rotacje':<9} {'Score'}")
    print(f"  {'─' * 82}")
    for i, (_, r) in enumerate(df.head(10).iterrows(), 1):
        print(f"  {i:<4} {r['address']:<44} {str(r['win_rate']):<8} {str(r['avg_pnl_pct']):<10} {int(r['closed_trades']):<9} {r['smart_score']}")
    log(f"\nWyniki: {OUTPUT_CSV}")
    print("=" * 62)

if __name__ == "__main__":
    main()
