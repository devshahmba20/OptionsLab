#!/usr/bin/env python3
"""
Options Lab - Phase 2 (bhavcopy -> Parquet -> Calendar dashboard)

Setup (ek j vaar):   pip install duckdb
Step 1 download+convert (2019 thi aaj sudhi, roj auto): python optlab.py update
Step 2 dashboard:    python optlab.py serve      -> browser ma http://localhost:8000

'build' fakat nava CSV j convert kare (pehla thi hoy te skip), etle roj chalavo to chale.
Parquet files: data/parquet/<EXCH>_<YYYYMMDD>.parquet
"""
import sys, os, re, json, glob, argparse, math
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import duckdb
import base64
AUTH_USER = os.environ.get("OL_USER", "Option Lab")     # Render > Environment ma OL_USER / OL_PASS mukjo
AUTH_PASS = os.environ.get("OL_PASS", "1985")

RAW, PQ = "bhavcopy", os.path.join("data", "parquet")

# ---------------------------------------------------------------- BUILD
IPQ = os.path.join("data", "idx")
AR = os.path.join("data", "archive")          # compact monthly files (python optlab.py compact)
ARJ = os.path.join(AR, "dates.json")
NEW_SQL = """SELECT '{x}' AS exch, CAST(TradDt AS DATE) AS date, TckrSymb AS sym,
    CASE FinInstrmTp WHEN 'IDO' THEN 'idx' ELSE 'stk' END AS kind, CAST(XpryDt AS DATE) AS exp,
    CAST(StrkPric AS DOUBLE) AS strike, OptnTp AS ot,
    COALESCE(NULLIF(TRY_CAST(ClsPric AS DOUBLE),0), TRY_CAST(SttlmPric AS DOUBLE), 0) AS close,
    TRY_CAST(UndrlygPric AS DOUBLE) AS und, COALESCE(TRY_CAST(OpnIntrst AS BIGINT),0) AS oi,
    COALESCE(TRY_CAST(TtlTradgVol AS BIGINT),0) AS vol, TRY_CAST(NewBrdLotQty AS INTEGER) AS lot
    FROM read_csv(?, all_varchar=true, header=true, ignore_errors=true)
    WHERE FinInstrmTp IN ('IDO','STO') AND OptnTp IN ('CE','PE')"""
NEW_SQL = """SELECT '{x}' AS exch, CAST(TradDt AS DATE) AS date, TckrSymb AS sym,
    CASE FinInstrmTp WHEN 'IDO' THEN 'idx' ELSE 'stk' END AS kind, CAST(XpryDt AS DATE) AS exp,
    CAST(StrkPric AS DOUBLE) AS strike, OptnTp AS ot,
    COALESCE(NULLIF(TRY_CAST(ClsPric AS DOUBLE),0), TRY_CAST(SttlmPric AS DOUBLE), 0) AS close,
    TRY_CAST(UndrlygPric AS DOUBLE) AS und
    FROM read_csv(?, all_varchar=true, header=true, ignore_errors=true)
    WHERE FinInstrmTp IN ('IDO','STO') AND OptnTp IN ('CE','PE')
    AND COALESCE(TRY_CAST(TtlTradgVol AS BIGINT),0) > 0"""
def _rm(f, keep):
    if not keep:
        try: os.remove(f)
        except OSError: pass

def build(keep=False):
    """CSV -> Parquet. Juna (pre Jul-2024) ane nava (UDiFF) bane format auto-detect. CSV convert pachi delete (--keep-csv na hoy to)."""
    os.makedirs(PQ, exist_ok=True)
    con = duckdb.connect()
    done = skip = bad = 0
    for exch in ("NSE", "BSE"):
        for f in sorted(glob.glob(os.path.join(RAW, exch, "*.csv"))):
            m = re.search(r"(\d{8})", os.path.basename(f))
            if not m:
                continue
            out = os.path.join(PQ, f"{exch}_{m.group(1)}.parquet")
            if os.path.exists(out):
                skip += 1; _rm(f, keep); continue
            try:
                cols = {r[0] for r in con.execute("DESCRIBE SELECT * FROM read_csv(?, all_varchar=true, header=true, ignore_errors=true)", [f]).fetchall()}
                sel = NEW_SQL if "TckrSymb" in cols else OLD_SQL if "SYMBOL" in cols else None
                if not sel:
                    raise ValueError("unknown CSV format")
                con.execute(f"COPY ({sel.format(x=exch)}) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)", [f])
                n = con.execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
                if n == 0:
                    os.remove(out); bad += 1; print(f"{os.path.basename(f)}: option rows nathi")
                else:
                    done += 1; _rm(f, keep)
            except Exception as e:
                bad += 1
                if os.path.exists(out): os.remove(out)
                print(f"{os.path.basename(f)}: ERROR {e}")
    print(f"Option files: converted {done}, already {skip}, problem {bad}")
    build_idx(con, keep)

def build_idx(con, keep=False):
    """NSE index close file (Nifty, India VIX...) -> data/idx/*.parquet"""
    os.makedirs(IPQ, exist_ok=True); n = 0
    for f in sorted(glob.glob(os.path.join(RAW, "IDX", "*.csv"))):
        m = re.search(r"(\d{8})", os.path.basename(f))
        if not m: continue
        t = m.group(1); out = os.path.join(IPQ, f"IDX_{t}.parquet")
        if os.path.exists(out):
            _rm(f, keep); continue
        try:
            con.execute(f"""COPY (SELECT CAST('{t[:4]}-{t[4:6]}-{t[6:]}' AS DATE) AS date, trim(index_name) AS name,
                TRY_CAST(closing_index_value AS DOUBLE) AS close
                FROM read_csv(?, all_varchar=true, header=true, normalize_names=true, ignore_errors=true))
                TO '{out}' (FORMAT PARQUET)""", [f]); n += 1; _rm(f, keep)
        except Exception as e:
            print("IDX", os.path.basename(f), "ERROR", e)
    print(f"Index (VIX) files converted: {n}")

# ---------------------------------------------------------------- UPDATE (history + daily)
def latest_date():
    ds = [_fd(f) for f in glob.glob(os.path.join(PQ, "NSE_*.parquet"))] + _arch_dates("NSE")
    return max(ds) if ds else None

def update(start=None, keep=False, workers=4, bse=False, retry=False):
    """Last date thi aaj sudhi download + convert. Pehli vaar 2019-01-01 thi. Monthly chunk ma, jethi disk full na thay."""
    import download_bhavcopy as dl
    from datetime import date, timedelta
    last = latest_date()
    if start: s = date.fromisoformat(start)
    elif last: s = date.fromisoformat(last) - timedelta(days=10)   # chhella 10 divas fari check (gap bharva)
    else: s = date(2019, 1, 1)
    end = date.today()
    names = ["NSE", "IDX"] + (["BSE"] if bse else [])
    print(f"Update: {s} -> {end}  ({', '.join(names)})")
    while s <= end:
        e = min(s + timedelta(days=30), end)
        print(f"\n=== {s} to {e} ===")
        dl.run(s, e, names, RAW, workers, retry)
        build(keep)
        s = e + timedelta(days=1)
    print("\nUpdate complete. Latest NSE date:", latest_date())


# ---------------------------------------------------------------- COMPACT (daily Parquet -> monthly archive, nano size)
def _mb(p): return os.path.getsize(p) / 1e6

def compact(keep_days=60, level=19, otm=0.0, no_stocks=False, minpx=0.0, delete=False):
    """data/parquet ni juni daily files -> data/archive/<EXCH>_<YYYYMM>.parquet (sorted, ZSTD). Chhella keep_days na divas daily j rahe.
    Default LOSSLESS. otm/no_stocks/minpx = rows ochhi karva (lossy). Delete fakat verify pachhi, ane --delete hoy to j."""
    from datetime import date, timedelta
    os.makedirs(AR, exist_ok=True)
    cutoff = (date.today() - timedelta(days=keep_days)).isoformat()
    flt = (["kind <> 'stk'"] if no_stocks else []) + ([f"(und IS NULL OR und <= 0 OR abs(strike / und - 1) <= {otm / 100})"] if otm else []) + ([f"close >= {minpx}"] if minpx else [])
    W = " WHERE " + " AND ".join(flt) if flt else ""
    groups = {}
    for f in glob.glob(os.path.join(PQ, "*.parquet")):
        d = _fd(f)
        if d < cutoff:
            groups.setdefault((os.path.basename(f).split("_")[0], d[:4] + d[5:7]), []).append(f)
    if not groups:
        print(f"Compact karva jevi juni daily file nathi (cutoff {cutoff}, keep-days {keep_days})."); return
    try: meta = json.load(open(ARJ))
    except Exception: meta = {}
    con = duckdb.connect(); tot_in = tot_out = 0.0; ok_files = []
    for (ex, ym), L in sorted(groups.items()):
        L = sorted(L); fin = os.path.join(AR, f"{ex}_{ym}.parquet"); tmp = fin + ".tmp"; merged = os.path.exists(fin)
        dts = {_fd(f) for f in L}
        q = f"SELECT * FROM read_parquet({_lst(L)}, union_by_name=true)"
        if os.path.exists(fin):          # pehlathi archive hoy to e mahino merge (same date replace)
            q += f" UNION ALL BY NAME SELECT * FROM read_parquet('{fin.replace(chr(92), '/')}') WHERE date NOT IN ({','.join(repr(d) for d in sorted(dts))})"
        sel = f"SELECT * FROM ({q}) t{W}"
        for opt in (f"COMPRESSION ZSTD, COMPRESSION_LEVEL {level}, PARQUET_VERSION V2, ROW_GROUP_SIZE 500000", f"COMPRESSION ZSTD, COMPRESSION_LEVEL {level}", "COMPRESSION ZSTD"):
            try:
                con.execute(f"COPY ({sel} ORDER BY sym, exp, strike, ot, date) TO '{tmp.replace(chr(92), '/')}' (FORMAT PARQUET, {opt})"); break
            except Exception as e:
                if os.path.exists(tmp): os.remove(tmp)
                last_err = e
        else:
            print(f"{ex} {ym}: ERROR {last_err}"); continue
        n1 = con.execute(f"SELECT count(*) FROM ({sel})").fetchone()[0]
        n2 = con.execute(f"SELECT count(*) FROM read_parquet('{tmp.replace(chr(92), '/')}')").fetchone()[0]
        bad = con.execute(f"SELECT count(*) FROM (SELECT * FROM ({sel}) EXCEPT ALL SELECT * FROM read_parquet('{tmp.replace(chr(92), '/')}'))").fetchone()[0]
        if n1 != n2 or bad:
            os.remove(tmp); print(f"{ex} {ym}: VERIFY FAIL ({n1} vs {n2}, diff {bad}) - skip"); continue
        os.replace(tmp, fin)
        a, b = sum(_mb(f) for f in L), _mb(fin)
        if not merged: tot_in += a; tot_out += b
        meta[ex] = sorted(set(meta.get(ex, [])) | dts); ok_files += L
        print(f"{ex} {ym}: {len(L):3d} files {a:8.1f} MB -> {b:7.1f} MB  ({a / max(b, 1e-9):.1f}x, rows {n1:,})" if not merged else f"{ex} {ym}: {len(L)} nayi file merge thai, archive have {b:.1f} MB (rows {n1:,})")
    json.dump(meta, open(ARJ, "w"))
    if tot_out:
        print(f"\nTotal: {tot_in:.0f} MB -> {tot_out:.0f} MB  ({tot_in / tot_out:.1f}x smaller)")
    if delete and ok_files:
        for f in ok_files: os.remove(f)
        print(f"{len(ok_files)} juni daily files delete thai (archive ma verify thai gayi chhe).")
    elif ok_files:
        print("Daily files haju rakhi chhe (app archive vaparshe, duplicate nahi). Size ghatadvi hoy to: --delete sathe pharī chalavo.")
    ar = sum(_mb(f) for f in glob.glob(os.path.join(AR, "*.parquet"))); dl_ = sum(_mb(f) for f in glob.glob(os.path.join(PQ, "*.parquet")))
    print(f"Hamna: archive {ar:.0f} MB + daily {dl_:.0f} MB")

# ---------------------------------------------------------------- QUERY
def _fd(f):
    t = re.search(r"_(\d{8})\.parquet$", f).group(1)
    return f"{t[:4]}-{t[4:6]}-{t[6:]}"

def _lst(fs):
    return "[" + ",".join("'" + f.replace("\\", "/").replace("'", "''") + "'" for f in fs) + "]"

def _arch_dates(exch=None):
    try:
        j = json.load(open(ARJ))
        return sorted({d for k, v in j.items() if exch in (None, k) for d in v})
    except Exception:
        return []

def all_dates():
    return sorted({_fd(f) for f in glob.glob(os.path.join(PQ, "*.parquet"))} | set(_arch_dates()))

def _fm(f):
    t = re.search(r"_(\d{6})\.parquet$", f).group(1); return f"{t[:4]}-{t[4:]}"

def db(a=None, b=None, last=0):
    """Fakat jaruri date-range ni files vanche: daily Parquet (data/parquet) + compact monthly archive (data/archive)."""
    ds = all_dates()
    if a: ds = [d for d in ds if d >= a]
    if b: ds = [d for d in ds if d <= b]
    if last: ds = ds[-last:]
    if not ds:
        raise RuntimeError("Aa date range ma Parquet data nathi. Pehla 'python optlab.py update' chalavo.")
    dset, ad = set(ds), set(_arch_dates())
    fs = [f for f in glob.glob(os.path.join(PQ, "*.parquet")) if _fd(f) in dset and _fd(f) not in ad]
    af = [f for f in glob.glob(os.path.join(AR, "*.parquet")) if ds[0][:7] <= _fm(f) <= ds[-1][:7]]
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit='{os.environ.get('OL_MEM', '300MB')}'")
        con.execute("SET threads TO 2")
        con.execute("SET preserve_insertion_order=false")
    except Exception:
        pass
    where = ""
    if af and (a or b or last):
        where = " WHERE date IN (" + ",".join(f"'{d}'" for d in ds) + ")" if last else f" WHERE date BETWEEN '{ds[0]}' AND '{ds[-1]}'"
    con.execute(f"CREATE VIEW opt AS SELECT * FROM read_parquet({_lst(fs + af)}, union_by_name=true){where}")
    ix = [f for f in glob.glob(os.path.join(IPQ, "*.parquet")) if (not a or _fd(f) >= a) and (not b or _fd(f) <= b)]
    if ix:
        con.execute(f"CREATE VIEW idx AS SELECT * FROM read_parquet({_lst(ix)}, union_by_name=true)")
    else:
        con.execute("CREATE VIEW idx AS SELECT NULL::DATE AS date, NULL::VARCHAR AS name, NULL::DOUBLE AS close WHERE false")
    return con

def api_symbols(q, con):
    lt = "any_value(lot)" if "lot" in {x[0] for x in con.execute("DESCRIBE opt").fetchall()} else "NULL"
    r = con.execute(f"SELECT sym, any_value(kind), any_value(exch), {lt} FROM opt GROUP BY sym ORDER BY 2, 1").fetchall()
    return [{"sym": a, "kind": b, "exch": c, "lot": d} for a, b, c, d in r]

def api_dates(q, con):
    return all_dates()

def api_expiries(q, con):
    r = con.execute("SELECT DISTINCT exp::VARCHAR FROM opt WHERE sym=? AND date=? AND exp>=date ORDER BY 1", [q["sym"], q["date"]]).fetchall()
    return [x[0] for x in r]

VIXNAME = "India VIX"
IDXNAMES = {"NIFTY": ["nifty 50"], "BANKNIFTY": ["nifty bank"], "FINNIFTY": ["nifty financial services", "nifty fin service"],
            "MIDCPNIFTY": ["nifty midcap select", "nifty mid select"], "NIFTYNXT50": ["nifty next 50"]}

# ---- Black-Scholes Greeks: tamari Excel (1_final.xlsm) nu j logic
#      r=0, q=0 | T=(expiry-date+1)/365 | S = darek expiry nu synthetic future | IV bisection (High 5, Low 0, 0.0001)
SQ2PI = math.sqrt(2 * math.pi)
def _Nc(x): return 0.5 * (1 + math.erf(x / math.sqrt(2)))
def _npdf(x): return math.exp(-x * x / 2) / SQ2PI
def _d1(S, K, T, r, v, q=0.0): return (math.log(S / K) + (r - q + 0.5 * v * v) * T) / (v * math.sqrt(T))
def _price(S, K, T, r, v, ot, q=0.0):
    a = _d1(S, K, T, r, v, q); b = a - v * math.sqrt(T)
    if ot == "CE": return math.exp(-q * T) * S * _Nc(a) - K * math.exp(-r * T) * _Nc(b)
    return K * math.exp(-r * T) * _Nc(-b) - math.exp(-q * T) * S * _Nc(-a)
def _iv(S, K, T, r, target, ot, q=0.0):
    """Excel VBA jevu j bisection: High=5, Low=0, tolerance 0.0001"""
    if not (S and K and T and target and target > 0): return None
    disc = K * math.exp(-r * T)
    intrinsic = max(S - disc, 0) if ot == "CE" else max(disc - S, 0)
    if target <= intrinsic: return None
    hi, lo = 5.0, 0.0
    while hi - lo > 0.0001:
        mid = (hi + lo) / 2
        if _price(S, K, T, r, mid, ot, q) > target: hi = mid
        else: lo = mid
    return (hi + lo) / 2
def _greeks(S, K, T, r, v, ot, q=0.0):
    a = _d1(S, K, T, r, v, q); sT = math.sqrt(T); nd = _npdf(a); N2 = _Nc(a - v * sT)
    delta = _Nc(a) if ot == "CE" else _Nc(a) - 1
    base = -(S * v * nd) / (2 * sT)
    theta = (base - r * K * math.exp(-r * T) * N2) / 365 if ot == "CE" else (base + r * K * math.exp(-r * T) * (1 - N2)) / 365
    return dict(delta=delta, vega=0.01 * S * sT * nd, theta=theta, gamma=nd / (S * v * sT))

def _gkx(S, K, T, r, p, ot):
    v = _iv(S, K, T, r, p, ot) if (S and p) else None
    if not v or v <= 0.0002:
        return None
    return v, _greeks(S, K, T, r, v, ot)

def _spots(con, sym, d0, d1):
    """date -> spot: pehla bhavcopy no underlying, nahi to NSE index close file"""
    sp = {d: v for d, v in con.execute("SELECT date::VARCHAR, max(und) FROM opt WHERE sym=? AND date BETWEEN ? AND ? GROUP BY 1", [sym, d0, d1]).fetchall() if v}
    names = IDXNAMES.get(sym)
    if names:
        for d, v in con.execute(f"SELECT date::VARCHAR, close FROM idx WHERE lower(name) IN ({','.join('?'*len(names))}) AND date BETWEEN ? AND ?", names + [d0, d1]).fetchall():
            sp.setdefault(d, v)
    return sp

def _est_spot(ch):
    """Put-call parity thi spot no andaaz (underlying na hoy tyare)"""
    best, sp = 1e18, None
    for k, x in ch.items():
        if "CE" in x and "PE" in x:
            df = abs(x["CE"] - x["PE"])
            if df < best: best, sp = df, k + x["CE"] - x["PE"]
    return sp

def _atm(ch, spot, gap):
    """Provisional ATM -> ATM strike used -> synthetic future -> final strike -> straddle"""
    ks = sorted(k for k, x in ch.items() if "CE" in x and "PE" in x)
    if not spot or not ks:
        return {}
    near = lambda t: min(ks, key=lambda k: abs(k - t))
    prov = round(spot / gap) * gap; atm = near(prov)
    sf = atm + ch[atm]["CE"] - ch[atm]["PE"]; fk = near(sf)
    return dict(prov=prov, atm=atm, ce=ch[atm]["CE"], pe=ch[atm]["PE"], sf=round(sf, 2), fk=fk,
                fce=ch[fk]["CE"], fpe=ch[fk]["PE"], straddle=round(ch[fk]["CE"] + ch[fk]["PE"], 2))

def _chain(con, sym, d, e):
    ch = {}
    for k, ot, c in con.execute("SELECT strike, ot, close FROM opt WHERE sym=? AND date=? AND exp=? AND close>0", [sym, d, e]).fetchall():
        ch.setdefault(k, {})[ot] = c
    return ch

def api_calendar(q, con):
    sym, d0, e1, e2 = q["sym"], q["date"], q["e1"], q["e2"]
    gap, n = float(q["gap"]), int(q["n"])
    if e2 <= e1:
        raise ValueError("Later expiry, earlier expiry thi pachi hovi joiye.")
    if gap <= 0 or not 1 <= n <= 200:
        raise ValueError("Gap > 0 ane strikes 1 thi 200 vachche rakho.")
    spots = _spots(con, sym, d0, e1)
    spot = spots.get(d0) or _est_spot(_chain(con, sym, d0, e1))
    if q.get("mode") == "manual":
        start = float(q["start"])
    else:
        if spot is None:
            raise ValueError("ATM nathi malto. Manual strike vapro.")
        start = round(spot / gap) * gap - (n // 2) * gap
    strikes = [start + i * gap for i in range(n)]
    rows = con.execute("""SELECT date::VARCHAR, strike, ot, exp::VARCHAR, close FROM opt
        WHERE sym=? AND exp IN (?,?) AND date BETWEEN ? AND ? AND strike BETWEEN ? AND ? AND close>0""",
        [sym, e1, e2, d0, e1, strikes[0], strikes[-1]]).fetchall()
    dates = sorted({r[0] for r in rows})
    rate = float(q.get("rate") or 0) / 100.0
    P = {}
    for d, k, ot, ex, c in rows:
        P.setdefault((ot, k, d), {})["e" if ex == e1 else "l"] = c
    vix = dict(con.execute("SELECT date::VARCHAR, close FROM idx WHERE lower(name)=lower(?) AND date BETWEEN ? AND ?", [VIXNAME, d0, e1]).fetchall())
    chains = {}
    for d, ex, k, ot, c in con.execute("SELECT date::VARCHAR, exp::VARCHAR, strike, ot, close FROM opt WHERE sym=? AND exp IN (?,?) AND date BETWEEN ? AND ? AND close>0", [sym, e1, e2, d0, e1]).fetchall():
        chains.setdefault((d, ex), {}).setdefault(k, {})[ot] = c
    meta = []
    for d in dates:
        sp, est = spots.get(d), False
        if not sp:
            sp, est = _est_spot(chains.get((d, e1), {})), True
        meta.append({"spot": sp, "est": est and sp is not None, "vix": vix.get(d),
                     "e": _atm(chains.get((d, e1), {}), sp, gap), "l": _atm(chains.get((d, e2), {}), sp, gap)})
    from datetime import date as _D
    dd = lambda a, b: (_D.fromisoformat(b) - _D.fromisoformat(a)).days + 1   # Excel: expiry - today + 1
    Te = {d: dd(d, e1) / 365 for d in dates}; Tl = {d: dd(d, e2) / 365 for d in dates}
    rd = lambda x, n: None if x is None else round(x, n)
    out = {}
    for ot in ("CE", "PE"):
        tbl = []
        for k in strikes:
            line = []
            for i, d in enumerate(dates):
                v = P.get((ot, k, d), {})
                e, l = v.get("e"), v.get("l")
                ge = _gkx(meta[i]["e"].get("sf"), k, Te[d], rate, e, ot)
                gl = _gkx(meta[i]["l"].get("sf"), k, Tl[d], rate, l, ot)
                ie, il = (ge[0] * 100 if ge else None), (gl[0] * 100 if gl else None)
                x, y = (ge[1] if ge else {}), (gl[1] if gl else {})
                dr = x["delta"] / y["delta"] if x and y and y["delta"] else None
                line.append([e, l, round(l / e, 2) if e and l else None, round(l - e, 2) if e and l else None,
                             rd(ie, 2), rd(il, 2), rd(x.get("delta"), 4), rd(y.get("delta"), 4), rd(dr, 3),
                             rd(x.get("vega"), 3), rd(y.get("vega"), 3), rd(x.get("theta"), 3), rd(y.get("theta"), 3),
                             rd(x.get("gamma"), 6), rd(y.get("gamma"), 6), rd(ie - il, 2) if ie is not None and il is not None else None])
            tbl.append({"strike": k, "cells": line})
        out[ot] = tbl
    return {"spot": spot, "dates": dates, "strikes": strikes, "CE": out["CE"], "PE": out["PE"], "meta": meta}

def api_info(q, con):
    sym, d0, gap = q["sym"], q["date"], float(q["gap"])
    chs = [_chain(con, sym, d0, q["e1"]), _chain(con, sym, d0, q["e2"])]
    spot = _spots(con, sym, d0, d0).get(d0) or _est_spot(chs[0])
    v = con.execute("SELECT close FROM idx WHERE date=? AND lower(name)=lower(?)", [d0, VIXNAME]).fetchone()
    return {"spot": spot, "vix": v[0] if v else None,
            "exp": [{"exp": q[k], **_atm(ch, spot, gap)} for k, ch in zip(("e1", "e2"), chs)]}

# ---------------------------------------------------------------- STRATEGY ANALYZER
def _pay(S, legs):
    return sum(l["qty"] * ((max(S - l["strike"], 0) if l["ot"] == "CE" else max(l["strike"] - S, 0)) - l["entry"]) for l in legs)

def api_strategy(q, con):
    """Koi pan multi-leg strategy: legs=[{ot,strike,exp,qty(+buy/-sell)}]. Strike entry date na nazdiki listed strike par snap thay."""
    sym, d0 = q["sym"], q["date"]
    legs = json.loads(q["legs"]); lots = max(1, int(q.get("lots") or 1))
    if not legs:
        raise ValueError("Legs nathi.")
    exps = sorted({l["exp"] for l in legs}); ph = ",".join("?" * len(exps))
    av = {}
    for ex, ot, k, c in con.execute(f"SELECT exp::VARCHAR, ot, strike, close FROM opt WHERE sym=? AND date=? AND close>0 AND exp IN ({ph})", [sym, d0] + exps).fetchall():
        av.setdefault((ex, ot), {})[k] = c
    L = []
    for l in legs:
        m = av.get((l["exp"], l["ot"]))
        if not m:
            raise ValueError(f"{l['exp']} {l['ot']}: entry date par data nathi.")
        k = min(m, key=lambda x: abs(x - float(l["strike"])))
        L.append(dict(ot=l["ot"], exp=l["exp"], strike=k, qty=int(l["qty"]), entry=m[k]))
    end = exps[0]                      # P&L pehla expiry sudhi (diagonal ma near expiry)
    P = {}
    for d, ex, ot, k, c in con.execute(f"SELECT date::VARCHAR, exp::VARCHAR, ot, strike, close FROM opt WHERE sym=? AND date BETWEEN ? AND ? AND exp IN ({ph}) AND close>0", [sym, d0, end] + exps).fetchall():
        P[(d, ex, ot, k)] = c
    dates, pnl = [], []
    for d in sorted({k[0] for k in P}):
        tot = 0
        for l in L:
            p = P.get((d, l["exp"], l["ot"], l["strike"]))
            if p is None:
                if d == l["exp"]: p = 0.0      # expiry day par OTM = 0
                else: tot = None; break
            tot += l["qty"] * (p - l["entry"])
        if tot is not None:
            dates.append(d); pnl.append(round(tot, 2))
    sp = _spots(con, sym, d0, end)
    lot = con.execute("SELECT max(lot) FROM opt WHERE sym=? AND date=?", [sym, d0]).fetchone()[0] if "lot" in {x[0] for x in con.execute("DESCRIBE opt").fetchall()} else None
    pay = None
    if len(exps) == 1:                 # single expiry -> exact expiry payoff (piecewise linear)
        ks = sorted({l["strike"] for l in L}); sc = sum(l["qty"] for l in L if l["ot"] == "CE")
        pts = [0.0] + ks; v = [_pay(x, L) for x in pts]; be = []
        for i in range(len(pts) - 1):
            fa, fb = v[i], v[i + 1]
            if fa * fb < 0: be.append(pts[i] + (pts[i + 1] - pts[i]) * fa / (fa - fb))
            elif fb == 0: be.append(pts[i + 1])
        if sc and v[-1] * sc < 0: be.append(pts[-1] - v[-1] / sc)
        spot0 = sp.get(d0)
        lo, hi = min(ks + [spot0 or ks[0]]) * 0.95, max(ks + [spot0 or ks[-1]]) * 1.05
        cx = sorted({lo, hi} | {k for k in ks if lo <= k <= hi})
        pay = {"be": sorted({round(b, 2) for b in be}), "maxp": "Unlimited" if sc > 0 else round(max(v), 2),
               "maxl": "Unlimited" if sc < 0 else round(min(v), 2), "curve": [[x, round(_pay(x, L), 2)] for x in cx]}
    return {"legs": L, "lots": lots, "lot": lot, "net": round(sum(l["qty"] * l["entry"] for l in L), 2), "spot": sp.get(d0),
            "dates": dates, "pnl": pnl, "spots": [sp.get(d) for d in dates], "pay": pay}

# ---------------------------------------------------------------- DIAGONAL SCANNER (IV + Greeks, Black-76)
import math
def _N(x): return 0.5 * (1 + math.erf(x / math.sqrt(2)))

def _gk(F, K, T, px, ot, r):
    """Excel jevu j: BS, S=F (synthetic future), IV bisection (0.0001), delta/vega(per 1 vol pt)/theta(per din). r=0 par Excel sathe barabar match."""
    if T <= 0 or not px or px <= 0: return None
    z = _gkx(F, K, T, r, px, ot)
    if not z: return None
    v, g = z
    return dict(iv=v * 100, delta=g["delta"], vega=g["vega"], theta=g["theta"])

def _prevd(d):
    ds = [x for x in all_dates() if x < d]
    return max(ds) if ds else None

def api_diag(q, con):
    """Darek block = alag (type, gap, from-to strike, step). Near K sell + far K+gap (CE)/K-gap (PE) buy. Prev close, IV, Greeks sathe."""
    from datetime import date
    sym, d0, e1, e2 = q["sym"], q["date"], q["e1"], q["e2"]
    r = float(q.get("r") or 0) / 100; tadd = 0 if str(q.get("tadd", "1")) == "0" else 1; n = max(1, min(60, int(q.get("n") or 10)))
    spec = json.loads(q["blocks"]) if q.get("blocks") else [{"ot": "CE", "gap": 200}]
    if not spec: raise ValueError("Ek block to rakho.")
    if e2 <= e1: raise ValueError("Far expiry, near expiry thi pachi hovi joiye.")
    ch = {e: _chain(con, sym, d0, e) for e in (e1, e2)}
    if not ch[e1] or not ch[e2]: raise ValueError("Aa date par ek expiry no data nathi.")
    T = {e: ((date.fromisoformat(e) - date.fromisoformat(d0)).days + tadd) / 365 for e in (e1, e2)}
    spot = _spots(con, sym, d0, d0).get(d0) or _est_spot(ch[e1])
    if not spot: raise ValueError("Spot nathi malto.")
    F = {}
    for e in (e1, e2):
        ks = [k for k, x in ch[e].items() if "CE" in x and "PE" in x]
        if not ks: raise ValueError(f"{e}: CE+PE banne no data nathi.")
        k = min(ks, key=lambda k: abs(k - spot)); F[e] = k + (ch[e][k]["CE"] - ch[e][k]["PE"]) / math.exp(-r * T[e])
    pd = con.execute("SELECT max(date)::VARCHAR FROM opt WHERE date<?", [d0]).fetchone()[0]
    pch = {e: _chain(con, sym, pd, e) for e in (e1, e2)} if pd else {}
    gp = float(q.get("gap") or 50); info = []
    for e in (e1, e2):
        a = _atm(ch[e], spot, gp)
        info.append({"exp": e, **({"sf": a["sf"], "fk": a["fk"], "st": a["straddle"], "pct": round(a["straddle"] / spot * 100, 2)} if a else {})})
    gc = {}
    def gk(e, k, ot):
        if (e, k, ot) not in gc:
            p = ch[e].get(k, {}).get(ot); gc[(e, k, ot)] = (p, _gk(F[e], k, T[e], p, ot, r) if p else None)
        return gc[(e, k, ot)]
    gv = lambda z, key, dd=2: round(z[key], dd) if z else None
    blocks, atm0 = [], None
    for b in spec:
        ot, g = b.get("ot") or "CE", float(b.get("gap") or 0)
        ks = sorted(k for k, x in ch[e1].items() if ot in x)
        if not ks: continue
        i0 = min(range(len(ks)), key=lambda i: abs(ks[i] - F[e1])); atm0 = atm0 or ks[i0]
        fr, to, st = b.get("from"), b.get("to"), float(b.get("step") or 0)
        if fr not in (None, "") and to not in (None, ""):
            lo, hi = sorted((float(fr), float(to)))
            sel = [k for k in ks if lo <= k <= hi and (not st or min((k - lo) % st, st - (k - lo) % st) < 1e-6)]
        else:
            sel = ks[max(0, i0 - n): i0 + n + 1]
        rows = []
        for k in sel:
            fk = k + g if ot == "CE" else k - g
            p1, a = gk(e1, k, ot); p2, bb = gk(e2, fk, ot)
            if not p1 or not p2: continue
            net = round(p2 - p1, 2); pv = None
            x1, x2 = pch.get(e1, {}).get(k, {}).get(ot), pch.get(e2, {}).get(fk, {}).get(ot)
            if x1 and x2: pv = round(x2 - x1, 2)
            rows.append([k, fk, p1, p2, net, pv, None if pv is None else round(net - pv, 2), round(p2 / p1, 2), gv(a, "iv"), gv(bb, "iv"),
                         gv(a, "delta", 3), gv(bb, "delta", 3), gv(a, "theta"), gv(bb, "theta"), gv(a, "vega"), gv(bb, "vega"), k == ks[i0]])
        blocks.append({"ot": ot, "gap": g, "rows": rows})
    return {"spot": spot, "F": [round(F[e1], 2), round(F[e2], 2)], "dte": [round(T[e1] * 365) - tadd, round(T[e2] * 365) - tadd], "atm": atm0, "prev": pd, "info": info, "blocks": blocks}

def _mround(x, m):
    return math.floor(x / m + 0.5) * m          # Excel MROUND

def api_parity(q, con):
    """Sensex (BSE) vs Nifty (NSE) parity. Darek block = (type, Sensex gap/step, strikes, Nifty strike set auto ya manual).
    Auto: Nifty strike = MROUND(Sensex strike / ratio, 50) (+shift). Spread = Sensex px - K * Nifty px."""
    from datetime import date as _D
    ss, ns = q.get("ssym") or "SENSEX", q.get("nsym") or "NIFTY"
    d0, es, en = q["date"], q["es"], q["en"]
    ngap, sgap = float(q.get("ngap") or 50), float(q.get("sgap") or 100)
    K0, lotS, lotN = float(q.get("k") or 3.25), float(q.get("lots") or 20), float(q.get("lotn") or 65)
    r = float(q.get("rate") or 0) / 100
    spec = json.loads(q["blocks"]) if q.get("blocks") else [{"ot": "CE", "step": 500, "n": 8}, {"ot": "PE", "step": 500, "n": 8}]
    if not 1 <= len(spec) <= 16: raise ValueError("Blocks/ladder 1 thi 16 vachche rakho.")
    if ngap <= 0 or sgap <= 0 or K0 <= 0: raise ValueError("Gap ane K 0 thi mota rakho.")
    chS, chN = _chain(con, ss, d0, es), _chain(con, ns, d0, en)
    if not chS: raise ValueError(f"{ss} {es} expiry no {d0} no data nathi. BSE data load karyo? (python optlab.py update --bse)")
    if not chN: raise ValueError(f"{ns} {en} expiry no {d0} no data nathi.")
    spS = _spots(con, ss, d0, d0).get(d0) or _est_spot(chS)
    spN = _spots(con, ns, d0, d0).get(d0) or _est_spot(chN)
    aS, aN = _atm(chS, spS, sgap), _atm(chN, spN, ngap)
    if not aS or not aN: raise ValueError("Synthetic future nathi malto (CE/PE banne no close joiye).")
    fS, fN = aS["sf"], aN["sf"]
    r_syn = fS / fN; r_spot = (spS / spN) if (spS and spN) else None
    ratio = r_spot if (q.get("src") == "spot" and r_spot) else r_syn
    first = lambda x: x not in (None, "", "auto")
    plans = []
    for i, bl in enumerate(spec, 1):
        ot = bl.get("ot", "CE"); K = float(bl.get("k") or K0); shift = float(bl.get("shift") or 0)
        rt, basep = ratio, None
        if first(bl.get("lad")):                        # Excel ladder: base pair +- n steps
            rt = r_spot or ratio                        # ladder ma hamesha spot ratio (Sensex spot / Nifty spot)
            nl = max(0, min(20, int(float(bl["lad"])))); sst = float(bl.get("lsstep") or 500); nst = float(bl.get("lnstep") or 150)
            if sst <= 0 or nst <= 0: raise ValueError(f"Block {i}: Sensex step ane Nifty step 0 thi motu rakho.")
            if first(bl.get("lbase")):
                x = re.split(r"[/:]", str(bl["lbase"]).strip())
                if len(x) != 2: raise ValueError(f"Block {i}: base '{bl['lbase']}' galat (74100/22900 jem lakho).")
                bS, bN = float(x[0]), float(x[1])
            else:
                if not spS: raise ValueError("Sensex spot nathi malto.")
                bS = _mround(spS, sgap); bN = _mround(bS / rt, ngap)
            bN += shift; basep = (bS, bN)
            prs = [(bS + j * sst, bN + j * nst) for j in range(-nl, nl + 1)]
            lab = f"ladder base {bS:g}/{bN:g}, Sensex step {sst:g}, Nifty step {nst:g}, +-{nl} rows"
            plans.append({"ot": ot, "k": K, "pairs": prs, "label": lab + (f", Nifty shift {shift:+g}" if shift else "") + f", K {K:g}", "base": basep, "ratio": rt})
            continue
        if first(bl.get("pairs")):                      # manual pairs: "74500/23050, 74000/22900"
            prs = []
            for part in re.split(r"[,;\s]+", str(bl["pairs"]).strip()):
                if not part: continue
                x = re.split(r"[/:]", part)
                if len(x) != 2: raise ValueError(f"Block {i}: pair '{part}' galat (Sensex/Nifty lakho, jem ke 74500/23050).")
                prs.append((float(x[0]), float(x[1])))
            lab = "manual pairs"
        else:
            step = float(bl.get("step") or 500); n = max(1, min(40, int(float(bl.get("n") or 8))))
            if step <= 0: raise ValueError(f"Block {i}: step 0 thi motu rakho.")
            st = float(bl["from"]) if first(bl.get("from")) else _mround(fS, step) - (n // 2) * step
            sks = [st + j * step for j in range(n)]
            if first(bl.get("nfrom")):
                nst = float(bl["nstep"]) if first(bl.get("nstep")) else None
                nks = [float(bl["nfrom"]) + (j * nst if nst else _mround((sk - sks[0]) / ratio, ngap)) for j, sk in enumerate(sks)]
                lab = f"gap {step:g}, Nifty from {float(bl['nfrom']):g}" + (f" step {nst:g}" if nst else " (auto step)")
            elif first(bl.get("nstep")):
                nst = float(bl["nstep"]); nks = [_mround(sks[0] / ratio, ngap) + j * nst for j in range(n)]
                lab = f"gap {step:g}, Nifty step {nst:g}"
            else:
                nks = [_mround(sk / ratio, ngap) for sk in sks]; lab = f"gap {step:g}"
            prs = [(sk, nk + shift) for sk, nk in zip(sks, nks)]
        plans.append({"ot": ot, "k": K, "pairs": prs, "label": lab + (f", Nifty shift {shift:+g}" if shift else "") + f", K {K:g}", "base": None, "ratio": ratio})
    end = min(es, en); pd_ = _prevd(d0)
    allS = sorted({p[0] for pl in plans for p in pl["pairs"]}); allN = sorted({p[1] for pl in plans for p in pl["pairs"]})
    P = {}
    for sym, ex, ks in ((ss, es, allS), (ns, en, allN)):
        for d, k, ot, c, v in con.execute(f"SELECT date::VARCHAR, strike, ot, close, vol FROM opt WHERE sym=? AND exp=? AND date BETWEEN ? AND ? AND strike IN ({','.join('?'*len(ks))}) AND close>0",
                                          [sym, ex, pd_ or d0, end] + ks).fetchall():
            P[(sym, d, k, ot)] = (c, v)
    HASLOT = "lot" in {x[0] for x in con.execute("DESCRIBE opt").fetchall()}
    lot = lambda sym: ((con.execute("SELECT max(lot) FROM opt WHERE sym=? AND date=?", [sym, d0]).fetchone() or [None])[0]) if HASLOT else None
    Ts = ((_D.fromisoformat(es) - _D.fromisoformat(d0)).days + 1) / 365
    Tn = ((_D.fromisoformat(en) - _D.fromisoformat(d0)).days + 1) / 365
    rd = lambda x, k: None if x is None else round(x, k)
    dates = sorted({d for (sy, d, k, o) in P if d >= d0})
    blocks = []
    for pl in plans:
        ot, K, snap, series = pl["ot"], pl["k"], [], []
        for sk, nk in pl["pairs"]:
            series.append({"sk": sk, "nk": nk, "base": (sk, nk) == pl.get("base"), "cells": [[(P.get((ss, d, sk, ot)) or (None,))[0], (P.get((ns, d, nk, ot)) or (None,))[0]] for d in dates]})
            a, b = P.get((ss, d0, sk, ot)), P.get((ns, d0, nk, ot))
            if not a or not b: continue
            ps, pn = a[0], b[0]
            pa, pb = (P.get((ss, pd_, sk, ot)), P.get((ns, pd_, nk, ot))) if pd_ else (None, None)
            spread = ps - K * pn; pspread = (pa[0] - K * pb[0]) if pa and pb else None
            gs, gn = _gkx(fS, sk, Ts, r, ps, ot), _gkx(fN, nk, Tn, r, pn, ot)
            dr = gs[1]["delta"] / gn[1]["delta"] if gs and gn and gn[1]["delta"] else None
            rt = pl["ratio"]
            snap.append({"sk": sk, "nk": nk, "base": (sk, nk) == pl.get("base"), "nex": rd(sk / rt, 2), "mis": rd(sk / rt - nk, 2), "ps": ps, "pn": pn, "knn": rd(K * pn, 2),
                "spread": rd(spread, 2), "pspread": rd(pspread, 2), "chg": rd(spread - pspread, 2) if pspread is not None else None,
                "dr": rd(dr, 3), "ad": rd(dr * rt, 3) if dr is not None else None,
                "brp": rd((ps + pn) * 2 * 0.00128, 2), "brr": rd((ps * lotS + pn * lotN) * 2 * 0.00128, 0),
                "ivs": rd(gs[0] * 100, 2) if gs else None, "ivn": rd(gn[0] * 100, 2) if gn else None,
                "ds": rd(gs[1]["delta"], 4) if gs else None, "dn": rd(gn[1]["delta"], 4) if gn else None,
                "ths": rd(gs[1]["theta"], 3) if gs else None, "thn": rd(gn[1]["theta"], 3) if gn else None,
                "vs": rd(a[1] / lotS, 0), "vn": rd(b[1] / lotN, 0)})
        blocks.append({"ot": ot, "k": K, "label": pl["label"], "snap": snap, "series": series})
    v = con.execute("SELECT close FROM idx WHERE date=? AND lower(name)=lower(?)", [d0, VIXNAME]).fetchone()
    info = {"date": d0, "es": es, "en": en, "gapdays": (_D.fromisoformat(es) - _D.fromisoformat(en)).days,
            "dayS": round(Ts * 365), "dayN": round(Tn * 365), "spS": spS, "spN": spN, "fS": fS, "fN": fN,
            "r_syn": r_syn, "r_spot": r_spot, "ratio": ratio, "src": "spot" if ratio == r_spot else "syn", "atm": _mround(fS, 500),
            "k": K0, "lots": lotS, "lotn": lotN, "lots_data": lot(ss), "lotn_data": lot(ns), "vix": v[0] if v else None, "prev": pd_}
    return {"info": info, "dates": dates, "blocks": blocks}

# ---------------------------------------------------------------- CALENDAR BACKTEST (EOD, index options)
def _pre(d, n=400):
    from datetime import date as D, timedelta as T
    return (D.fromisoformat(d) - T(days=n)).isoformat()

_ORD = {}
def _o(s):
    v = _ORD.get(s)
    if v is None:
        from datetime import date as D
        v = _ORD[s] = D.fromisoformat(s).toordinal()
    return v

def _cbt_cfg(q):
    f = lambda k, d: float(q[k]) if q.get(k) not in (None, "") else d
    off = f("off", 1.0) / 100
    legs = {"ce": [("CE", 0.0)], "pe": [("PE", 0.0)], "both": [("CE", 0.0), ("PE", 0.0)],
            "dbl": [("CE", off), ("PE", -off)]}.get(q.get("mode", "ce"))
    if not legs: raise ValueError("Mode galat.")
    c = dict(sym=q.get("sym") or "NIFTY", mode=q.get("mode", "ce"), legs=legs, frm=q["from"], to=q["to"], split=q.get("split") or "2023-01-01",
             nl=int(f("nl", 7)), nh=int(f("nh", 15)), fl=int(f("fl", 30)), fh=int(f("fh", 45)), ft=f("ft", 37),
             ratio=f("ratio", 1.0), pct=f("pct", 0.8), minvol=f("minvol", 100), minoi=f("minoi", 0), vixmax=f("vixmax", 0),
             tp=f("tp", 30) / 100, sl=f("sl", 40) / 100, xd=int(f("xd", 2)), mv=f("mv", 0), rate=f("rate", 7) / 100,
             cost=f("cost", 1), lot=f("lot", 1), tune=int(f("tune", 0)), mint=int(f("mint", 20)))
    if c["nl"] > c["nh"] or c["fl"] > c["fh"]: raise ValueError("DTE range ma 'from' 'to' thi nano rakho.")
    if c["nh"] >= c["fh"]: raise ValueError("Near DTE far DTE thi nano hovo joiye.")
    if c["tp"] <= 0 or c["sl"] <= 0: raise ValueError("TP ane SL 0 thi motu rakho.")
    return c

def _cbt_load(con, c):
    a, b, sym = _pre(c["frm"]), c["to"], c["sym"]
    sp = _spots(con, sym, a, b)
    alld = {r[0] for r in con.execute("SELECT DISTINCT date::VARCHAR FROM opt WHERE sym=? AND date BETWEEN ? AND ?", [sym, a, b]).fetchall()}
    if not alld: raise ValueError(f"{sym} no data {a} thi {b} vachche nathi.")
    miss, est = alld - set(sp), 0
    if miss:                                         # spot nathi: nazdik expiry ni put-call parity thi andaaz
        for d, v in con.execute("""WITH ne AS (SELECT date, min(exp) e FROM opt WHERE sym=? AND date BETWEEN ? AND ? AND exp>date GROUP BY date),
            pc AS (SELECT o.date, o.strike, max(CASE WHEN o.ot='CE' THEN o.close END) ce, max(CASE WHEN o.ot='PE' THEN o.close END) pe
                   FROM opt o JOIN ne ON o.date=ne.date AND o.exp=ne.e WHERE o.sym=? AND o.close>0 GROUP BY 1,2)
            SELECT date::VARCHAR, arg_min(strike+ce-pe, abs(ce-pe)) FROM pc WHERE ce IS NOT NULL AND pe IS NOT NULL GROUP BY 1""", [sym, a, b, sym]).fetchall():
            if d in miss and v: sp[d] = v; est += 1
    con.execute("CREATE OR REPLACE TEMP TABLE sp(date DATE, spot DOUBLE)")
    con.executemany("INSERT INTO sp VALUES (CAST(? AS DATE), ?)", [(d, v) for d, v in sp.items() if d in alld])
    ots = sorted({o for o, _ in c["legs"]})
    cur = con.execute(f"""SELECT o.date::VARCHAR, o.exp::VARCHAR, o.strike, o.ot, o.close, o.vol, o.oi FROM opt o JOIN sp ON o.date=sp.date
        WHERE o.sym=? AND o.ot IN ({','.join('?' * len(ots))}) AND o.close>0 AND o.exp>o.date AND date_diff('day', o.date, o.exp) <= ?
        AND abs(o.strike/sp.spot-1) <= 0.06""", [sym] + ots + [c["fh"] + 7])
    P, ex, ks = {}, {}, {}
    while True:
        rows = cur.fetchmany(50000)
        if not rows: break
        for d, e, k, ot, cl, vo, oi in rows:
            P[(d, e, k, ot)] = (cl, vo or 0, oi or 0); ex.setdefault(d, set()).add(e); ks.setdefault((d, e, ot), set()).add(k)
    vix = dict(con.execute("SELECT date::VARCHAR, close FROM idx WHERE lower(name)=lower(?) AND date BETWEEN ? AND ?", [VIXNAME, a, b]).fetchall())
    return sp, P, ex, ks, vix, est, len(miss)

def _cbt_days(c, sp, P, ex, ks, vix):
    days, hist = [], []
    for d in sorted(ex):
        spot = sp[d]; od = _o(d); r = {"d": d, "spot": spot, "legs": None, "vix": vix.get(d), "dtes": sorted(_o(e) - od for e in ex[d])}
        dte = {e: _o(e) - od for e in ex[d]}
        nr = [e for e in dte if c["nl"] <= dte[e] <= c["nh"]]
        if nr:
            near = min(nr, key=lambda e: dte[e])
            fr = [e for e in dte if c["fl"] <= dte[e] <= c["fh"] and dte[e] > dte[near]]
            if fr:
                far = min(fr, key=lambda e: abs(dte[e] - c["ft"])); legs = []
                for ot, off in c["legs"]:
                    com = ks.get((d, near, ot), set()) & ks.get((d, far, ot), set())
                    if not com: legs = None; break
                    k = min(com, key=lambda x: abs(x - spot * (1 + off)))
                    n_, f_ = P[(d, near, k, ot)], P[(d, far, k, ot)]
                    legs.append(dict(ot=ot, k=k, nc=n_[0], fc=f_[0], nv=n_[1], fv=f_[1], no=n_[2], fo=f_[2],
                                     ivn=_iv(spot, k, dte[near] / 365, c["rate"], n_[0], ot), ivf=_iv(spot, k, dte[far] / 365, c["rate"], f_[0], ot)))
                if legs:
                    vn = [L["ivn"] for L in legs if L["ivn"]]; vf = [L["ivf"] for L in legs if L["ivf"]]
                    ivn = sum(vn) / len(vn) if vn else None; ivf = sum(vf) / len(vf) if vf else None
                    pct = None
                    if ivf:
                        w = hist[-252:]
                        if len(w) >= 60: pct = sum(1 for x in w if x < ivf) / len(w)
                        hist.append(ivf)
                    r.update(near=near, far=far, dn=dte[near], df=dte[far], legs=legs, debit=sum(L["fc"] - L["nc"] for L in legs),
                             ivn=ivn, ivf=ivf, ratio=(ivn / ivf) if ivn and ivf else None, pct=pct,
                             mvol=min(min(L["nv"], L["fv"]) for L in legs), moi=min(min(L["no"], L["fo"]) for L in legs))
        days.append(r)
    return days

def _cbt_fails(r, c, ratio, pct=None):
    pct = c["pct"] if pct is None else pct
    if not r.get("legs"): return ["Near/Far expiry ya strike range ma nathi malta"]
    x = []
    if r["debit"] <= 0: x.append(f"Debit {r['debit']:.2f} (far near thi mehngu hovu joiye)")
    if r["ratio"] is None: x.append("IV nathi nikdi")
    elif r["ratio"] < ratio: x.append(f"IV ratio {r['ratio']:.3f} < {ratio:g}")
    if r["pct"] is not None and r["pct"] > pct: x.append(f"Far IV percentile {r['pct']:.2f} > {pct:g} (far mehngi)")
    if r["mvol"] < c["minvol"]: x.append(f"Volume {r['mvol']:g} < {c['minvol']:g}")
    if c["minoi"] and r["moi"] < c["minoi"]: x.append(f"OI {r['moi']:g} < {c['minoi']:g}")
    if c["vixmax"] and r["vix"] and r["vix"] > c["vixmax"]: x.append(f"VIX {r['vix']:.1f} > {c['vixmax']:g}")
    return x

def _cbt_run(days, P, c, lo, end, ratio, tp, sl, hi=None):
    """Entry fakat lo <= date < hi; end=data ni chhelli date (exclusive). Ek time ek j position."""
    trades, tr, last_d = [], None, None
    def close(d, val, why):
        net = val - tr["debit"] - 4 * c["cost"] * len(tr["legs"])
        trades.append(dict(entry=tr["d"], exit=d, near=tr["near"], far=tr["far"], k="/".join(f"{L['k']:g}" for L in tr["legs"]), spot=round(tr["spot"], 1),
                           debit=round(tr["debit"], 2), exitv=round(val, 2), pnl=round(net, 2), pct=round(net / tr["debit"] * 100, 1),
                           days=_o(d) - _o(tr["d"]), reason=why, ratio=round(tr["ratio"], 3) if tr["ratio"] else None,
                           ivpct=round(tr["pct"], 2) if tr["pct"] is not None else None))
    for r in days:
        d = r["d"]
        if d >= end: break
        last_d = d
        if tr:
            left = _o(tr["near"]) - _o(d); vals = []
            for L in tr["legs"]:
                n_, f_ = P.get((d, tr["near"], L["k"], L["ot"])), P.get((d, tr["far"], L["k"], L["ot"]))
                if not n_ or not f_: vals = None; break
                vals.append(f_[0] - n_[0])
            if vals is None:
                if left > c["xd"]: continue
                val = tr["last"]
            else:
                val = tr["last"] = sum(vals)
            pnl = val - tr["debit"]
            why = ("TP" if pnl >= tp * tr["debit"] else "SL" if pnl <= -sl * tr["debit"] else
                   "MOVE" if c["mv"] and abs(r["spot"] / tr["spot"] - 1) * 100 >= c["mv"] else "TIME" if left <= c["xd"] else None)
            if why: close(d, val, why); tr = None
            continue
        if d < lo or (hi and d >= hi): continue
        if r.get("legs") and not _cbt_fails(r, c, ratio):
            tr = dict(d=d, near=r["near"], far=r["far"], legs=r["legs"], debit=r["debit"], last=r["debit"], spot=r["spot"], ratio=r["ratio"], pct=r["pct"])
    if tr and last_d: close(last_d, tr["last"], "OPEN")
    return trades

def _cbt_met(t, lot):
    if not t: return dict(n=0, pf=None, win=0, tot=0, rs=0, dd=0, avg=0, aw=0, al=0, hold=0)
    eq = pk = dd = 0.0
    for x in t:
        eq += x["pnl"]; pk = max(pk, eq); dd = min(dd, eq - pk)
    w = [x["pnl"] for x in t if x["pnl"] > 0]; l = [x["pnl"] for x in t if x["pnl"] <= 0]; tot = sum(x["pnl"] for x in t)
    return dict(n=len(t), pf=round(sum(w) / -sum(l), 2) if l and sum(l) < 0 else None, win=round(len(w) / len(t) * 100, 1), tot=round(tot, 1),
                rs=round(tot * lot), dd=round(dd, 1), avg=round(tot / len(t), 2), aw=round(sum(w) / len(w), 2) if w else 0,
                al=round(sum(l) / len(l), 2) if l else 0, hold=round(sum(x["days"] for x in t) / len(t), 1))

def api_cbt(q, con):
    c = _cbt_cfg(q)
    sp, P, ex, ks, vix, est, nmiss = _cbt_load(con, c)
    days = _cbt_days(c, sp, P, ex, ks, vix)
    frm, split, END = c["frm"], c["split"], "9999-12-31"
    run = lambda lo, end, rm=c["ratio"], tp=c["tp"], sl=c["sl"], hi=None: _cbt_run(days, P, c, lo, end, rm, tp, sl, hi)
    all_t = run(frm, END); ins_t = run(frm, split); oos_t = run(split, END)
    M = lambda t: _cbt_met(t, c["lot"])
    fixed = {"all": M(all_t), "ins": M(ins_t), "oos": M(oos_t)}
    tuned = None
    if c["tune"]:
        best = None
        for rm in (1.0, 1.05, 1.1):
            for tp in (0.2, 0.3, 0.5):
                for sl in (0.3, 0.5):
                    m = M(run(frm, split, rm, tp, sl))
                    sc = 99 if (m["pf"] is None and m["n"]) else (m["pf"] or 0)
                    if m["n"] >= c["mint"] and (best is None or sc > best[0]): best = (sc, rm, tp, sl, m)
        if best:
            _, rm, tp, sl, m = best; to_ = run(split, END, rm, tp, sl)
            tuned = {"ratio": rm, "tp": tp, "sl": sl, "ins": m, "oos": M(to_), "oos_trades": len(to_)}
    yrs = {}
    for x in all_t: yrs.setdefault(x["exit"][:4], []).append(x)
    years = [dict(y=y, **M(t)) for y, t in sorted(yrs.items())]
    cum, eq = 0.0, []
    for x in all_t: cum += x["pnl"]; eq.append([x["exit"], round(cum, 1)])
    pv = lambda m: 99 if (m["pf"] is None and m["n"]) else (m["pf"] or 0)
    o, i = fixed["oos"], fixed["ins"]; posy = sum(1 for y in years if y["tot"] > 0)
    checks = [
        dict(ok=o["n"] >= 30, txt=f"Out-of-sample trades: {o['n']} (kam se kam 30 joiye. Ochha trade = result bharosapatra nahi)"),
        dict(ok=pv(o) >= 1.3, txt=f"Out-of-sample profit factor: {o['pf'] if o['pf'] is not None else 'koi loss nathi'} (1.3 thi vadhare joiye)"),
        dict(ok=o["tot"] > 0 and (o["dd"] == 0 or o["tot"] / -o["dd"] >= 2), txt=f"Total profit / max drawdown (OOS): {round(o['tot'] / -o['dd'], 1) if o['dd'] else '-'} (2 thi vadhare joiye)"),
        dict(ok=bool(years) and posy >= 0.6 * len(years), txt=f"Profit vala varsh: {posy} / {len(years)} (60%+ joiye. Ek j varsh no profit = luck/overfit)"),
        dict(ok=i["n"] >= 20 and pv(o) >= 0.6 * pv(i), txt=f"In-sample trades {i['n']} (20+ joiye), PF {i['pf']} vs out-of-sample PF {o['pf']} (OOS ghanu nichu hoy to overfit)")]
    npass = sum(1 for k in checks if k["ok"])
    verdict = ("Saru: badhi checks pass. Haju paper-trade 1-2 mahina karo, pachi nano size." if npass == 5 else
               "Madhyam: thodi checks fail. Parameters ek ek badli ne jovo, ane size nano rakho." if npass >= 3 else
               "Haju taiyar nathi: ghani checks fail. Real paisa na lagavo, filters/params sudharo.")
    last = next((r for r in reversed(days) if r["d"] <= c["to"]), None); today = None
    if last:
        fl_ = _cbt_fails(last, c, c["ratio"])
        if not last.get("legs"): fl_ = [f"Near DTE {c['nl']}-{c['nh']} ane Far DTE {c['fl']}-{c['fh']} vala expiry aaje nathi. Aaje na expiry DTE (divas): {', '.join(map(str, last['dtes']))}. Monthly expiry joiye to 'Monthly Study' tab vapro."]
        today = dict(d=last["d"], spot=round(last["spot"], 1), ok=not fl_, fails=fl_, vix=last["vix"])
        if last.get("legs"):
            today.update(near=last["near"], far=last["far"], dn=last["dn"], df=last["df"], debit=round(last["debit"], 2),
                         k="/".join(f"{L['ot']} {L['k']:g}" for L in last["legs"]), ivn=round(last["ivn"] * 100, 2) if last["ivn"] else None,
                         ivf=round(last["ivf"] * 100, 2) if last["ivf"] else None, ratio=round(last["ratio"], 3) if last["ratio"] else None,
                         pct=round(last["pct"], 2) if last["pct"] is not None else None)
    return dict(meta=dict(days=len(days), spot_est=est, spot_missing=nmiss, rows=len(P), lot=c["lot"], split=split), today=today, fixed=fixed, tuned=tuned,
                years=years, equity=eq, trades=all_t, checks=checks, npass=npass, verdict=verdict)

# ---------------------------------------------------------------- MONTHLY CALENDAR STUDY (fixed idea: Nth monthly sell, +G monthly buy, ATM/OTM, hold X days)
def _spot_map(con, sym, a, b):
    sp = _spots(con, sym, a, b)
    alld = {r[0] for r in con.execute("SELECT DISTINCT date::VARCHAR FROM opt WHERE sym=? AND date BETWEEN ? AND ?", [sym, a, b]).fetchall()}
    if not alld: raise ValueError(f"{sym} no data {a} thi {b} vachche nathi.")
    miss, est = alld - set(sp), 0
    if miss:
        for d, v in con.execute("""WITH ne AS (SELECT date, min(exp) e FROM opt WHERE sym=? AND date BETWEEN ? AND ? AND exp>date GROUP BY date),
            pc AS (SELECT o.date, o.strike, max(CASE WHEN o.ot='CE' THEN o.close END) ce, max(CASE WHEN o.ot='PE' THEN o.close END) pe
                   FROM opt o JOIN ne ON o.date=ne.date AND o.exp=ne.e WHERE o.sym=? AND o.close>0 GROUP BY 1,2)
            SELECT date::VARCHAR, arg_min(strike+ce-pe, abs(ce-pe)) FROM pc WHERE ce IS NOT NULL AND pe IS NOT NULL GROUP BY 1""", [sym, a, b, sym]).fetchall():
            if d in miss and v: sp[d] = v; est += 1
    return sp, alld, est, len(miss)

def _cst_cfg(q):
    f = lambda k, d: float(q[k]) if q.get(k) not in (None, "") else d
    lst = lambda k, d, t: [t(x) for x in re.split(r"[,\s;]+", str(q.get(k) or d).strip()) if x != ""]
    c = dict(sym=q.get("sym") or "NIFTY", mode=q.get("mode", "dbl"), frm=q["from"], to=q["to"], N=int(f("n", 2)), G=int(f("g", 1)),
             offs=lst("offs", "0,200,300,500", float), holds=lst("holds", "15,20", lambda x: int(float(x))), nl=f("nl", None), nh=f("nh", None),
             minvol=f("minvol", 1), cost=f("cost", 1), lot=f("lot", 1), rate=f("rate", 7) / 100)
    if c["mode"] not in ("ce", "pe", "dbl"): raise ValueError("Mode galat.")
    if c["N"] < 1 or c["G"] < 1: raise ValueError("Near month no. ane Far gap 1 ke vadhare rakho.")
    if not c["offs"] or len(c["offs"]) > 8 or not c["holds"] or len(c["holds"]) > 6 or min(c["holds"]) < 1: raise ValueError("Offsets (max 8) ane Hold days (max 6, 1+) barabar lakho.")
    return c

def api_cst(q, con):
    c = _cst_cfg(q); sym, a, b, N, G = c["sym"], c["frm"], c["to"], c["N"], c["G"]; need = N + G
    sp, alld, est, nmiss = _spot_map(con, sym, a, b)
    byd = {}
    for d, e in con.execute("SELECT DISTINCT date::VARCHAR, exp::VARCHAR FROM opt WHERE sym=? AND date BETWEEN ? AND ? AND exp>date", [sym, a, b]).fetchall():
        byd.setdefault(d, set()).add(e)
    mon = {}
    for d, es in byd.items():                        # monthly = darek mahina no chhello expiry
        best = {}
        for e in es:
            if e[:7] not in best or e > best[e[:7]]: best[e[:7]] = e
        mon[d] = sorted(best.values())
    sel = sorted({(d, e) for d, m in mon.items() if d in sp for e in m[:need + 1]})
    if not sel: raise ValueError("Monthly expiry data nathi malto.")
    con.execute("CREATE OR REPLACE TEMP TABLE sp(date DATE, spot DOUBLE)"); con.executemany("INSERT INTO sp VALUES (CAST(? AS DATE), ?)", [(d, v) for d, v in sp.items() if d in alld])
    con.execute("CREATE OR REPLACE TEMP TABLE se(date DATE, exp DATE)"); con.executemany("INSERT INTO se VALUES (CAST(? AS DATE), CAST(? AS DATE))", sel)
    P, ks = {}, {}
    band = max(abs(x) for x in c["offs"]) + 400          # fakat jaruri strikes (spot +- offset +- 400 pts), memory bachave
    cur = con.execute("""SELECT o.date::VARCHAR, o.exp::VARCHAR, o.strike, o.ot, o.close, o.vol FROM opt o JOIN sp ON o.date=sp.date
            JOIN se ON o.date=se.date AND o.exp=se.exp WHERE o.sym=? AND o.close>0 AND abs(o.strike-sp.spot) <= ?""", [sym, band])
    while True:
        chunk = cur.fetchmany(50000)
        if not chunk: break
        for d, e, k, ot, cl, vo in chunk:
            P[(d, e, k, ot)] = (cl, vo or 0); ks.setdefault((d, e, ot), set()).add(k)
    dts = sorted(d for d in alld if d in sp); od = [_o(d) for d in dts]
    if not dts: raise ValueError("Data nathi.")
    def entry(d, spec):
        m = mon.get(d)
        if not m or len(m) < need: return None
        near, far = m[N - 1], m[N - 1 + G]; legs = []
        for ot, off in spec:
            com = ks.get((d, near, ot), set()) & ks.get((d, far, ot), set())
            if not com: return None
            k = min(com, key=lambda x: abs(x - (sp[d] + off))); n_, f_ = P[(d, near, k, ot)], P[(d, far, k, ot)]
            if n_[1] < c["minvol"] or f_[1] < c["minvol"]: return None
            legs.append((ot, k, n_[0], f_[0]))
        debit = sum(x[3] - x[2] for x in legs)
        return (near, far, _o(near) - _o(d), _o(far) - _o(d), legs, debit) if debit > 0 else None
    lastd = next((d for d in reversed(dts) if len(mon.get(d, [])) >= need), dts[-1])
    sp0 = lambda off: {"ce": [("CE", off)], "pe": [("PE", -off)], "dbl": [("CE", off), ("PE", -off)]}[c["mode"]]
    e0 = entry(lastd, sp0(c["offs"][0]))
    nl, nh = c["nl"], c["nh"]
    if nl is None or nh is None:
        dn0 = (_o(mon[lastd][N - 1]) - _o(lastd)) if len(mon.get(lastd, [])) >= need else None
        nl, nh = (dn0 - 7, dn0 + 7) if dn0 else (0, 400)
    today = []
    for off in c["offs"]:
        e = entry(lastd, sp0(off)); t = dict(off=off, d=lastd, spot=round(sp[lastd], 1), monthly=mon.get(lastd, []))
        if e:
            near, far, dn, df, legs, debit = e; iv = lambda i, T, e_: [_iv(sp[lastd], x[1], T / 365, c["rate"], x[i], x[0]) for x in legs]
            vn, vf = [x for x in iv(2, dn, e) if x], [x for x in iv(3, df, e) if x]
            t.update(near=near, far=far, dn=dn, df=df, strikes=" / ".join(f"{x[0]} {x[1]:g}" for x in legs), debit=round(debit, 2), rs=round(debit * c["lot"]),
                     ivn=round(sum(vn) / len(vn) * 100, 2) if vn else None, ivf=round(sum(vf) / len(vf) * 100, 2) if vf else None)
        else: t["err"] = f"{N}-th ane {need}-th monthly expiry nathi malta / volume ochhu. Aaje na monthly: {', '.join(mon.get(lastd, [])) or '-'}"
        today.append(t)
    maxh = max(c["holds"]); grid = []
    for off in c["offs"]:
        spec = sp0(off); res = {h: [] for h in c["holds"]}; skip = {h: 0 for h in c["holds"]}
        for i, d in enumerate(dts):
            e = entry(d, spec)
            if not e: continue
            near, far, dn, df, legs, debit = e
            if not (nl <= dn <= nh): continue
            path = []
            for j in range(i + 1, len(dts)):
                dd = od[j] - od[i]
                if dd > maxh + 5: break
                v = 0.0
                for ot, k, _, _ in legs:
                    n_, f_ = P.get((dts[j], near, k, ot)), P.get((dts[j], far, k, ot))
                    if not n_ or not f_: v = None; break
                    v += f_[0] - n_[0]
                if v is not None: path.append((dd, j, v))
            for h in c["holds"]:
                ex_ = next((p for p in path if p[0] >= h), None)
                if not ex_: skip[h] += 1; continue
                pts = [p for p in path if p[0] <= ex_[0]]
                net = ex_[2] - debit - 4 * c["cost"] * len(legs)
                res[h].append([d, dts[ex_[1]], "/".join(f"{x[1]:g}" for x in legs), round(sp[d], 1), round(debit, 2), round(ex_[2], 2), round(net, 2),
                               round(net / debit * 100, 1), round((sp[dts[ex_[1]]] / sp[d] - 1) * 100, 2),
                               round(min(p[2] - debit for p in pts) / debit * 100, 1), round(max(p[2] - debit for p in pts) / debit * 100, 1), dn])
        for h in c["holds"]:
            t = res[h]; pn = [x[6] for x in t]; w = [x for x in pn if x > 0]; l = [x for x in pn if x <= 0]
            bk = {}
            for x in t:
                mv = x[8]; key = (0 if mv <= -3 else 1 if mv <= -2 else 2 if mv <= -1 else 3 if mv <= -0.5 else 4 if mv < 0.5 else 5 if mv < 1 else 6 if mv < 2 else 7 if mv < 3 else 8)
                bk.setdefault(key, []).append(x)
            names = ["<= -3%", "-3 thi -2%", "-2 thi -1%", "-1 thi -0.5%", "+-0.5%", "0.5 thi 1%", "1 thi 2%", "2 thi 3%", ">= 3%"]
            yr = {}
            for x in t: yr.setdefault(x[0][:4], []).append(x[6])
            grid.append(dict(off=off, hold=h, n=len(t), skipped=skip[h], win=round(len(w) / len(t) * 100, 1) if t else 0,
                             avg=round(sum(pn) / len(pn), 2) if pn else 0, avgpct=round(sum(x[7] for x in t) / len(t), 2) if t else 0,
                             pf=round(sum(w) / -sum(l), 2) if l and sum(l) < 0 else None, worst=round(min(pn), 1) if pn else 0, best=round(max(pn), 1) if pn else 0,
                             tot=round(sum(pn), 1), rs=round(sum(pn) / len(pn) * c["lot"]) if pn else 0, mae=round(sum(x[9] for x in t) / len(t), 1) if t else 0,
                             stop30=round(sum(1 for x in t if x[9] <= -30) / len(t) * 100, 1) if t else 0,
                             buckets=[dict(name=names[k], n=len(v), win=round(sum(1 for x in v if x[6] > 0) / len(v) * 100, 1), avgpct=round(sum(x[7] for x in v) / len(v), 1),
                                           avg=round(sum(x[6] for x in v) / len(v), 1)) for k, v in sorted(bk.items())],
                             years=[dict(y=y, n=len(v), win=round(sum(1 for x in v if x > 0) / len(v) * 100, 1), avg=round(sum(v) / len(v), 1), tot=round(sum(v), 1)) for y, v in sorted(yr.items())],
                             trades=t))
    return dict(meta=dict(days=len(dts), spot_est=est, spot_missing=nmiss, nl=nl, nh=nh, lot=c["lot"], N=N, G=G, mode=c["mode"], last=lastd), today=today, grid=grid)


# ---------------------------------------------------------------- EVENTS + NEWS (market context)
import csv, time, bisect
EVENT_CATS = ["RBI", "FED", "BUDGET", "ELECTION", "GEO", "US", "POLICY", "COVID", "OTHER"]
EVENING = {"FED", "US"}          # US raat na events: Indian market ne aavta trading divas ma asar kare
EVFILE = os.path.join("data", "events.csv")
_RBI = "2019-02-07 2019-04-04 2019-06-06 2019-08-07 2019-10-04 2019-12-05 2020-02-06 2020-08-06 2020-10-09 2020-12-04 2021-02-05 2021-04-07 2021-06-04 2021-08-06 2021-10-08 2021-12-08 2022-02-10 2022-04-08 2022-06-08 2022-08-05 2022-09-30 2022-12-07 2023-02-08 2023-04-06 2023-06-08 2023-08-10 2023-10-06 2023-12-08 2024-02-08 2024-04-05 2024-06-07 2024-08-08 2024-10-09 2024-12-06 2025-02-07 2025-04-09 2025-06-06 2025-08-06 2025-10-01 2025-12-05 2026-02-06 2026-04-08 2026-06-05 2026-08-05 2026-10-07 2026-12-04 2027-02-05".split()
_FED = "2019-01-30 2019-03-20 2019-05-01 2019-06-19 2019-07-31 2019-09-18 2019-10-30 2019-12-11 2020-01-29 2020-04-29 2020-06-10 2020-07-29 2020-09-16 2020-11-05 2020-12-16 2021-01-27 2021-03-17 2021-04-28 2021-06-16 2021-07-28 2021-09-22 2021-11-03 2021-12-15 2022-01-26 2022-05-04 2022-06-15 2022-07-27 2022-09-21 2022-11-02 2022-12-14 2023-02-01 2023-03-22 2023-05-03 2023-06-14 2023-07-26 2023-09-20 2023-11-01 2023-12-13 2024-01-31 2024-03-20 2024-05-01 2024-06-12 2024-07-31 2024-11-07 2024-12-18 2025-01-29 2025-03-19 2025-05-07 2025-06-18 2025-07-30 2025-09-17 2025-10-29 2025-12-10 2026-01-28 2026-03-18 2026-04-29 2026-06-17 2026-07-29 2026-09-16 2026-10-28 2026-12-09".split()
_OTHER = """2020-03-27|RBI|H|RBI emergency: repo rate cut 75 bp + loan moratorium (COVID)
2020-05-22|RBI|M|RBI off-cycle policy: repo rate cut 40 bp
2022-05-04|RBI|H|RBI off-cycle surprise: repo rate hike 40 bp
2025-02-07|RBI|M|RBI MPC: first rate cut under Governor Malhotra
2025-06-06|RBI|H|RBI MPC: 50 bp repo cut + CRR cut (surprise)
2020-03-03|FED|H|US Fed emergency rate cut (50 bp)
2020-03-15|FED|H|US Fed emergency cut to near zero + QE (Sunday)
2022-03-16|FED|M|US Fed first rate hike since 2018
2024-09-18|FED|H|US Fed first rate cut (50 bp)
2019-02-01|BUDGET|M|Interim Union Budget
2019-07-05|BUDGET|H|Union Budget: super-rich surcharge, FPI selling
2020-02-01|BUDGET|H|Union Budget (Saturday special session)
2021-02-01|BUDGET|H|Union Budget: strong rally, no new taxes
2022-02-01|BUDGET|M|Union Budget
2023-02-01|BUDGET|M|Union Budget
2024-02-01|BUDGET|L|Interim Union Budget
2024-07-23|BUDGET|H|Union Budget: STT and capital gains tax hike
2025-02-01|BUDGET|M|Union Budget (Saturday special session): income tax relief
2026-02-01|BUDGET|H|Union Budget (Sunday special session): F&O STT hike, Nifty about -2%
2019-05-20|ELECTION|H|Lok Sabha exit polls rally
2019-05-23|ELECTION|H|Lok Sabha results: NDA majority
2024-06-03|ELECTION|H|Lok Sabha exit polls: big rally
2024-06-04|ELECTION|H|Lok Sabha results: NDA short of expectations, Sensex -5.7% (worst day since 2020)
2024-06-05|ELECTION|M|Post-result rebound
2020-11-10|ELECTION|L|Bihar election results
2021-05-02|ELECTION|L|West Bengal / TN / Kerala / Assam results (Sunday)
2022-03-10|ELECTION|M|UP + 4 state results: market rally
2022-12-08|ELECTION|L|Gujarat + HP results
2023-12-03|ELECTION|M|3 state results (Sunday): rally on next day
2024-10-08|ELECTION|L|Haryana + J&K results
2024-11-23|ELECTION|M|Maharashtra + Jharkhand results (Saturday): rally on Monday
2025-02-08|ELECTION|L|Delhi election results (Saturday)
2025-11-14|ELECTION|M|Bihar election results
2026-05-04|ELECTION|M|Results: West Bengal, Tamil Nadu, Kerala, Assam, Puducherry
2020-11-03|US|M|US presidential election (Biden)
2024-11-05|US|M|US presidential election (Trump wins)
2022-09-13|US|H|US CPI hotter than expected: global sell-off
2023-03-10|US|M|SVB (Silicon Valley Bank) collapse
2025-04-02|US|H|US 'Liberation Day' reciprocal tariffs announced
2025-04-09|US|H|US pauses reciprocal tariffs for 90 days
2025-09-19|US|M|US H-1B fee hike announced: IT stocks hit
2019-02-26|GEO|M|Balakot air strike
2019-08-05|GEO|M|Article 370 abrogation
2019-09-16|GEO|M|Saudi Aramco attack: crude spike
2019-09-20|POLICY|H|Corporate tax cut: Nifty +5% (biggest rally in a decade)
2020-01-06|GEO|M|US-Iran tension (Soleimani killing)
2020-03-12|COVID|H|WHO declares COVID-19 a pandemic: Nifty crashes
2020-03-13|COVID|H|Lower/upper circuit halts, extreme volatility
2020-03-16|COVID|H|Global COVID sell-off continues
2020-03-23|COVID|H|Biggest fall: Sensex about -13%
2020-03-24|COVID|H|21-day nationwide lockdown announced
2020-04-20|US|M|WTI crude turns negative
2020-06-16|GEO|M|India-China Galwan clash
2021-11-26|GEO|H|Omicron variant scare: Nifty about -3%
2022-02-24|GEO|H|Russia invades Ukraine: Nifty about -4.8%
2022-06-13|GEO|H|Global rate-hike fears: sharp sell-off
2023-01-25|GEO|M|Hindenburg report on Adani group
2023-10-09|GEO|M|Hamas attack on Israel: first Indian trading day
2024-04-15|GEO|M|Iran attack on Israel: first Indian trading day
2024-08-05|GEO|H|Global sell-off (yen carry unwind): Nifty about -2.7%
2024-10-01|GEO|H|Iran missile attack on Israel: West Asia tension
2024-10-03|GEO|H|West Asia tension: Sensex about -1,770
2025-04-07|GEO|H|Global tariff crash: heavy sell-off
2025-04-23|GEO|M|Pahalgam terror attack (first trading day after)
2025-05-07|GEO|M|Operation Sindoor
2025-05-12|GEO|H|India-Pakistan ceasefire: Sensex about +2,900
2025-06-13|GEO|M|Israel strikes Iran
2025-06-23|GEO|M|US strikes Iran nuclear sites (Sunday): first trading day
2025-08-06|GEO|H|US announces extra 25% tariff on India
2025-08-27|GEO|H|US 50% tariff on Indian goods takes effect
2025-09-03|POLICY|M|GST Council approves rate cuts (GST 2.0)
2025-09-22|POLICY|M|GST rate cuts come into effect
2026-02-03|GEO|H|India-US trade deal rally: Sensex about +2,070
2026-03-09|GEO|H|Middle East war + oil shock (Brent above 115): Sensex about -2,400
2026-03-12|GEO|H|Sell-off continues: oil, war fears, FPI selling
2026-03-19|GEO|H|Sensex -2,497 / Nifty -776 (-3.26%): crude above 110, Fed caution
2026-03-23|GEO|M|Broad sell-off continues (Sensex about -1,900)
2026-03-30|GEO|H|Sensex about -1,650, Bank Nifty -3.8%: RBI forex limits, crude 116
2026-05-11|GEO|H|Steepest fall since March: crude above 100, rupee record low 95.31"""
def _builtin():
    out = []
    for d in _RBI: out.append(f"{d}|RBI|M|RBI MPC policy decision")
    for d in _FED: out.append(f"{d}|FED|M|US Fed (FOMC) rate decision, India par asar next trading day")
    out.append("2024-06-12|FED|M|US Fed (FOMC) rate decision, India par asar next trading day")
    out.append("2022-01-26|FED|M|US Fed (FOMC) rate decision, India par asar next trading day")
    res = {}
    for ln in out + _OTHER.splitlines():          # same date+category: _OTHER (specific title) overrides generic RBI/FED line
        p = ln.split("|", 3)
        if len(p) == 4: res[(p[0], p[1])] = {"date": p[0], "cat": p[1], "impact": p[2], "title": p[3], "src": "builtin"}
    return sorted(res.values(), key=lambda e: (e["date"], e["cat"]))
_BUILTIN = _builtin()

def _evs():
    out = [dict(e, id="b%d" % i) for i, e in enumerate(_BUILTIN)]
    try:
        with open(EVFILE, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                if r.get("date") and r.get("title") and r.get("id"):
                    out.append({"id": r["id"], "date": r["date"], "cat": r.get("cat") or "OTHER", "impact": r.get("impact") or "M", "title": r["title"], "src": "custom"})
    except FileNotFoundError:
        pass
    return sorted(out, key=lambda e: (e["date"], e["cat"]))

def _react(date, cat, tds, tdset):
    """Event ni asar kyare: trading divas par j hoy to e divas; weekend/holiday ke US raat no event hoy to aavto trading divas."""
    if date in tdset and cat not in EVENING:
        return date
    i = bisect.bisect_right(tds, date)
    if i >= len(tds): return None
    from datetime import date as _D
    if (_D.fromisoformat(tds[i]) - _D.fromisoformat(date)).days > 5: return None   # data ma te samay no trading divas nathi
    return tds[i]

def api_events(q, con):
    tds = all_dates(); ts = set(tds)
    ev = _evs()
    for e in ev:
        e["react"] = _react(e["date"], e["cat"], tds, ts) or e["date"]
    return {"events": ev, "cats": EVENT_CATS}

def api_context(q, con):
    from datetime import date as _D, timedelta as _T
    d = q["date"]
    syms = [x for x in (q.get("syms") or "NIFTY").split(",") if x][:4]
    pd_ = _prevd(d)
    moves = []
    for sy in syms:
        a = _spots(con, sy, d, d).get(d)
        b = _spots(con, sy, pd_, pd_).get(pd_) if pd_ else None
        moves.append({"sym": sy, "close": a, "prev": b, "chg": round((a / b - 1) * 100, 2) if a and b else None})
    vm = dict(con.execute("SELECT date::VARCHAR, close FROM idx WHERE lower(name)=lower(?) AND date IN (?,?)", [VIXNAME, d, pd_ or d]).fetchall())
    vix = None
    if vm.get(d) is not None:
        vix = {"v": vm[d], "prev": vm.get(pd_), "chg": round((vm[d] / vm[pd_] - 1) * 100, 2) if vm.get(pd_) else None}
    exp = [r[0] for r in con.execute("SELECT DISTINCT sym FROM opt WHERE date=? AND exp=? AND kind='idx' ORDER BY 1", [d, d]).fetchall()]
    flags = []
    for m in moves:
        if m["chg"] is not None and abs(m["chg"]) >= 1.5: flags.append(f'Big move: {m["sym"]} {m["chg"]:+.2f}%')
    if vix and vix["chg"] is not None and abs(vix["chg"]) >= 8: flags.append(f'India VIX {vix["chg"]:+.1f}%')
    if exp: flags.append("Expiry day: " + ", ".join(exp))
    tds = all_dates(); ts = set(tds); ev = _evs()
    for e in ev: e["react"] = _react(e["date"], e["cat"], tds, ts) or e["date"]
    dd = _D.fromisoformat(d)
    lo, hi = (dd - _T(days=7)).isoformat(), (dd + _T(days=7)).isoformat()
    return {"date": d, "dow": dd.strftime("%A"), "prev": pd_, "moves": moves, "vix": vix, "flags": flags,
            "events": [e for e in ev if e["react"] == d],
            "upcoming": [e for e in ev if d < e["react"] <= hi][:12], "recent": [e for e in ev if lo <= e["react"] < d][-12:]}

def api_event_add(q, con):
    d, t = (q.get("date") or "").strip(), (q.get("title") or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", d) or not t: raise ValueError("Date ane title banne joiye.")
    cat = q.get("cat") if q.get("cat") in EVENT_CATS else "OTHER"
    imp = q.get("impact") if q.get("impact") in ("H", "M", "L") else "M"
    os.makedirs("data", exist_ok=True); new = not os.path.exists(EVFILE)
    eid = "c" + str(int(time.time() * 1000))
    with open(EVFILE, "a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        if new: w.writerow(["id", "date", "cat", "title", "impact"])
        w.writerow([eid, d, cat, t[:200], imp])
    return {"ok": 1, "id": eid}

def api_event_del(q, con):
    eid = q.get("id") or ""
    if not eid.startswith("c"): raise ValueError("Builtin event delete nathi thata.")
    try:
        with open(EVFILE, newline="", encoding="utf-8") as fh: rows = list(csv.reader(fh))
    except FileNotFoundError:
        return {"ok": 0}
    keep = [r for r in rows if not (r and r[0] == eid)]
    with open(EVFILE, "w", newline="", encoding="utf-8") as fh: csv.writer(fh).writerows(keep)
    return {"ok": 1}

def api_news(q, con):
    """Google News RSS (date range) thi headlines. Internet joiye. Result cache: data/news/"""
    import urllib.request, urllib.parse, xml.etree.ElementTree as ET, email.utils, hashlib
    from datetime import date as _D, timedelta as _T, timezone
    d = q.get("date") or ""
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", d): raise ValueError("Date galat 6e.")
    query = (q.get("q") or 'Nifty OR Sensex OR RBI OR Fed OR crude OR "stock market"').strip()[:200]
    os.makedirs(os.path.join("data", "news"), exist_ok=True)
    path = os.path.join("data", "news", f"{d}_{hashlib.md5(query.encode()).hexdigest()[:10]}.json")
    if os.path.exists(path):
        return json.load(open(path, encoding="utf-8"))
    dd = _D.fromisoformat(d)
    qq = f"{query} after:{(dd - _T(days=1)).isoformat()} before:{(dd + _T(days=1)).isoformat()}"
    url = "https://news.google.com/rss/search?q=" + urllib.parse.quote(qq) + "&hl=en-IN&gl=IN&ceid=IN:en"
    try:
        raw = urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"}), timeout=15).read()
        root = ET.fromstring(raw)
    except Exception as e:
        return {"items": [], "err": f"News load na thayu ({e}). 'Google News ma khol' vapro."}
    ist = timezone(_T(hours=5, minutes=30)); items = []
    for it in root.iter("item"):
        t = (it.findtext("title") or "").strip(); sc = it.find("source"); src = (sc.text or "") if sc is not None else ""
        if src and t.endswith(" - " + src): t = t[:-(len(src) + 3)]
        try: p = email.utils.parsedate_to_datetime(it.findtext("pubDate") or "").astimezone(ist)
        except Exception: p = None
        items.append({"title": t, "link": it.findtext("link") or "", "source": src, "time": p.strftime("%Y-%m-%d %H:%M") if p else "", "day": p.date().isoformat() if p else ""})
    same = [x for x in items if x["day"] == d]
    res = {"items": (same or items)[:30], "exact": bool(same), "err": "" if items else "Aa date mate news na malya. Query badli juo ya Google News ma khol."}
    if items: json.dump(res, open(path, "w", encoding="utf-8"))
    return res

ROUTES = {"/api/info": api_info, "/api/symbols": api_symbols, "/api/dates": api_dates, "/api/expiries": api_expiries, "/api/calendar": api_calendar, "/api/strategy": api_strategy, "/api/diag": api_diag, "/api/parity": api_parity, "/api/cbt": api_cbt, "/api/cst": api_cst, "/api/events": api_events, "/api/context": api_context, "/api/news": api_news, "/api/event_add": api_event_add, "/api/event_del": api_event_del}
SCOPE = {"/api/symbols": lambda q: (None, None, 3), "/api/dates": lambda q: None,
         "/api/events": lambda q: None, "/api/news": lambda q: None, "/api/event_add": lambda q: None, "/api/event_del": lambda q: None,
         "/api/context": lambda q: (_prevd(q["date"]) or q["date"], q["date"], 0),
         "/api/expiries": lambda q: (q["date"], q["date"], 0), "/api/calendar": lambda q: (q["date"], q["e1"], 0),
         "/api/info": lambda q: (q["date"], q["date"], 0),
         "/api/cst": lambda q: (q["from"], q["to"], 0),
         "/api/cbt": lambda q: (_pre(q["from"]), q["to"], 0),
         "/api/parity": lambda q: (_prevd(q["date"]) or q["date"], min(q["es"], q["en"]), 0),
         "/api/diag": lambda q: (_prevd(q["date"]) or q["date"], q["date"], 0),
         "/api/strategy": lambda q: (q["date"], max(l["exp"] for l in json.loads(q["legs"])), 0)}

# ---------------------------------------------------------------- UI
HTML = r"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Options Lab</title><style>
:root{--bg:#0d1020;--cd:#151932;--bd:#262b4d;--tx:#e6e8f0;--mu:#9aa3c7;--pos:#3ddc97;--neg:#ff6b6b;--rat:#7cc7ff;--hlbg:#ffb300;--hltx:#1a1200;--sm:#101534;--in:#0d1020;--ac:#4f6bff}
body{background:var(--bg);color:var(--tx);font:13px system-ui,sans-serif;margin:0;padding:16px}
h2{margin:0 0 10px}.card{background:var(--cd);border:1px solid var(--bd);border-radius:10px;padding:12px;margin-bottom:14px}
.row{display:flex;flex-wrap:wrap;gap:10px;align-items:end}.row label{display:flex;flex-direction:column;font-size:11px;color:var(--mu);gap:3px}
.row label.cb{flex-direction:row;align-items:center;gap:5px;color:var(--tx);font-size:12px}
select,input{background:var(--in);color:var(--tx);border:1px solid var(--bd);border-radius:6px;padding:7px;font:inherit;min-width:110px}
input[type=checkbox]{min-width:0;width:15px;height:15px;padding:0}input[type=color]{min-width:0;width:38px;height:30px;padding:1px}
button{background:var(--ac);color:#fff;border:0;border-radius:6px;padding:9px 16px;font:inherit;cursor:pointer}
.sc{overflow:auto;max-height:72vh}table{border-collapse:separate;border-spacing:0;font-variant-numeric:tabular-nums}
table{font-size:var(--fs,13px)}th,td{padding:.3em .7em;border-bottom:1px solid var(--bd);text-align:right;white-space:nowrap}
th{position:sticky;top:0;z-index:3;background:var(--cd);color:var(--mu)}tr.h2 th{top:var(--h1,27px)}
td.k,th.k{position:sticky;left:0;background:var(--cd);font-weight:700;text-align:center;z-index:2;border-right:1px solid var(--bd)}th.k{z-index:5;top:0}
.sm td{background:var(--sm);text-align:center}.sm td.k{text-align:left;color:var(--tx)}
.s-spot td{color:#7cc7ff}.s-vix td{color:#ffb86b}.s-sf td{color:#c3a6ff}.s-st td{color:#ff8fb1}.s-fk td{color:var(--mu)}
.gk{color:color-mix(in srgb,var(--tx) 62%,#8a6cff)}.g{color:var(--pos)}.r{color:var(--neg)}.m{color:var(--mu)}.rt{color:var(--rat)}
tr.basr td:not(.hi){background:#ffe600!important;color:#111!important;font-weight:700}
td.hi{background:var(--hlbg)!important;color:var(--hltx)!important;font-weight:700}th.best{color:var(--hlbg)}
.pn{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:10px;margin-bottom:14px}.pn .card{margin:0}
.kv{display:grid;grid-template-columns:1fr 1fr;gap:6px}.kv div{background:var(--in);border:1px solid var(--bd);border-radius:6px;padding:6px}.kv span{display:block;font-size:10px;color:var(--mu)}
#msg{margin:8px 0;color:#ffb86b}#tab-dia table{font-size:var(--dfs,13px)}.sec{margin:0 0 8px;color:var(--mu);font-size:11px;text-transform:uppercase;letter-spacing:.06em}.gh{background:#262b4d}.dpw{display:flex;gap:4px;align-items:center;flex-wrap:wrap}.dpb{padding:5px 10px}.dpi{min-width:140px}.dpn{font-size:11px;color:#ffb86b}
.ctx summary{cursor:pointer}.ctxk{display:inline-block;margin:2px 16px 2px 0}.bdg{display:inline-block;padding:2px 9px;border-radius:10px;background:#ffb300;color:#1a1200;font-size:11px;font-weight:700;margin:2px 6px 2px 0}
.ecat{display:inline-block;padding:1px 7px;border-radius:8px;color:#10131f;font-size:10px;font-weight:700;margin-right:6px}.evi{margin:5px 0;line-height:1.4}
.chip{display:inline-block;margin:3px 6px 0 0;padding:2px 8px;border:1px solid var(--bd);border-radius:10px;cursor:pointer;font-size:11px}.ctxn a,.ctxg{color:var(--rat)}.ctxg{cursor:pointer;padding:8px 0}.ctxn div{margin:4px 0}
.evdel{padding:0 6px;margin-left:6px;background:transparent;color:var(--neg);border:1px solid var(--bd)}
</style></head><body>
<div class="row" style="margin-bottom:10px"><button id="t1">Calendar</button><button id="t2" style="background:#262b4d">Strategy</button><button id="t3" style="background:#262b4d">Diagonal</button><button id="t4" style="background:#262b4d">Parity</button><button id="t5" style="background:#262b4d">Backtest</button><button id="t6" style="background:#262b4d">Monthly Study</button></div>
<div id="tab-cal"><h2>Calendar</h2><div class="card"><div class="row">
<label>Symbol<select id="sym"></select></label><label>View date<select id="date"></select></label>
<label>Earlier expiry<select id="e1"></select></label><label>Later expiry<select id="e2"></select></label>
<label>Strike gap<input id="gap" type="number" value="50" style="width:80px"></label><label>No. of strikes<input id="n" type="number" value="20" style="width:80px"></label><label>Interest % (Greeks)<input id="rate" type="number" step="0.1" value="0" style="width:90px"></label>
<label>Strike mode<select id="mode"><option value="auto">Auto ATM</option><option value="manual">Manual start</option></select></label>
<label>Start strike<input id="start" type="number" value="24000" style="width:90px"></label><button id="run">Run Calendar</button></div><div id="msg"></div><div id="info" class="m"></div></div>
<div id="info2"></div>
<div class="card"><div class="row"><details style="position:relative"><summary style="cursor:pointer;padding:7px"><b>Columns</b> (show/hide, order)</summary><div id="ccols" class="card" style="position:absolute;z-index:9;min-width:230px;max-height:380px;overflow:auto"></div></details><b style="margin-left:10px">Top rows:</b><label class="cb"><input type="checkbox" class="sr" value="spot" checked autocomplete="off">Spot</label><label class="cb"><input type="checkbox" class="sr" value="vix" checked autocomplete="off">VIX</label><label class="cb"><input type="checkbox" class="sr" value="sf" checked autocomplete="off">Syn Fut</label><label class="cb"><input type="checkbox" class="sr" value="st" checked autocomplete="off">Straddle</label><label class="cb"><input type="checkbox" class="sr" value="fk" checked autocomplete="off">Final Strike</label>
<label>Highlight<select id="hl" autocomplete="off"><option value="dmax">Highest each day</option><option value="dmin">Lowest each day</option><option value="gmax">Highest (only 1 cell)</option><option value="gmin">Lowest (only 1 cell)</option><option value="max">Highest per strike</option><option value="min">Lowest per strike</option><option value="off">Off</option></select></label>
<label>Theme<select id="th" autocomplete="off"></select></label>
<label>Font size<input type="number" id="fs" value="13" min="9" max="26" style="width:70px"></label><label>Background<input type="color" id="bgc" value="#0d1020"></label><label>Table<input type="color" id="tbc" value="#151932"></label><label>Text<input type="color" id="txc" value="#e6e8f0"></label><label>Highlight colour<input type="color" id="hlc" value="#ffb300"></label><label>Ratio colour<input type="color" id="rtc" value="#7cc7ff"></label>
<label>Positive<input type="color" id="cpos"></label><label>Negative<input type="color" id="cneg"></label><button id="csave" class="gh">Save as default</button><button id="caddth" class="gh">Add theme</button><button id="dlce">Download CE CSV</button><button id="dlpe">Download PE CSV</button>
<details style="position:relative"><summary style="cursor:pointer;padding:7px">Dates (show/hide)</summary><div id="dl" class="card" style="position:absolute;z-index:9;max-height:260px;overflow:auto;min-width:170px"></div></details>
<button id="da" style="padding:6px 10px">All</button><button id="dn" style="padding:6px 10px">None</button></div></div>
<div id="out"></div></div>
<div id="tab-str" style="display:none"><h2>Strategy Analyzer</h2><div class="card"><div class="row">
<label>Symbol<select id="ssym"></select></label><label>Entry date<select id="sdate"></select></label>
<label>Expiry<select id="se1"></select></label><label>Far expiry (diagonal)<select id="se2"></select></label>
<label>Strategy<select id="stp" style="min-width:230px"><option value="straddle">Straddle: sell ATM CE+PE</option><option value="strangle">Strangle: sell CE/PE at ATM &plusmn;A</option>
<option value="rcall">Call ratio: buy 1 ATM CE, sell R CE at +A</option><option value="rput">Put ratio: buy 1 ATM PE, sell R PE at -A</option>
<option value="condor">Iron condor: sell &plusmn;A, buy &plusmn;(A+B)</option><option value="dcall">Diagonal call: sell near ATM, buy far at +A</option><option value="dput">Diagonal put: sell near ATM, buy far at -A</option></select></label>
<label>Strike gap<input id="sgap" type="number" value="50" style="width:70px"></label><label>A (offset)<input id="sa" type="number" value="200" style="width:80px"></label>
<label>B (wing)<input id="sb" type="number" value="200" style="width:80px"></label><label>R (ratio)<input id="srt" type="number" value="2" style="width:60px"></label>
<label>Lots<input id="slots" type="number" value="1" style="width:60px"></label><label class="cb"><input type="checkbox" id="srev">Reverse (buy/sell ulto)</label>
<button id="srun">Run Strategy</button></div><div id="smsg" style="margin:8px 0;color:#ffb86b"></div></div><div id="sout"></div></div>
<div id="tab-dia" style="display:none"><h2>Diagonal Scanner</h2>
<div class="card"><div class="sec">1. Selection</div><div class="row">
<label>Symbol<select id="dgsym"></select></label><label>View date<select id="dgdate"></select></label>
<label>Near expiry<select id="dge1"></select></label><label>Far expiry<select id="dge2"></select></label>
<label>Strike gap<input id="dggap" type="number" value="50" style="width:70px"></label><label>Auto: strikes each side<input id="dgn" type="number" value="10" style="width:80px"></label>
<label>Rate % (Excel = 0)<input id="dgr" type="number" step="0.1" value="0" style="width:90px"></label><label>Time (T)<select id="dgt"><option value="1">expiry &minus; date + 1 (Excel)</option><option value="0">expiry &minus; date</option></select></label></div></div>
<div class="card"><div class="sec">2. Blocks (darek gap ni alag strike range)</div><div id="dgb"></div>
<div class="row" style="margin-top:6px"><button id="dgadd" class="gh">+ Add block</button><button id="dgrun">Run Diagonal</button></div>
<div id="dgmsg" style="margin:8px 0;color:#ffb86b"></div><div class="m">Sell near strike K, buy far strike K&plusmn;gap. IV/Greeks EOD close thi (Excel logic: Black-Scholes, S = synthetic future, IV bisection). Theta = per din, Vega = per 1 vol point.</div></div>
<div class="card"><div class="sec">3. View, theme, download</div><div class="row">
<label class="cb"><input type="checkbox" id="dgrev">Reverse legs (far pehla, near pachi)</label>
<details style="position:relative"><summary style="cursor:pointer;padding:7px">Columns (show/hide)</summary><div id="dgcols" class="card" style="position:absolute;z-index:9;min-width:190px"></div></details>
<label>Font size<input id="dgfs" type="number" value="13" min="9" max="26" style="width:70px"></label><label>Theme<select id="dgth"></select></label>
<label>Background<input type="color" id="dgbg"></label><label>Table<input type="color" id="dgtb"></label><label>Text<input type="color" id="dgtx"></label><label>Positive<input type="color" id="dgpos"></label><label>Negative<input type="color" id="dgneg"></label>
<button id="dgsave" class="gh">Save as default</button><button id="dgaddth" class="gh">Add theme</button><button id="dgcsv">Download CSV</button></div></div>
<div id="dgout"></div></div>
<div id="tab-par" style="display:none"><h2>Parity: Sensex vs Nifty</h2><div class="card"><div class="row">
<label>Sensex symbol<select id="pss"></select></label><label>Nifty symbol<select id="pns"></select></label><label>View date<select id="pdate"></select></label>
<label>Sensex expiry<select id="pes"></select></label><label>Nifty expiry<select id="pen"></select></label><label>Expiry gap<span id="pgap" class="m" style="padding:8px 0">-</span></label></div>
<div class="row" style="margin-top:8px"><label>Ratio source<select id="psrc"><option value="spot">Spot (Sensex / Nifty)</option><option value="syn">Synthetic future (Excel G13)</option></select></label>
<label>Nifty qty multiplier (K)<input id="pk" type="number" step="0.05" value="3.25" style="width:90px"></label><label>Sensex lot<input id="plots" type="number" value="20" style="width:70px"></label><label>Nifty lot<input id="plotn" type="number" value="65" style="width:70px"></label>
<label>Nifty MROUND<input id="pngap" type="number" value="50" style="width:70px"></label><label>Sensex ATM gap<input id="psgap" type="number" value="100" style="width:80px"></label>
<label>Interest %<input id="prate" type="number" step="0.1" value="0" style="width:70px"></label>
<button id="prun">Run Parity</button><button id="pdl" class="gh">Download CSV</button>
<details style="position:relative"><summary style="cursor:pointer;padding:7px">Columns (show/hide)</summary><div id="pcols" class="card" style="position:absolute;z-index:9;min-width:210px;max-height:340px;overflow:auto"></div></details></div>
<div id="pmsg" style="margin:8px 0;color:#ffb86b"></div></div>
<div class="card"><div class="sec">Excel ladder (base pair ni upar niche)</div><div id="pl"></div>
<div class="row" style="margin-top:6px"><button id="pladd" class="gh">+ Add ladder</button><button id="pladd2" class="gh">+ Add CE ane PE ladder</button></div>
<div class="m" style="margin-top:6px">Base Sensex = Sensex spot ne "Sensex ATM gap" (100) thi MROUND. Base Nifty = (base Sensex &divide; spot ratio) ne "Nifty MROUND" (50) thi MROUND. Pachi Sensex base &plusmn; Sensex step ane Nifty base &plusmn; Nifty step, "Rows upar/niche" jetla. Base row yellow. Darek ladder ni K alag (3.3 / 3.25). Base jate set karvu hoy to "Base override" ma 74100/22900 lakho. Nifty shift = Nifty base ma &plusmn; points (Excel na biji block jem).</div></div>
<div class="card"><div class="sec">Blocks (darek gap / strike set ni alag table)</div><div id="pb"></div>
<div class="row" style="margin-top:6px"><button id="padd" class="gh">+ Add block</button><button id="padd3" class="gh">+ Add gaps 500 / 1000 / 1500</button></div>
<div class="m" style="margin-top:6px">Sensex gap = Sensex strike step. Auto = Nifty strike Sensex strike &divide; ratio, 50 thi MROUND. Manual: "Sensex from" ane "Nifty from" (ane Nifty step, Excel jem 74500 / 23050, step 500 / 150), ya "Manual pairs" ma 74500/23050, 74000/22900 lakho. Nifty shift = badhi Nifty strike ma &plusmn; points.</div></div><div id="pinfo"></div><div id="psnap"></div>
<div class="card"><div class="row"><b>Spread by date (strike pairs view date par fix)</b><label>Show<select id="pmet"><option value="sp">Spread (Sensex - K x Nifty)</option><option value="s">Sensex premium</option><option value="n">Nifty premium</option><option value="k">K x Nifty</option></select></label>
<label>Highlight<select id="phl"><option value="dmax">Highest each day</option><option value="dmin">Lowest each day</option><option value="gmax">Highest (only 1 cell)</option><option value="gmin">Lowest (only 1 cell)</option><option value="off">Off</option></select></label></div></div><div id="pser"></div></div>
<div id="tab-bt" style="display:none"><h2>Calendar Backtest (Index options, EOD)</h2>
<div class="card"><details open><summary style="cursor:pointer;font-weight:700">Aa strategy shu 6e ane expert traders kem vape 6e (vancho)</summary>
<div class="m" style="line-height:1.55;margin-top:8px">
<b>Long calendar:</b> same strike par <b>near expiry SELL</b> + <b>far expiry BUY</b>. Debit = far px - near px. <b>Max loss = debit</b> (cost sahit), eatle risk fixed 6e.<br>
<b>Profit kyathi:</b> near option ni theta jaldi tute (tame sell karyu 6e), far ni dhime. Spot strike ni aaspaas rahe to near lagbhag 0 thai jay ane far bache. Vega positive 6e: IV vadhe to far vadhare vadhe. <b>Loss:</b> spot strike thi khub door jay (banne bhaji), ke IV ghatay.<br>
<b>Expert rules (aa tab ma filters 6e):</b>
1) <b>IV ratio = near IV / far IV &ge; 1</b>: near mehngu hoy tyare sell. 2) <b>Far IV percentile nicho</b> (&le; 0.8, 1 varsh na rolling): far sasti hoy tyare buy. 3) <b>Liquidity</b>: volume ane OI joiye, nahi to bid-ask khai jay. 4) <b>VIX bahu ooncho na hoy</b> (IV crush no risk). 5) <b>Exit rules</b>: profit 20-30% of debit par book, loss 30-50% of debit par cut, near expiry thi 1-2 divas pehla nikli jao (gamma / pin risk). 6) <b>Spot move exit</b>: spot entry thi &plusmn;X% jay to tent ni bahar, adjust/exit. 7) <b>Double calendar</b>: CE upar (+X%) ane PE niche (-X%) alag calendar, tent pahoro, spot move vadhare sahan thay, pan debit vadhare. 8) <b>Size</b>: ek trade ma capital no 1-2% j risk (debit x lot x lots).<br>
<b>Result kem vancvu:</b> <b>PF (profit factor)</b> = kul profit / kul loss, 1.3 thi vadhare joiye. <b>Max DD</b> = equity ma moti ma moti ghat. <b>In-sample</b> = split date pehla (parameter shodhva), <b>Out-of-sample</b> = split pachi (sachi test). In-sample ma saru ane OOS ma kharab = <b>overfit</b>. Ek j varsh ma profit = luck. Niche "Verdict" 5 checks batave 6e.<br>
<b>Data note:</b> EOD close thi j test 6e (intraday nathi), aetle SL/TP divas ne ant na price par pade 6e. Spot: bhavcopy underlying (Jul 2024 pachi) &rarr; NSE index file &rarr; put-call parity andaaz. P&amp;L <b>points</b> ma 6e (sachu measure), Rs = points x Lot size.</div></details></div>
<div class="card"><div class="row">
<label>Index<select id="btsym"></select></label>
<label>Mode<select id="btmode"><option value="ce">ATM Call calendar (CE)</option><option value="pe">ATM Put calendar (PE)</option><option value="both">ATM CE + PE calendar</option><option value="dbl">Double calendar (CE upar, PE niche)</option></select></label>
<label>From<input id="btfrom" type="date"></label><label>To<input id="btto" type="date"></label><label>Split (in/out sample)<input id="btsplit" type="date" value="2023-01-01"></label></div>
<div class="row" style="margin-top:8px">
<label>Near DTE from<input id="btnl" type="number" step="any" value="7" style="width:70px" placeholder=""></label><label>Near DTE to<input id="btnh" type="number" step="any" value="15" style="width:70px" placeholder=""></label><label>Far DTE from<input id="btfl" type="number" step="any" value="30" style="width:70px" placeholder=""></label><label>Far DTE to<input id="btfh" type="number" step="any" value="45" style="width:70px" placeholder=""></label><label>Far target DTE<input id="btft" type="number" step="any" value="37" style="width:70px" placeholder=""></label><label>Double: strike offset %<input id="btoff" type="number" step="any" value="1" style="width:90px" placeholder=""></label></div>
<div class="row" style="margin-top:8px">
<label>Min IV ratio (near/far)<input id="btrat" type="number" step="any" value="1" style="width:90px" placeholder=""></label><label>Max far-IV percentile (0-1)<input id="btpct" type="number" step="any" value="0.8" style="width:110px" placeholder=""></label><label>Min volume<input id="btvol" type="number" step="any" value="100" style="width:80px" placeholder=""></label><label>Min OI (0=off)<input id="btoi" type="number" step="any" value="0" style="width:80px" placeholder=""></label><label>Max VIX (0=off)<input id="btvix" type="number" step="any" value="0" style="width:80px" placeholder=""></label></div>
<div class="row" style="margin-top:8px">
<label>Take profit % of debit<input id="bttp" type="number" step="any" value="30" style="width:90px" placeholder=""></label><label>Stop loss % of debit<input id="btsl" type="number" step="any" value="40" style="width:90px" placeholder=""></label><label>Exit when near DTE &le;<input id="btxd" type="number" step="any" value="2" style="width:80px" placeholder=""></label><label>Exit if spot moves % (0=off)<input id="btmv" type="number" step="any" value="0" style="width:100px" placeholder=""></label><label>Rate % (IV mate)<input id="btrate" type="number" step="any" value="7" style="width:70px" placeholder=""></label><label>Cost pts / leg / side<input id="btcost" type="number" step="any" value="1" style="width:80px" placeholder=""></label><label>Lot size (Rs mate)<input id="btlot" type="number" step="any" value="1" style="width:80px" placeholder=""></label>
<label style="flex-direction:row;gap:6px;align-items:center"><input id="bttune" type="checkbox" style="width:auto"> Auto-tune (in-sample grid)</label>
<button id="btrun">Run Backtest</button><button id="btdl" class="gh">Download trades CSV</button></div>
<div id="btmsg" style="margin:8px 0;color:#ffb86b"></div></div>
<div id="btout"></div></div>
<div id="tab-cs" style="display:none"><h2>Monthly Calendar Study (ek fixed idea no itihas)</h2>
<div class="card"><details open><summary style="cursor:pointer;font-weight:700">Aa tab shu kare 6e (pehla vancho)</summary>
<div class="m" style="line-height:1.55;margin-top:8px">
Tame ek <b>fixed idea</b> aapo: jem ke "<b>Near = 2jo monthly expiry (29-Dec) SELL, Far = tena pachi no monthly (25-Jan) BUY, ATM ke OTM strike, 15-20 divas hold</b>". Tool itihas ma <b>darek divas e</b> aa j setup kholine 15/20 divas pachi band kare 6e, ane batave 6e ke <b>average profit ke loss</b> thayo, ketla % trade ma profit, ane spot ketlo hali tyare shu thayu.<br>
<b>Near month no.</b>: 1 = chalu mahino no monthly, 2 = tena pachi no (aaje 30-Oct hoy to 2 = 29-Dec). <b>Far gap</b>: near thi ketla monthly aagal (1 = 25-Jan).<br>
<b>Strike offsets (points)</b>: comma thi lakho, jem <b>0,200,300,500</b>. Mode "Call+Put": 0 = ATM CE+PE (calendar straddle), 200 = CE ATM+200 ane PE ATM-200 (double calendar). Mode "Call": CE ATM+offset. Mode "Put": PE ATM-offset. Darek offset ni alag ginti thase, tethi tame ATM vs OTM sidhu compare kari sako.<br>
<b>Hold days</b>: calendar divas, jem <b>15,20</b>. <b>Entry near DTE</b>: khali rakho to aaje na jevi DTE (&plusmn;7 divas) vala j divas gano (jem aaje near expiry ~60 divas dur 6e). <br>
<b>Dhyan:</b> darek divas nu entry alag gane 6e, eatle trades ek bija sathe overlap thay. Sachi independent sankhya lagbhag <b>trades &divide; hold days</b> jetli. Nana sample par bharoso na karo. P&amp;L <b>points</b> ma (after cost); Rs = avg points x lot size. EOD close thi, aetle intraday SL nathi.</div></details></div>
<div class="card"><div class="row"><label>Index<select id="cssym"></select></label>
<label>Mode<select id="csmode"><option value="dbl">Call+Put (0 = ATM, &gt;0 = double calendar)</option><option value="ce">Call calendar (CE ATM+offset)</option><option value="pe">Put calendar (PE ATM-offset)</option></select></label>
<label>Near month no.<input id="csn" type="number" step="any" value="2" style="width:80px" placeholder=""></label><label>Far gap (months)<input id="csg" type="number" step="any" value="1" style="width:80px" placeholder=""></label><label>Strike offsets (points)<input id="csoffs" type="text" step="any" value="0,200,300,500" style="width:170px" placeholder=""></label><label>Hold days<input id="csholds" type="text" step="any" value="15,20" style="width:110px" placeholder=""></label></div>
<div class="row" style="margin-top:8px"><label>From<input id="csfrom" type="date"></label><label>To<input id="csto" type="date"></label>
<label>Entry near DTE from<input id="csnl" type="number" step="any" value="" style="width:100px" placeholder="auto"></label><label>Entry near DTE to<input id="csnh" type="number" step="any" value="" style="width:100px" placeholder="auto"></label><label>Min volume<input id="csvol" type="number" step="any" value="1" style="width:80px" placeholder=""></label><label>Cost pts / leg / side<input id="cscost" type="number" step="any" value="1" style="width:90px" placeholder=""></label><label>Lot size (Rs mate)<input id="cslot" type="number" step="any" value="1" style="width:80px" placeholder=""></label><label>Rate % (IV mate)<input id="csrate" type="number" step="any" value="7" style="width:70px" placeholder=""></label>
<button id="csrun">Run Study</button><button id="csdl" class="gh">Download selected trades CSV</button></div>
<div id="csmsg" style="margin:8px 0;color:#ffb86b"></div></div>
<div id="csout"></div></div>
<script>
const $=s=>document.querySelector(s);
async function api(p,q){const r=await fetch(p+'?'+new URLSearchParams(q));const t=await r.text();let j;try{j=JSON.parse(t)}catch(e){throw new Error(!t.trim()?'Server e khali jawab apyo (HTTP '+r.status+'). Lagbhag server ni memory puri thai gai ke restart thayu. Nano From-To range lo (jem 1-2 varsh) ane fari try karo.':'Server no jawab JSON nathi (HTTP '+r.status+'): '+t.slice(0,150).replace(/<[^>]*>/g,' '))}if(j.error)throw new Error(j.error);return j}
const msg=t=>$('#msg').textContent=t||'';
function fill(el,a,sel){el.innerHTML=a.map(x=>`<option>${x}</option>`).join('');if(sel&&a.includes(sel))el.value=sel}
async function loadSyms(){try{const s=await api('/api/symbols',{});fill($('#sym'),s.map(x=>x.sym),'NIFTY');await loadDates()}catch(e){msg(e.message)}}
async function loadDates(){const d=await api('/api/dates',{sym:$('#sym').value});fill($('#date'),d);$('#date').value=d[0];await loadExp()}
async function loadExp(){const e=await api('/api/expiries',{sym:$('#sym').value,date:$('#date').value});fill($('#e1'),e);fill($('#e2'),e);$('#e2').value=e[1]||e[0]}
$('#sym').onchange=()=>loadDates().catch(e=>msg(e.message));$('#date').onchange=()=>loadExp().catch(e=>msg(e.message));
const f=(v,d=2)=>v==null?'-':(+v).toLocaleString('en-IN',{minimumFractionDigits:d,maximumFractionDigits:d});
let R=null,SHOW=new Set();
const NM=['Earlier','Later','Ratio','Value','IV E %','IV L %','Delta E','Delta L','Delta ratio','Vega E','Vega L','Theta E','Theta L','Gamma E','Gamma L','IV diff'],DEC=[2,2,2,2,2,2,4,4,3,3,3,3,3,6,6,2];
function tbl(t,dates,title,showTop){const cols=[...document.querySelectorAll('.vc:checked')].map(x=>+x.value),nm=NM,hl=$('#hl').value,sr=[...document.querySelectorAll('.sr:checked')].map(x=>x.value);
const idx=R.dates.map((d,i)=>SHOW.has(d)?i:-1).filter(i=>i>=0);
if(!cols.length||!idx.length)return `<div class="card m">${title}: kai column/date select nathi.</div>`;
let best=-1,gi=-1,gr=-1;
if(hl==='gmax'||hl==='gmin'){let gv=null;t.forEach((r,ri)=>idx.forEach(i=>{const v=r.cells[i][3];if(v!=null&&(gv===null||(hl==='gmax'?v>gv:v<gv))){gv=v;gi=i;gr=ri}}));best=gi}
else if(hl==='max'||hl==='min'){let bv=null;idx.forEach(i=>{const v=t.map(r=>r.cells[i][3]).filter(x=>x!=null);if(v.length){const a=v.reduce((p,c)=>p+c,0)/v.length;if(bv===null||(hl==='max'?a>bv:a<bv)){bv=a;best=i}}})}
const dcol={};if(hl==='dmax'||hl==='dmin')idx.forEach(i=>{let bv=null,br=-1;t.forEach((r,ri)=>{const v=r.cells[i][3];if(v!=null&&(bv===null||(hl==='dmax'?v>bv:v<bv))){bv=v;br=ri}});dcol[i]=br});
let h='<tr><th class="k" rowspan="2">STRIKE</th>'+idx.map(i=>`<th colspan="${cols.length}" class="${i===best?'best':''}">${R.dates[i]}${i===best?' \u2605':''}</th>`).join('')+'</tr><tr class="h2">'+idx.map(()=>cols.map(c=>`<th>${nm[c]}</th>`).join('')).join('')+'</tr>';
const M=R.meta,srow=(lab,fn,k)=>`<tr class="sm s-${k}"><td class="k">${lab}</td>`+idx.map(i=>`<td colspan="${cols.length}">${fn(M[i])}</td>`).join('')+'</tr>',pf=v=>v==null?'-':f(v);
const SR={spot:[['SPOT',m=>pf(m.spot)+(m.est?'*':'')]],vix:[['INDIA VIX',m=>pf(m.vix)]],sf:[['SYN FUT (EARLIER)',m=>pf(m.e.sf)],['SYN FUT (LATER)',m=>pf(m.l.sf)]],st:[['STRADDLE (EARLIER)',m=>pf(m.e.straddle)],['STRADDLE (LATER)',m=>pf(m.l.straddle)]],fk:[['FINAL STRIKE (E / L)',m=>(m.e.fk??'-')+' / '+(m.l.fk??'-')]]};
const top=!showTop?'':sr.map(k=>SR[k].map(([l,fn])=>srow(l,fn,k)).join('')).join('');
const b=t.map((r,ri)=>{let hi=-1;if(hl==='max'||hl==='min'){let bb=null;idx.forEach(i=>{const v=r.cells[i][3];if(v!=null&&(bb===null||(hl==='max'?v>bb:v<bb))){bb=v;hi=i}})}
return `<tr><td class="k">${r.strike}</td>`+idx.map(i=>{const c=r.cells[i];return cols.map(k=>{const v=c[k];let cl=k===3?(v==null?'m':v>=0?'g':'r'):k===2?'rt':k>=4?'gk':'';if(k===3&&((hi===i)||(ri===gr&&i===gi)||dcol[i]===ri))cl+=' hi';return `<td class="${cl}">${f(v,DEC[k])}</td>`}).join('')}).join('')+'</tr>'}).join('');
return `<div class="card"><b>${title}</b><div class="sc"><table>${h}${top}${b}</table></div></div>`}
function panel(r){const p=x=>x==null?'-':f(x),box=(a,b)=>`<div><span>${a}</span><b>${b}</b></div>`;
const side=`<div class="card"><b>VIEW DATE: ${$('#date').value}</b><div class="kv" style="margin-top:8px">${box('SPOT',p(r.spot))}${box('INDIA VIX',r.vix==null?'n/a (IDX file load karo)':p(r.vix))}</div></div>`;
const ex=(e,t)=>e.atm==null?`<div class="card"><b>${t}: ${e.exp}</b><p class="m">Data nathi</p></div>`:`<div class="card"><b>${t}: ${e.exp}</b><div class="kv" style="margin-top:8px">${box('PROVISIONAL ATM',p(e.prov))}${box('ATM STRIKE USED',p(e.atm))}${box('ATM CE',p(e.ce))}${box('ATM PE',p(e.pe))}${box('SYNTHETIC FUTURE',p(e.sf))}${box('FINAL STRIKE',p(e.fk))}${box('FINAL CE',p(e.fce))}${box('FINAL PE',p(e.fpe))}${box('STRADDLE VALUE',p(e.straddle))}</div></div>`;
$('#info2').innerHTML=`<div class="pn">${side}${ex(r.exp[0],'EARLIER EXPIRY')}${ex(r.exp[1],'LATER EXPIRY')}</div>`}
function fit(){document.querySelectorAll('.sc').forEach(el=>{const t=el.querySelector('table');if(t&&t.rows[0])t.style.setProperty('--h1',t.rows[0].offsetHeight+'px')})}
function sync(){const a=[...document.querySelectorAll('.sc')];a.forEach(el=>el.onscroll=()=>a.forEach(o=>{if(o!==el&&o.scrollLeft!==el.scrollLeft)o.scrollLeft=el.scrollLeft}))}
function draw(){if(!R)return;$('#out').innerHTML=R.dates.length?tbl(R.CE,R.dates,'CALL / CE CALENDAR',true)+tbl(R.PE,R.dates,'PUT / PE CALENDAR',false):'<div class="card m">Aa range ma data nathi.</div>';fit();sync()}
function dlist(){$('#dl').innerHTML=R.dates.map(d=>`<label style="flex-direction:row;gap:6px;color:var(--tx)"><input type="checkbox" class="dc" value="${d}" ${SHOW.has(d)?'checked':''}>${d}</label>`).join('')}
document.addEventListener('change',e=>{if(e.target.classList.contains('dc')){e.target.checked?SHOW.add(e.target.value):SHOW.delete(e.target.value);draw()}else if(e.target.classList.contains('vc')||e.target.classList.contains('sr')||e.target.id==='hl')draw()});
$('#da').onclick=()=>{if(R){SHOW=new Set(R.dates);dlist();draw()}};$('#dn').onclick=()=>{if(R){SHOW=new Set();dlist();draw()}};
$('#run').onclick=async()=>{msg('Loading...');try{const q={sym:$('#sym').value,date:$('#date').value,e1:$('#e1').value,e2:$('#e2').value,gap:$('#gap').value,n:$('#n').value,mode:$('#mode').value,start:$('#start').value,rate:$('#rate').value};
const [r,inf]=await Promise.all([api('/api/calendar',q),api('/api/info',q)]);R=r;SHOW=new Set(r.dates);msg('');$('#info').textContent=`Strikes: ${r.strikes[0]} - ${r.strikes[r.strikes.length-1]} | Dates: ${r.dates.length}`;panel(inf);dlist();draw()}catch(e){msg(e.message)}};
const mk=(bg,cd,bd,tx,mu,ac,dk)=>({'--bg':bg,'--cd':cd,'--bd':bd,'--tx':tx,'--mu':mu,'--ac':ac,'--sm':'color-mix(in srgb,'+cd+' 88%,#000)','--in':dk?bg:cd,'--pos':dk?'#3ddc97':'#0a8f5a','--neg':dk?'#ff6b6b':'#d63a3a'});
const TH={midnight:mk('#0d1020','#151932','#262b4d','#e6e8f0','#9aa3c7','#4f6bff',1),charcoal:mk('#121212','#1c1c1e','#333333','#ececec','#a0a0a0','#4f6bff',1),light:mk('#f4f6fb','#ffffff','#dfe3ee','#1a1f36','#5b6482','#2563eb',0),
vs13light:mk('#eeeef2','#ffffff','#ccceDB','#1e1e1e','#717171','#007acc',0),vs13dark:mk('#2d2d30','#1e1e1e','#3f3f46','#f1f1f1','#9b9b9b','#007acc',1),vs13blue:mk('#293955','#364e6f','#4d6082','#ffffff','#b0bdd3','#4f9fe0',1),
vs2012:mk('#f5f5f5','#ffffff','#cccccc','#1e1e1e','#717171','#1ba1e2',0),vs2010:mk('#cbd3e3','#ffffff','#99a9c7','#1b2a4a','#5c6d8f','#3a5ba0',0),vs2008:mk('#d6dbe9','#ffffff','#9aa8c7','#1b2540','#5a678a','#4a6fb2',0),
vs2005:mk('#e9e7e3','#ffffff','#aca899','#000000','#5f5b50','#316ac5',0),vs60:mk('#d4d0c8','#ffffff','#808080','#000000','#404040','#000080',0),vsnet2003:mk('#ece9d8','#ffffff','#aca899','#000000','#5f5b50','#0a246a',0),
office2003:mk('#c6d8f4','#f4f8fe','#8db2e3','#15428b','#4b6ea8','#3a6ea5',0),office2007:mk('#d9e7f8','#ffffff','#99bbe8','#15428b','#5073a6','#1f5db5',0),office2010:mk('#e4e7ec','#ffffff','#b9c0cc','#1e395b','#5b6b82','#2a5ca8',0),
office2013:mk('#f3f3f3','#ffffff','#d4d4d4','#262626','#6b6b6b','#2b579a',0),office2016:mk('#e6e6e6','#ffffff','#c8c8c8','#262626','#666666','#217346',0),lightdefault:mk('#f0f0f0','#ffffff','#cccccc','#1a1a1a','#666666','#0078d7',0),defaultdark:mk('#1f1f1f','#2b2b2b','#444444','#eeeeee','#a0a0a0','#0a84ff',1)};
const TL={midnight:'Midnight',charcoal:'Charcoal',light:'Light',vs13light:'VS 2013 Light',vs13dark:'VS 2013 Dark',vs13blue:'VS 2013 Blue',vs2012:'Visual Studio 2012',vs2010:'Visual Studio 2010',vs2008:'Visual Studio 2008',vs2005:'Visual Studio 2005',vs60:'Visual Studio 6.0',vsnet2003:'Visual Studio .NET 2003',office2003:'Office 2003',office2007:'Office 2007',office2010:'Office 2010',office2013:'Office 2013',office2016:'Office 2016',lightdefault:'Light Default',defaultdark:'Default Dark'};
try{Object.assign(TH,JSON.parse(localStorage.getItem('ol-themes')||'{}'))}catch(e){}for(const k in TH)if(![...$('#th').options].some(o=>o.value===k))$('#th').add(new Option(TL[k]||k,k));
const root=document.documentElement.style,IDS=['th','hl','fs','bgc','tbc','txc','hlc','rtc'];
const save=()=>{try{const o={};IDS.forEach(i=>o[i]=$('#'+i).value);localStorage.setItem('ol-set',JSON.stringify(o))}catch(e){}};
const mix=c=>'color-mix(in srgb,'+c+' 88%,#000)';
const APP={bgc:v=>root.setProperty('--bg',v),tbc:v=>{root.setProperty('--cd',v);root.setProperty('--sm',mix(v))},txc:v=>root.setProperty('--tx',v),hlc:v=>root.setProperty('--hlbg',v),rtc:v=>root.setProperty('--rat',v),fs:v=>{root.setProperty('--fs',(+v||13)+'px');fit()}};
function theme(){const t=TH[$('#th').value];for(const k in t)root.setProperty(k,t[k]);$('#bgc').value=t['--bg'];$('#tbc').value=t['--cd'];$('#txc').value=t['--tx']}
$('#th').onchange=()=>{theme();save()};
for(const k in APP)$('#'+k).oninput=()=>{APP[k]($('#'+k).value);save()};
$('#hl').onchange=()=>{draw();save()};
try{const o=JSON.parse(localStorage.getItem('ol-set')||'{}');if(o.th&&TH[o.th]){$('#th').value=o.th;theme()}for(const k in APP)if(o[k]){$('#'+k).value=o[k];APP[k](o[k])}$('#hl').value=o.hl||'dmax'}catch(e){$('#hl').value='dmax'}
if(!$('#hl').value)$('#hl').value='dmax';
const smsg=t=>$('#smsg').textContent=t||'';
function tab(n){['cal','str','dia','par','bt','cs'].forEach((x,i)=>$('#tab-'+x).style.display=i===n?'':'none');if(n===1&&!$('#ssym').options.length)loadS();if(n===2&&!$('#dgsym').options.length)loadDg();if(n===3&&!$('#pss').options.length)loadPar();if(n===4&&!$('#btsym').options.length)loadBt();if(n===5&&!$('#cssym').options.length)loadCs()}
$('#t1').onclick=()=>tab(0);$('#t2').onclick=()=>tab(1);$('#t3').onclick=()=>tab(2);$('#t4').onclick=()=>tab(3);$('#t5').onclick=()=>tab(4);$('#t6').onclick=()=>tab(5);
const dgmsg=t=>$('#dgmsg').textContent=t||'';
async function loadDg(){try{const s=await api('/api/symbols',{});fill($('#dgsym'),s.map(x=>x.sym),'NIFTY');await loadDgD()}catch(e){dgmsg(e.message)}}
async function loadDgD(){const d=await api('/api/dates',{});fill($('#dgdate'),d);$('#dgdate').value=d[0];await loadDgE()}
async function loadDgE(){const e=await api('/api/expiries',{sym:$('#dgsym').value,date:$('#dgdate').value});fill($('#dge1'),e);fill($('#dge2'),e);$('#dge2').value=e[1]||e[0]}
$('#dgsym').onchange=()=>loadDgD().catch(e=>dgmsg(e.message));$('#dgdate').onchange=()=>loadDgE().catch(e=>dgmsg(e.message));
function dgRow(ot,gap,fr,to,st){const d=document.createElement('div');d.className='row dgr';d.style.marginBottom='6px';
d.innerHTML=`<label>Type<select class="b-ot"><option>CE</option><option>PE</option></select></label><label>Gap<input class="b-g" type="number" value="${gap}" style="width:80px"></label><label>From strike<input class="b-f" type="number" value="${fr}" placeholder="auto" style="width:100px"></label><label>To strike<input class="b-t" type="number" value="${to}" placeholder="auto" style="width:100px"></label><label>Step<input class="b-s" type="number" value="${st}" style="width:70px"></label><button class="b-x" style="padding:6px 10px;background:#262b4d">Remove</button>`;
d.querySelector('.b-ot').value=ot;d.querySelector('.b-x').onclick=()=>d.remove();$('#dgb').appendChild(d)}
$('#dgadd').onclick=()=>dgRow('CE',200,'','',100);dgRow('CE',200,'','',100);dgRow('CE',300,'','',100);dgRow('CE',400,'','',100);
function drawDg(r){r=r||DG;if(!r)return;const rev=$('#dgrev').checked,on=DGON,cs=dgOrd(rev).filter(c=>on.has(c.g)),g=(v,d=2)=>v==null?'-':f(v,d),box=(a,b)=>`<div><span>${a}</span><b>${b}</b></div>`;
let h=`<div class="pn"><div class="card"><b>VIEW DATE</b><div class="kv" style="margin-top:8px">${box('SPOT',g(r.spot))}${box('ATM (near)',r.atm??'-')}${box('PREV CLOSE DATE',r.prev||'-')}</div></div>`+r.info.map((e,i)=>`<div class="card"><b>${i?'FAR':'NEAR'} EXPIRY: ${e.exp} (DTE ${r.dte[i]})</b><div class="kv" style="margin-top:8px">${box('SYNTHETIC FUTURE',g(e.sf))}${box('FINAL STRIKE',g(e.fk,0))}${box('STRADDLE',g(e.st))}${box('STRADDLE % OF SPOT',e.pct==null?'-':g(e.pct)+'%')}</div></div>`).join('')+'</div>';
r.blocks.forEach(b=>{h+=`<div class="card"><b>${b.ot==='CE'?'CALL':'PUT'} DIAGONAL: gap ${b.gap} (sell near K, buy far K${b.ot==='CE'?'+':'-'}${Math.abs(b.gap)})</b><div class="sc"><table><tr><th class="k">${rev?'FAR':'NEAR'} K</th>${cs.map(c=>`<th>${c.l}</th>`).join('')}</tr>`
+(b.rows.length?b.rows.map(x=>`<tr${x[16]?' style="font-weight:700"':''}><td class="k">${x[rev?1:0]}${x[16]?' \u2605':''}</td>`+cs.map(c=>{const v=c.r(x);return `<td class="${c.c?(v==null?'':v>=0?'g':'r'):c.cls||''}">${g(v,c.d)}</td>`}).join('')+'</tr>').join(''):`<tr><td colspan="${cs.length+1}" class="m">Aa block ma data nathi (strike range ke gap tapasjo).</td></tr>`)+'</table></div></div>'});
$('#dgout').innerHTML=h}
$('#dgrun').onclick=async()=>{dgmsg('Loading...');try{drawDg(DG=await api('/api/diag',{sym:$('#dgsym').value,date:$('#dgdate').value,e1:$('#dge1').value,e2:$('#dge2').value,blocks:JSON.stringify([...document.querySelectorAll('#dgb .dgr')].map(d=>({ot:d.querySelector('.b-ot').value,gap:+d.querySelector('.b-g').value,from:d.querySelector('.b-f').value,to:d.querySelector('.b-t').value,step:d.querySelector('.b-s').value}))),gap:$('#dggap').value,n:$('#dgn').value,r:$('#dgr').value,tadd:$('#dgt').value}));dgmsg('')}catch(e){dgmsg(e.message)}};
async function loadS(){try{const s=await api('/api/symbols',{});fill($('#ssym'),s.map(x=>x.sym),'NIFTY');await loadSD()}catch(e){smsg(e.message)}}
async function loadSD(){const d=await api('/api/dates',{});fill($('#sdate'),d);$('#sdate').value=d[0];await loadSE()}
async function loadSE(){const e=await api('/api/expiries',{sym:$('#ssym').value,date:$('#sdate').value});fill($('#se1'),e);fill($('#se2'),e);$('#se2').value=e[1]||e[0]}
$('#ssym').onchange=()=>loadSD().catch(e=>smsg(e.message));$('#sdate').onchange=()=>loadSE().catch(e=>smsg(e.message));
const TPL={straddle:(k,A,B,R)=>[['CE',k,-1,0],['PE',k,-1,0]],strangle:(k,A,B,R)=>[['CE',k+A,-1,0],['PE',k-A,-1,0]],
rcall:(k,A,B,R)=>[['CE',k,1,0],['CE',k+A,-R,0]],rput:(k,A,B,R)=>[['PE',k,1,0],['PE',k-A,-R,0]],
condor:(k,A,B,R)=>[['CE',k+A,-1,0],['CE',k+A+B,1,0],['PE',k-A,-1,0],['PE',k-A-B,1,0]],
dcall:(k,A,B,R)=>[['CE',k,-1,0],['CE',k+A,1,1]],dput:(k,A,B,R)=>[['PE',k,-1,0],['PE',k-A,1,1]]};
function chart(pts,t,lx,mk){const W=720,H=230,P=50,xs=pts.map(p=>p[0]),ys=pts.map(p=>p[1]);
let x0=Math.min(...xs),x1=Math.max(...xs),y0=Math.min(0,...ys),y1=Math.max(0,...ys);if(y0===y1)y1=y0+1;
const X=x=>P+(x-x0)/((x1-x0)||1)*(W-2*P),Y=y=>H-24-(y-y0)/(y1-y0)*(H-38);
const tx=(x,y,s,a)=>`<text x="${x}" y="${y}" fill="var(--mu)" font-size="10" text-anchor="${a}">${s}</text>`;
return `<div class="card"><b>${t}</b><svg viewBox="0 0 ${W} ${H}" style="width:100%;max-width:${W}px;display:block"><line x1="${P}" x2="${W-P}" y1="${Y(0)}" y2="${Y(0)}" stroke="var(--mu)" stroke-dasharray="4"/>`
+(mk!=null&&mk>=x0&&mk<=x1?`<line x1="${X(mk)}" x2="${X(mk)}" y1="14" y2="${H-24}" stroke="var(--hlbg)" stroke-dasharray="3"/>`:'')
+`<polyline fill="none" stroke="var(--rat)" stroke-width="2" points="${pts.map(p=>X(p[0])+','+Y(p[1])).join(' ')}"/>`
+tx(P-4,Y(y1)+4,f(y1,0),'end')+tx(P-4,Y(y0)+4,f(y0,0),'end')+tx(P,H-8,lx[0],'start')+tx(W-P,H-8,lx[1],'end')+'</svg></div>'}
function drawS(r){const m=(r.lot||1)*r.lots,u=r.lot?'\u20b9':'pts',cl=x=>x>=0?'g':'r',v=x=>typeof x==='string'?x:f(x*m,0),box=(a,b,c)=>`<div><span>${a}</span><b class="${c||''}">${b}</b></div>`,p=r.pay;
const lg=r.legs.map(l=>`<tr><td>${l.qty>0?'BUY':'SELL'} ${Math.abs(l.qty)}</td><td>${l.exp}</td><td>${l.ot}</td><td>${l.strike}</td><td>${f(l.entry)}</td></tr>`).join('');
let h=`<div class="pn"><div class="card"><b>LEGS</b><div class="sc"><table><tr><th>Side</th><th>Expiry</th><th>Type</th><th>Strike</th><th>Entry</th></tr>${lg}</table></div></div>
<div class="card"><b>SUMMARY (${u}, ${r.lots} lot${r.lot?' \u00d7 '+r.lot:''})</b><div class="kv" style="margin-top:8px">${box(r.net>0?'NET DEBIT':'NET CREDIT',v(Math.abs(r.net)))}${box('SPOT @ ENTRY',r.spot==null?'-':f(r.spot))}`
+(p?`${box('MAX PROFIT',v(p.maxp),'g')}${box('MAX LOSS',v(p.maxl),'r')}${box('BREAKEVEN(S)',p.be.length?p.be.map(x=>f(x,0)).join(' / '):'-')}`:`${box('EXPIRY PAYOFF','Diagonal: alag expiry, exact payoff nathi')}`)+`</div></div></div>`;
if(p)h+=chart(p.curve.map(c=>[c[0],c[1]*m]),`EXPIRY PAYOFF (${u})`,[f(p.curve[0][0],0),f(p.curve[p.curve.length-1][0],0)],r.spot);
if(r.dates.length>1)h+=chart(r.pnl.map((y,i)=>[i,y*m]),`P&L BY DATE (${u})`,[r.dates[0],r.dates[r.dates.length-1]],null);
h+=`<div class="card"><b>DAILY P&L</b><div class="sc"><table><tr><th>Date</th><th>Spot</th><th>P&L pts</th><th>P&L ${u}</th></tr>`+r.dates.map((d,i)=>`<tr><td>${d}</td><td>${r.spots[i]==null?'-':f(r.spots[i])}</td><td class="${cl(r.pnl[i])}">${f(r.pnl[i])}</td><td class="${cl(r.pnl[i])}">${f(r.pnl[i]*m,0)}</td></tr>`).join('')+'</table></div></div>';
$('#sout').innerHTML=h}
$('#srun').onclick=async()=>{smsg('Loading...');try{const s=$('#ssym').value,d=$('#sdate').value,e1=$('#se1').value,e2=$('#se2').value,t=$('#stp').value,A=+$('#sa').value,B=+$('#sb').value,R=+$('#srt').value,rv=$('#srev').checked?-1:1;
if(t[0]==='d'&&e2<=e1)throw new Error('Far expiry, expiry thi pachi hovi joiye.');
const inf=await api('/api/info',{sym:s,date:d,gap:$('#sgap').value,e1,e2}),k=inf.exp[0].fk;if(k==null)throw new Error('ATM nathi malto (aa date/expiry ma data nathi).');
const ex=[e1,e2],legs=TPL[t](k,A,B,R).map(([ot,st,q,i])=>({ot,strike:st,qty:q*rv,exp:ex[i]}));
drawS(await api('/api/strategy',{sym:s,date:d,legs:JSON.stringify(legs),lots:$('#slots').value}));smsg('')}catch(e){smsg(e.message)}};
let DG=null;
const DGG=[['sp','SPREAD'],['pv','PREV'],['ch','CHG'],['k2','2nd leg strike'],['pr','Premiums'],['ra','Ratio'],['iv','IV'],['dl','Delta (legs)'],['dr','Delta L1/L2'],['dd','Delta L1-L2'],['th','Theta (legs)'],['nt','Net theta'],['vg','Vega (legs)'],['nv','Net vega']];
let DGO=['k2','sp','pv','ch','pr','ra','iv','dl','dr','dd','th','nt','vg','nv'],DGON=new Set(DGO);const DEF_O=DGO.slice(),DGL=Object.fromEntries(DGG);
try{const o=JSON.parse(localStorage.getItem('ol-dgorder')||'null');if(Array.isArray(o)&&o.length===DGO.length&&DGO.every(x=>o.includes(x)))DGO=o}catch(e){}
function dgList(){$('#dgcols').innerHTML='<div class="m" style="margin-bottom:4px">Upar = table ma dabi bajoo (pehla)</div>'+DGO.map((k,i)=>`<div style="display:flex;align-items:center;gap:6px;margin:3px 0;color:var(--tx)"><label class="cb" style="display:flex;flex-direction:row;gap:6px;flex:1"><input type="checkbox" class="dgc" value="${k}" ${DGON.has(k)?'checked':''}>${DGL[k]}</label><button class="dgm gh" data-g="${k}" data-d="-1" ${i?'':'disabled'} style="padding:2px 8px">&uarr;</button><button class="dgm gh" data-g="${k}" data-d="1" ${i<DGO.length-1?'':'disabled'} style="padding:2px 8px">&darr;</button></div>`).join('')+'<button class="dgm gh" data-reset="1" style="padding:4px 8px;margin-top:4px">Reset order</button>'}
function dgOrd(rev){const all=dgCols(rev);return DGO.flatMap(g=>all.filter(c=>c.g===g))}
$('#dgcols').addEventListener('click',e=>{const b=e.target.closest('.dgm');if(!b)return;if(b.dataset.reset)DGO=DEF_O.slice();else{const i=DGO.indexOf(b.dataset.g),j=i+ +b.dataset.d;if(j<0||j>=DGO.length)return;[DGO[i],DGO[j]]=[DGO[j],DGO[i]]}
try{localStorage.setItem('ol-dgorder',JSON.stringify(DGO))}catch(x){}dgList();drawDg()});
dgList();
function dgCols(rev){const F=(x,p,s)=>x[p[rev?1-s:s]],K=[0,1],PR=[2,3],IV=[8,9],DL=[10,11],TH_=[12,13],VG=[14,15],n1=rev?'F':'N',n2=rev?'N':'F',df=(a,b)=>a==null||b==null?null:b-a,d1=x=>F(x,DL,0),d2=x=>F(x,DL,1),
pair=(g,lab,p,d)=>[{g,l:`${lab} ${n1}`,r:x=>F(x,p,0),d},{g,l:`${lab} ${n2}`,r:x=>F(x,p,1),d}];
return [{g:'sp',l:'SPREAD',r:x=>x[4],d:2},{g:'pv',l:'PREV',r:x=>x[5],d:2},{g:'ch',l:'CHG',r:x=>x[6],d:2,c:1},{g:'k2',l:n2+' K',r:x=>F(x,K,1),d:0},
...pair('pr','PREM',PR,2),{g:'ra',l:'RATIO F/N',r:x=>x[7],d:2,cls:'rt'},...pair('iv','IV',IV,2),...pair('dl','&Delta;',DL,3),
{g:'dr',l:`&Delta; ${n1}/${n2}`,r:x=>{const a=d1(x),b=d2(x);return a==null||!b?null:a/b},d:3},{g:'dd',l:`&Delta; ${n1}&minus;${n2}`,r:x=>df(d2(x),d1(x)),d:3},
...pair('th','&theta;',TH_,2),{g:'nt',l:'NET &theta;',r:x=>df(x[12],x[13]),d:2,c:1},...pair('vg','V',VG,2),{g:'nv',l:'NET V',r:x=>df(x[14],x[15]),d:2,c:1}]}
document.addEventListener('change',e=>{if(e.target.classList.contains('dgc')){e.target.checked?DGON.add(e.target.value):DGON.delete(e.target.value);drawDg()}else if(e.target.id==='dgrev')drawDg()});
$('#dgcsv').onclick=()=>{if(!DG)return dgmsg('Pehla Run Diagonal karo.');const rev=$('#dgrev').checked,cs=dgOrd(rev),dec=t=>t.replace(/&Delta;/g,'Delta').replace(/&theta;/g,'Theta').replace(/&minus;/g,'-');
const L=[['Spot '+DG.spot,'Date '+$('#dgdate').value,'Near '+DG.info[0].exp,'Far '+DG.info[1].exp,'Prev '+(DG.prev||'')],['Block',rev?'FAR K':'NEAR K',...cs.map(c=>dec(c.l))]];
DG.blocks.forEach(b=>b.rows.forEach(x=>L.push([b.ot+' gap '+b.gap,x[rev?1:0],...cs.map(c=>{const v=c.r(x);return v==null?'':Math.round(v*1e4)/1e4})])));
const a=document.createElement('a');a.href=URL.createObjectURL(new Blob(['\ufeff'+L.map(r=>r.join(',')).join('\n')],{type:'text/csv'}));a.download='diagonal_'+$('#dgsym').value+'_'+$('#dgdate').value+'.csv';a.click()};
const CV=n=>getComputedStyle(document.documentElement).getPropertyValue(n).trim(),SK=['--bg','--cd','--bd','--tx','--mu','--sm','--in','--pos','--neg','--hlbg','--rat','--ac'];
function dgSync(){[['dgbg','--bg'],['dgtb','--cd'],['dgtx','--tx'],['dgpos','--pos'],['dgneg','--neg']].forEach(([i,v])=>{const c=CV(v);if(/^#[0-9a-f]{6}$/i.test(c))$('#'+i).value=c})}
function dgThOpts(){$('#dgth').innerHTML=$('#th').innerHTML;$('#dgth').value=$('#th').value}
dgThOpts();$('#dgth').onchange=()=>{$('#th').value=$('#dgth').value;theme();save();dgSync()};
[['dgbg','bgc'],['dgtb','tbc'],['dgtx','txc']].forEach(([i,k])=>$('#'+i).oninput=()=>{$('#'+k).value=$('#'+i).value;APP[k]($('#'+i).value);save()});
$('#dgpos').oninput=()=>root.setProperty('--pos',$('#dgpos').value);$('#dgneg').oninput=()=>root.setProperty('--neg',$('#dgneg').value);
const dgfs=()=>root.setProperty('--dfs',(+$('#dgfs').value||13)+'px');$('#dgfs').oninput=dgfs;
$('#dgsave').onclick=()=>{try{const v={};SK.forEach(k=>v[k]=CV(k));localStorage.setItem('ol-def',JSON.stringify({v,fs:$('#dgfs').value}));save();dgmsg('Default theme save thai gayu.')}catch(e){dgmsg('Save nathi thatu: '+e.message)}};
$('#dgaddth').onclick=()=>{const n=(prompt('Navi theme nu nam:')||'').replace(/[^\w ]/g,'').trim();if(!n)return;const v={};SK.slice(0,9).forEach(k=>v[k]=CV(k));TH[n]=v;
try{const o=JSON.parse(localStorage.getItem('ol-themes')||'{}');o[n]=v;localStorage.setItem('ol-themes',JSON.stringify(o))}catch(e){}
if(![...$('#th').options].some(o=>o.value===n))$('#th').add(new Option(n,n));$('#th').value=n;save();dgThOpts();dgmsg('Theme "'+n+'" add thai.')};
try{const d=JSON.parse(localStorage.getItem('ol-def')||'null');if(d){for(const k in d.v)if(d.v[k])root.setProperty(k,d.v[k]);if(d.fs)$('#dgfs').value=d.fs}}catch(e){}
dgfs();dgSync();
/* ---- Calendar: columns order, pos/neg, add theme, save default, CSV ---- */
const DEF_CO=[0,1,2,3,4,5,15,6,7,8,9,10,11,12,13,14];let CO=DEF_CO.slice(),CON=new Set([0,1,2,3]);
try{const o=JSON.parse(localStorage.getItem('ol-ccols')||'null');if(o&&o.o&&o.o.length===16){CO=o.o;CON=new Set(o.on)}}catch(e){}
const cSave=()=>{try{localStorage.setItem('ol-ccols',JSON.stringify({o:CO,on:[...CON]}))}catch(e){}};
function cList(){$('#ccols').innerHTML='<div class="m" style="margin-bottom:4px">Upar = table ma dabi bajoo (pehla)</div>'+CO.map((k,i)=>`<div style="display:flex;align-items:center;gap:6px;margin:3px 0;color:var(--tx)"><label class="cb" style="display:flex;flex-direction:row;gap:6px;flex:1"><input type="checkbox" class="vc" value="${k}" ${CON.has(k)?'checked':''} autocomplete="off">${NM[k]}</label><button class="cm gh" data-g="${k}" data-d="-1" ${i?'':'disabled'} style="padding:2px 8px">&uarr;</button><button class="cm gh" data-g="${k}" data-d="1" ${i<CO.length-1?'':'disabled'} style="padding:2px 8px">&darr;</button></div>`).join('')+'<button class="cm gh" data-reset="1" style="padding:4px 8px;margin-top:4px">Reset order</button>'}
$('#ccols').addEventListener('click',e=>{const b=e.target.closest('.cm');if(!b)return;if(b.dataset.reset){CO=DEF_CO.slice()}else{const i=CO.indexOf(+b.dataset.g),j=i+ +b.dataset.d;if(j<0||j>=CO.length)return;[CO[i],CO[j]]=[CO[j],CO[i]]}cSave();cList();draw()});
document.addEventListener('change',e=>{if(e.target.classList&&e.target.classList.contains('vc')){const k=+e.target.value;e.target.checked?CON.add(k):CON.delete(k);cSave()}});
cList();
function cSync(){[['cpos','--pos'],['cneg','--neg']].forEach(([i,v])=>{const c=CV(v);if(/^#[0-9a-f]{6}$/i.test(c))$('#'+i).value=c})}
$('#cpos').oninput=()=>root.setProperty('--pos',$('#cpos').value);$('#cneg').oninput=()=>root.setProperty('--neg',$('#cneg').value);
$('#th').addEventListener('change',()=>{cSync();dgSync()});
$('#csave').onclick=()=>{try{const v={};SK.forEach(k=>v[k]=CV(k));localStorage.setItem('ol-def',JSON.stringify({v,fs:$('#fs').value}));save();msg('Default theme save thai gayu.')}catch(e){msg('Save nathi thatu: '+e.message)}};
$('#caddth').onclick=()=>{const n=(prompt('Navi theme nu nam:')||'').replace(/[^\w ]/g,'').trim();if(!n)return;const v={};SK.slice(0,9).concat(['--ac']).forEach(k=>v[k]=CV(k));TH[n]=v;
try{const o=JSON.parse(localStorage.getItem('ol-themes')||'{}');o[n]=v;localStorage.setItem('ol-themes',JSON.stringify(o))}catch(e){}
if(![...$('#th').options].some(o=>o.value===n))$('#th').add(new Option(n,n));$('#th').value=n;save();dgThOpts();msg('Theme "'+n+'" add thai.')};
const csvTxt=t=>{const cols=[...document.querySelectorAll('.vc:checked')].map(x=>+x.value),idx=R.dates.map((d,i)=>SHOW.has(d)?i:-1).filter(i=>i>=0),M=R.meta;
const rows=[['STRIKE',...idx.flatMap(i=>cols.map(c=>R.dates[i]+' '+NM[c]))]];
const mr=(l,fn)=>rows.push([l,...idx.flatMap(i=>cols.map((c,j)=>j?'':fn(M[i])))]);
mr('SPOT',m=>m.spot??'');mr('INDIA VIX',m=>m.vix??'');mr('SYN FUT E',m=>m.e.sf??'');mr('SYN FUT L',m=>m.l.sf??'');mr('STRADDLE E',m=>m.e.straddle??'');mr('STRADDLE L',m=>m.l.straddle??'');
t.forEach(r=>rows.push([r.strike,...idx.flatMap(i=>cols.map(c=>r.cells[i][c]??''))]));return rows.map(r=>r.join(',')).join('\n')};
const dlf=(n,tx)=>{const a=document.createElement('a');a.href=URL.createObjectURL(new Blob(['\ufeff'+tx],{type:'text/csv'}));a.download=n;a.click();setTimeout(()=>URL.revokeObjectURL(a.href),1500)};
$('#dlce').onclick=()=>{if(!R)return msg('Pehla Run Calendar karo.');dlf(`calendar_${$('#sym').value}_${$('#date').value}_CE.csv`,csvTxt(R.CE))};
$('#dlpe').onclick=()=>{if(!R)return msg('Pehla Run Calendar karo.');dlf(`calendar_${$('#sym').value}_${$('#date').value}_PE.csv`,csvTxt(R.PE))};
/* ---- Parity tab (Sensex vs Nifty) ---- */
let PBN=0;const pmsg=t=>$('#pmsg').textContent=t||'';
const PC=[['nex','Sensex K / ratio',2],['sk','Sensex K',0],['nk','Nifty K',0],['mis','Mismatch (Nifty pts)',2],['ps','Sensex px',2],['pn','Nifty px',2],['knn','K x Nifty',2],['spread','Spread',2],['pspread','Prev spread',2],['chg','Change',2],['dr','Delta ratio',3],['ad','A.Delta',3],['brp','Brkg pts',2],['brr','Brkg Rs',0],['ivs','IV S %',2],['ivn','IV N %',2],['ds','Delta S',4],['dn','Delta N',4],['ths','Theta S',3],['thn','Theta N',3],['vs','Vol S (lots)',0],['vn','Vol N (lots)',0]];
let PR=null,PON=new Set(['nex','sk','nk','mis','ps','pn','knn','spread','pspread','chg','dr','ad','brp','brr','ivs','ivn']);
try{const o=JSON.parse(localStorage.getItem('ol-pcols2')||'null');if(o)PON=new Set(o)}catch(e){}
function pcList(){$('#pcols').innerHTML=PC.map(c=>`<label class="cb" style="display:flex;flex-direction:row;gap:6px;margin:3px 0;color:var(--tx)"><input type="checkbox" class="pc" value="${c[0]}" ${PON.has(c[0])?'checked':''} autocomplete="off">${c[1]}</label>`).join('')}
pcList();
document.addEventListener('change',e=>{const c=e.target.classList;if(!c)return;if(c.contains('pc')){e.target.checked?PON.add(e.target.value):PON.delete(e.target.value);try{localStorage.setItem('ol-pcols2',JSON.stringify([...PON]))}catch(x){}pdraw()}else if(e.target.id==='pmet'||e.target.id==='phl')pdraw()});
async function loadPar(){try{const s=await api('/api/symbols',{});const ix=s.filter(x=>x.kind==='idx').map(x=>x.sym);fill($('#pss'),ix,'SENSEX');fill($('#pns'),ix,'NIFTY');const d=await api('/api/dates',{});fill($('#pdate'),d);$('#pdate').value=d[d.length-1];await parExp()}catch(e){pmsg(e.message)}}
async function parExp(){const q=s=>({sym:$(s).value,date:$('#pdate').value});const [a,b]=await Promise.all([api('/api/expiries',q('#pss')),api('/api/expiries',q('#pns'))]);fill($('#pes'),a);fill($('#pen'),b);parGap();if(!a.length)pmsg($('#pss').value+' no data aa date par nathi.');else pmsg('')}
function parGap(){const a=$('#pes').value,b=$('#pen').value;if(!a||!b){$('#pgap').textContent='-';return}const g=Math.round((Date.parse(a)-Date.parse(b))/864e5);$('#pgap').textContent=Math.abs(g)+' days ('+(g>0?'Nifty pehla expire':g<0?'Sensex pehla expire':'same din')+')'}
$('#pss').onchange=()=>parExp().catch(e=>pmsg(e.message));$('#pns').onchange=$('#pss').onchange;$('#pdate').onchange=$('#pss').onchange;$('#pes').onchange=parGap;$('#pen').onchange=parGap;
function pRow(ot,step,n,from,nfrom,nstep,shift,k,pairs){const d=document.createElement('div');d.className='row pbr';d.style.marginBottom='6px';
d.innerHTML=`<label>Type<select class="p-ot"><option>CE</option><option>PE</option></select></label><label>Sensex gap<input class="p-s" type="number" value="${step}" style="width:80px"></label><label>No. of strikes<input class="p-n" type="number" value="${n}" style="width:70px"></label>
<label>Sensex from<input class="p-f" type="number" value="${from}" placeholder="auto" style="width:90px"></label><label>Nifty from<input class="p-nf" type="number" value="${nfrom}" placeholder="auto" style="width:90px"></label><label>Nifty step<input class="p-ns" type="number" value="${nstep}" placeholder="auto" style="width:80px"></label>
<label>Nifty shift<input class="p-sh" type="number" value="${shift}" placeholder="0" style="width:70px"></label><label>K<input class="p-k" type="number" step="0.05" value="${k}" placeholder="global" style="width:70px"></label>
<label>Manual pairs<input class="p-pr" type="text" value="${pairs}" placeholder="74500/23050, 74000/22900" style="width:220px"></label><button class="p-x" style="padding:6px 10px;background:#262b4d">Remove</button>`;
d.querySelector('.p-ot').value=ot;d.querySelector('.p-x').onclick=()=>d.remove();$('#pb').appendChild(d)}
$('#padd').onclick=()=>pRow('CE',500,8,'','','','','','');$('#padd3').onclick=()=>[500,1000,1500].forEach(g=>pRow('CE',g,8,'','','','','',''));
function lRow(ot,k,ss,ns,n,sh,base){const d=document.createElement('div');d.className='row plr';d.style.marginBottom='6px';
d.innerHTML=`<label>Type<select class="l-ot"><option>CE</option><option>PE</option></select></label><label>K<input class="l-k" type="number" step="0.05" value="${k}" style="width:70px"></label><label>Sensex step<input class="l-s" type="number" value="${ss}" style="width:80px"></label><label>Nifty step<input class="l-n" type="number" value="${ns}" style="width:80px"></label><label>Rows upar/niche<input class="l-c" type="number" value="${n}" style="width:80px"></label><label>Nifty shift<input class="l-sh" type="number" value="${sh}" placeholder="0" style="width:70px"></label><label>Base override<input class="l-b" type="text" value="${base}" placeholder="auto (74100/22900)" style="width:150px"></label><button class="l-x" style="padding:6px 10px;background:#262b4d">Remove</button>`;
d.querySelector('.l-ot').value=ot;d.querySelector('.l-x').onclick=()=>d.remove();$('#pl').appendChild(d)}
$('#pladd').onclick=()=>lRow('CE',3.3,500,150,3,'','');$('#pladd2').onclick=()=>{lRow('CE',3.3,500,150,3,'','');lRow('PE',3.3,500,150,3,'','')};
lRow('CE',3.3,500,150,3,'','');lRow('PE',3.3,500,150,3,'','');
$('#prun').onclick=async()=>{pmsg('Loading...');try{const lb=[...document.querySelectorAll('#pl .plr')].map(d=>({ot:d.querySelector('.l-ot').value,k:d.querySelector('.l-k').value,lsstep:d.querySelector('.l-s').value,lnstep:d.querySelector('.l-n').value,lad:d.querySelector('.l-c').value,shift:d.querySelector('.l-sh').value,lbase:d.querySelector('.l-b').value}));
const bl=lb.concat([...document.querySelectorAll('#pb .pbr')].map(d=>({ot:d.querySelector('.p-ot').value,step:d.querySelector('.p-s').value,n:d.querySelector('.p-n').value,from:d.querySelector('.p-f').value,nfrom:d.querySelector('.p-nf').value,nstep:d.querySelector('.p-ns').value,shift:d.querySelector('.p-sh').value,k:d.querySelector('.p-k').value,pairs:d.querySelector('.p-pr').value})));
PR=await api('/api/parity',{ssym:$('#pss').value,nsym:$('#pns').value,date:$('#pdate').value,es:$('#pes').value,en:$('#pen').value,src:$('#psrc').value,k:$('#pk').value,lots:$('#plots').value,lotn:$('#plotn').value,ngap:$('#pngap').value,sgap:$('#psgap').value,rate:$('#prate').value,blocks:JSON.stringify(bl)});pmsg('');pdraw()}catch(e){pmsg(e.message)}};
function pdraw(){if(!PR)return;const I=PR.info,p=x=>x==null?'-':f(x),b=(a,v)=>`<div><span>${a}</span><b>${v}</b></div>`;
$('#pinfo').innerHTML=`<div class="pn"><div class="card"><b>VIEW DATE: ${I.date}</b><div class="kv" style="margin-top:8px">${b('SENSEX SPOT',p(I.spS))}${b('NIFTY SPOT',p(I.spN))}${b('SENSEX SYN FUT',p(I.fS))}${b('NIFTY SYN FUT',p(I.fN))}${b('INDIA VIX',p(I.vix))}${b('PREV DATE',I.prev||'-')}</div></div>
<div class="card"><b>RATIO (Sensex / Nifty)</b><div class="kv" style="margin-top:8px">${b('SYNTHETIC (G13)',f(I.r_syn,4))}${b('SPOT (H13)',I.r_spot?f(I.r_spot,4):'-')}${b('USED ('+(I.src==='spot'?'spot':'synthetic')+')',f(I.ratio,4))}${b('DIFF (syn - spot)',I.r_spot?f(I.r_syn-I.r_spot,4):'-')}</div></div>
<div class="card"><b>EXPIRY ANE LOTS</b><div class="kv" style="margin-top:8px">${b('SENSEX ATM (MROUND 500)',p(I.atm))}${b('EXPIRY GAP',Math.abs(I.gapdays)+' days')}${b('SENSEX EXPIRY',I.es+' ('+I.dayS+'d)')}${b('NIFTY EXPIRY',I.en+' ('+I.dayN+'d)')}${b('K (global)',f(I.k,2))}${b('LOTS S / N',I.lots+' / '+I.lotn+(I.lots_data?' (data '+I.lots_data+' / '+(I.lotn_data||'-')+')':''))}</div></div></div>`;
const cols=PC.filter(c=>PON.has(c[0])),hl=$('#phl').value,m=$('#pmet').value;let S1='',S2='';
PR.blocks.forEach((B,bi0)=>{const R=B.snap,T=`${B.ot} block ${bi0+1}: ${B.label}`;
 if(!R.length)S1+=`<div class="card"><b>${T}</b><p class="m">Aa strikes ma banne ni price nathi.</p></div>`;
 else{let bi=-1,bv=null;if(hl!=='off')R.forEach((r,i)=>{const v=r.spread;if(v!=null&&(bv===null||((hl==='dmax'||hl==='gmax')?v>bv:v<bv))){bv=v;bi=i}});
 S1+=`<div class="card"><b>${T} (Sensex ${B.ot} - K x Nifty ${B.ot}, ${I.date})</b><div class="sc"><table><tr>${cols.map(c=>`<th>${c[1]}</th>`).join('')}</tr>${R.map((r,i)=>`<tr class="${r.base?'basr':''}">${cols.map(c=>{const v=r[c[0]];let cl=['spread','chg'].includes(c[0])?(v==null?'m':v>=0?'g':'r'):(c[0]==='mis'||c[0]==='nex')?'rt':['ds','dn','dr','ad','ivs','ivn','ths','thn'].includes(c[0])?'gk':'';if(c[0]==='spread'&&i===bi)cl+=' hi';return `<td class="${cl}">${f(v,c[2])}</td>`}).join('')}</tr>`).join('')}</table></div></div>`}
 if(PR.dates.length){const K=B.k,val=c=>{const s=c[0],n=c[1];if(m==='s')return s;if(m==='n')return n;if(m==='k')return n==null?null:K*n;return s!=null&&n!=null?s-K*n:null};
 const V=B.series.map(r=>r.cells.map(val));let g=null,gi=-1,gj=-1;const col={};
 if(hl==='dmax'||hl==='dmin')PR.dates.forEach((d,j)=>{let bv=null,bi=-1;V.forEach((row,i)=>{const v=row[j];if(v!=null&&(bv===null||(hl==='dmax'?v>bv:v<bv))){bv=v;bi=i}});col[j]=bi});
 else if(hl!=='off')V.forEach((row,i)=>row.forEach((v,j)=>{if(v!=null&&(g===null||(hl==='gmax'?v>g:v<g))){g=v;gi=i;gj=j}}));
 S2+=`<div class="card"><b>${B.ot} block ${bi0+1}: ${$('#pmet').selectedOptions[0].text} (${B.label})</b><div class="sc"><table><tr><th class="k">SENSEX / NIFTY</th>${PR.dates.map(d=>`<th>${d}</th>`).join('')}</tr>${B.series.map((r,i)=>`<tr class="${r.base?'basr':''}"><td class="k">${r.sk} / ${r.nk}</td>${V[i].map((v,j)=>{let cl=m==='sp'?(v==null?'m':v>=0?'g':'r'):'';if(col[j]===i||(i===gi&&j===gj))cl+=' hi';return `<td class="${cl}">${f(v)}</td>`}).join('')}</tr>`).join('')}</table></div></div>`}});
$('#psnap').innerHTML=S1;$('#pser').innerHTML=S2;if(typeof fit==='function')fit()}
$('#pdl').onclick=()=>{if(!PR)return pmsg('Pehla Run Parity karo.');const out=[];document.querySelectorAll('#psnap table,#pser table').forEach(t=>{out.push((t.closest('.card').querySelector('b')||{}).textContent||'');t.querySelectorAll('tr').forEach(tr=>{const row=[];tr.querySelectorAll('th,td').forEach(c=>{row.push(c.textContent.replace(/[,\s\u2605]+$/,'').replace(/,/g,''))});out.push(row.join(','))});out.push('')});dlf(`parity_${PR.info.date}_${PR.info.es}_${PR.info.en}.csv`,out.join('\n'))};
/* ---- Backtest tab ---- */
let BT=null;const btm=t=>$('#btmsg').textContent=t||'';
const bn=(x,d=0)=>x==null?'-':Number(x).toLocaleString('en-IN',{maximumFractionDigits:d,minimumFractionDigits:d});
const bc=x=>x==null?'':x>0?'color:var(--pos)':x<0?'color:var(--neg)':'';
async function loadBt(){try{const s=await api('/api/symbols',{});fill($('#btsym'),s.filter(x=>x.kind==='idx').map(x=>x.sym),'NIFTY');const d=await api('/api/dates',{});$('#btfrom').value=d[0]>'2019-01-01'?d[0]:'2019-01-01';$('#btto').value=d[d.length-1]}catch(e){btm(e.message)}}
const BTI={sym:'btsym',mode:'btmode',from:'btfrom',to:'btto',split:'btsplit',nl:'btnl',nh:'btnh',fl:'btfl',fh:'btfh',ft:'btft',off:'btoff',ratio:'btrat',pct:'btpct',minvol:'btvol',minoi:'btoi',vixmax:'btvix',tp:'bttp',sl:'btsl',xd:'btxd',mv:'btmv',rate:'btrate',cost:'btcost',lot:'btlot'};
const btRow=(n,m,tn)=>`<tr><td><b>${n}</b></td><td>${m.n}</td><td>${m.win}%</td><td>${m.pf==null?(m.n?'&infin;':'-'):m.pf}</td><td style="${bc(m.tot)}">${bn(m.tot,1)}</td><td style="${bc(m.rs)}">${bn(m.rs)}</td><td style="color:var(--neg)">${bn(m.dd,1)}</td><td style="${bc(m.avg)}">${bn(m.avg,2)}</td><td>${bn(m.aw,1)} / ${bn(m.al,1)}</td><td>${m.hold}</td></tr>`;
function btEq(eq,split){if(!eq.length)return'';const W=900,H=230,P=34,v=eq.map(x=>x[1]),mn=Math.min(0,...v),mx=Math.max(0,...v),sp=(mx-mn)||1,X=i=>P+i*(W-P-8)/Math.max(1,eq.length-1),Y=y=>H-P/2-(y-mn)*(H-P)/sp;
const pts=eq.map((e,i)=>X(i).toFixed(1)+','+Y(e[1]).toFixed(1)).join(' ');const si=eq.findIndex(e=>e[0]>=split);
return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;max-height:260px"><line x1="${P}" x2="${W-8}" y1="${Y(0)}" y2="${Y(0)}" stroke="currentColor" opacity=".35" stroke-dasharray="4"/>${si>0?`<line x1="${X(si)}" x2="${X(si)}" y1="6" y2="${H-8}" stroke="#ffb86b" stroke-dasharray="3"/><text x="${X(si)+4}" y="16" fill="#ffb86b" font-size="11">out-of-sample</text>`:''}<polyline fill="none" stroke="var(--pos)" stroke-width="2" points="${pts}"/><text x="4" y="${Y(mx)+4}" font-size="11" fill="currentColor">${bn(mx)}</text><text x="4" y="${Y(mn)+4}" font-size="11" fill="currentColor">${bn(mn)}</text><text x="${W-90}" y="${H-2}" font-size="11" fill="currentColor">${eq[eq.length-1][0]}</text><text x="${P}" y="${H-2}" font-size="11" fill="currentColor">${eq[0][0]}</text></svg>`}
function btdraw(){const B=BT,f=B.fixed,t=B.today;let h='';
h+=`<div class="card"><div class="sec">Verdict: ${B.npass} / 5 checks pass</div><div style="font-weight:700;margin:4px 0">${B.verdict}</div>${B.checks.map(c=>`<div style="margin:2px 0;color:${c.ok?'var(--pos)':'var(--neg)'}">${c.ok?'&#10004;':'&#10008;'} ${c.txt}</div>`).join('')}<div class="m" style="margin-top:6px">${B.meta.days} divas data, spot andaaz ${B.meta.spot_est}/${B.meta.spot_missing} divas ${B.meta.spot_missing?'(IDX file download karo: python download_bhavcopy.py &lt;from&gt; &lt;to&gt; --exchange IDX, pachi python optlab.py build)':''}</div></div>`;
if(t){h+=`<div class="card"><div class="sec">Aaje no setup (${t.d}, spot ${bn(t.spot,1)}${t.vix?', VIX '+t.vix:''})</div><div style="font-weight:700;color:${t.ok?'var(--pos)':'var(--neg)'}">${t.ok?'ENTRY SIGNAL: badha filters pass':'Entry nathi: filters fail'}</div>${t.near?`<div class="m">Sell ${t.near} (${t.dn}d) / Buy ${t.far} (${t.df}d), strike ${t.k}, debit ${t.debit} pts, IV near ${t.ivn}% far ${t.ivf}%, ratio ${t.ratio}, far IV percentile ${t.pct==null?'-':t.pct}</div>`:''}${t.fails.map(x=>`<div style="color:var(--neg)">&#10008; ${x}</div>`).join('')}</div>`}
h+=`<div class="card"><div class="sec">Summary (points ma; Rs = points x lot size)</div><div class="sc" style="overflow:auto"><table><tr><th>Period</th><th>Trades</th><th>Win %</th><th>PF</th><th>Total pts</th><th>Total Rs</th><th>Max DD pts</th><th>Avg / trade</th><th>Avg win / loss</th><th>Avg days</th></tr>${btRow('Badhu (selected params)',f.all)}${btRow('In-sample',f.ins)}${btRow('Out-of-sample',f.oos)}${B.tuned?btRow(`Tuned in-sample (ratio ${B.tuned.ratio}, TP ${B.tuned.tp*100}%, SL ${B.tuned.sl*100}%)`,B.tuned.ins)+btRow('Tuned out-of-sample',B.tuned.oos):''}</table></div>${B.tuned?'<div class="m">Tuned = in-sample ma best PF vala params, pachi out-of-sample ma test. Aa j sacho test 6e.</div>':''}</div>`;
h+=`<div class="card"><div class="sec">Equity curve (cumulative points, after cost)</div>${btEq(B.equity,B.meta.split)}</div>`;
h+=`<div class="card"><div class="sec">Varsh pramane P&amp;L</div><div class="sc" style="overflow:auto"><table><tr><th>Year</th><th>Trades</th><th>Win %</th><th>PF</th><th>Total pts</th><th>Total Rs</th><th>Max DD pts</th></tr>${B.years.map(y=>`<tr><td><b>${y.y}</b></td><td>${y.n}</td><td>${y.win}%</td><td>${y.pf==null?'&infin;':y.pf}</td><td style="${bc(y.tot)}">${bn(y.tot,1)}</td><td style="${bc(y.rs)}">${bn(y.rs)}</td><td style="color:var(--neg)">${bn(y.dd,1)}</td></tr>`).join('')}</table></div></div>`;
const rc={};B.trades.forEach(x=>rc[x.reason]=(rc[x.reason]||0)+1);
h+=`<div class="card"><div class="sec">Trades (${B.trades.length}) | exit reason: ${Object.entries(rc).map(([k,v])=>k+' '+v).join(', ')} (TP profit, SL loss, TIME near expiry, MOVE spot move, OPEN chhelle band)</div><div class="sc" style="overflow:auto;max-height:440px"><table><tr><th>Entry</th><th>Exit</th><th>Near</th><th>Far</th><th>Strike</th><th>Spot</th><th>Debit</th><th>Exit val</th><th>P&amp;L pts</th><th>% debit</th><th>Days</th><th>Reason</th><th>IV ratio</th><th>IV pct</th></tr>${B.trades.map(x=>`<tr><td>${x.entry}</td><td>${x.exit}</td><td>${x.near}</td><td>${x.far}</td><td>${x.k}</td><td>${bn(x.spot,1)}</td><td>${x.debit}</td><td>${x.exitv}</td><td style="${bc(x.pnl)}">${x.pnl}</td><td style="${bc(x.pct)}">${x.pct}</td><td>${x.days}</td><td>${x.reason}</td><td>${x.ratio==null?'-':x.ratio}</td><td>${x.ivpct==null?'-':x.ivpct}</td></tr>`).join('')}</table></div></div>`;
$('#btout').innerHTML=h}
$('#btrun').onclick=async()=>{btm('Chalu 6e... data ketlo 6e te pramane 1-3 minute lagi shake.');$('#btrun').disabled=true;try{const q={};for(const k in BTI)q[k]=$('#'+BTI[k]).value;q.tune=$('#bttune').checked?1:0;BT=await api('/api/cbt',q);btdraw();btm('')}catch(e){btm(e.message)}$('#btrun').disabled=false};
$('#btdl').onclick=()=>{if(!BT)return btm('Pehla Run Backtest karo.');const H=['entry','exit','near','far','k','spot','debit','exitv','pnl','pct','days','reason','ratio','ivpct'];dlf('calendar_trades.csv',[H.join(',')].concat(BT.trades.map(x=>H.map(k=>x[k]==null?'':x[k]).join(','))).join('\n'))};
/* ---- Monthly Study tab ---- */
let CS=null,CSI=0;const csm=t=>$('#csmsg').textContent=t||'';
async function loadCs(){try{const s=await api('/api/symbols',{});const ix=s.filter(x=>x.kind==='idx').map(x=>x.sym);fill($('#cssym'),ix,ix.includes('BANKNIFTY')?'BANKNIFTY':'NIFTY');const d=await api('/api/dates',{});$('#csfrom').value=d[0]>'2019-01-01'?d[0]:'2019-01-01';$('#csto').value=d[d.length-1]}catch(e){csm(e.message)}}
const CSI_={sym:'cssym',mode:'csmode',n:'csn',g:'csg',offs:'csoffs',holds:'csholds',from:'csfrom',to:'csto',nl:'csnl',nh:'csnh',minvol:'csvol',cost:'cscost',lot:'cslot',rate:'csrate'};
function csdraw(){const C=CS,M=C.meta,G=C.grid;let h='';
const md={dbl:'CE+PE',ce:'CE',pe:'PE'}[M.mode];
h+=`<div class="card"><div class="sec">Aaje no setup (${M.last}): Near = ${M.N}-th monthly, Far = +${M.G} monthly, ${md}</div><div class="sc" style="overflow:auto"><table><tr><th>Offset</th><th>Sell (near)</th><th>Buy (far)</th><th>Strikes</th><th>Debit pts</th><th>Debit Rs (1 lot)</th><th>IV near %</th><th>IV far %</th></tr>${C.today.map(t=>t.err?`<tr><td><b>${t.off}</b></td><td colspan="7" style="color:var(--neg)">${t.err}</td></tr>`:`<tr><td><b>${t.off}</b></td><td>${t.near} (${t.dn}d)</td><td>${t.far} (${t.df}d)</td><td>${t.strikes}</td><td>${t.debit}</td><td>${bn(t.rs)}</td><td>${t.ivn==null?'-':t.ivn}</td><td>${t.ivf==null?'-':t.ivf}</td></tr>`).join('')}</table></div><div class="m">Entry window: near DTE ${M.nl} thi ${M.nh} divas. ${M.spot_missing?'Spot andaaz '+M.spot_est+'/'+M.spot_missing+' divas.':''}</div></div>`;
const ok=G.filter(g=>g.n>=30);const best=ok.length?ok.reduce((a,b)=>b.avgpct>a.avgpct?b:a):null;
h+=`<div class="card"><div class="sec">Result (darek offset x hold). Row par click karo = niche detail</div><div class="sc" style="overflow:auto"><table><tr><th></th><th>Offset</th><th>Hold</th><th>Trades</th><th>Win %</th><th>Avg P&amp;L pts</th><th>Avg % of debit</th><th>Avg Rs</th><th>PF</th><th>Worst pts</th><th>Best pts</th><th>Avg worst-point in hold %</th><th>% trades 30% loss touch</th><th>Skipped</th></tr>${G.map((g,i)=>`<tr data-i="${i}" style="cursor:pointer;${i===CSI?'outline:2px solid #ffb86b':''}"><td>${best===g?'&#9733;':''}</td><td><b>${g.off}</b></td><td>${g.hold}d</td><td>${g.n}</td><td>${g.win}%</td><td style="${bc(g.avg)}">${bn(g.avg,1)}</td><td style="${bc(g.avgpct)}">${bn(g.avgpct,1)}%</td><td style="${bc(g.rs)}">${bn(g.rs)}</td><td>${g.pf==null?(g.n?'&infin;':'-'):g.pf}</td><td style="color:var(--neg)">${bn(g.worst,1)}</td><td style="color:var(--pos)">${bn(g.best,1)}</td><td style="color:var(--neg)">${bn(g.mae,1)}%</td><td>${bn(g.stop30,1)}%</td><td>${g.skipped}</td></tr>`).join('')}</table></div><div class="m">&#9733; = sauthi saro avg % of debit (kam se kam 30 trades vala ma). "Worst-point" = hold daramiyan sauthi kharab P&amp;L (debit na %). "30% loss touch" = ketla trade ma 30% loss ni sparsh thayo (SL 30% rakho to aatla kapai jata).</div></div>`;
const g=G[CSI];if(g&&g.n){const bk=g.buckets,flat=bk.find(b=>b.name==='+-0.5%'),big=bk.filter(b=>b.name==='<= -3%'||b.name==='-3 thi -2%'||b.name==='2 thi 3%'||b.name===">= 3%"),bn2=big.reduce((a,b)=>a+b.n,0),ba=bn2?big.reduce((a,b)=>a+b.avgpct*b.n,0)/bn2:null;
h+=`<div class="card"><div class="sec">Detail: offset ${g.off}, hold ${g.hold} divas</div><div style="margin:4px 0;line-height:1.5">${g.n} entries ma <b>${g.win}%</b> ma profit. Average <b style="${bc(g.avg)}">${bn(g.avg,1)} pts (${bn(g.avgpct,1)}% of debit)</b>, worst ${bn(g.worst,1)} pts, best ${bn(g.best,1)} pts. Hold daramiyan average sauthi kharab point ${bn(g.mae,1)}% ane ${bn(g.stop30,1)}% trade ma 30% loss ni sparsh thayo.${flat?` Spot lagbhag sthir (&plusmn;0.5%) rahyo tyare avg ${flat.avgpct}% (${flat.n} trades).`:''}${ba!=null?` Spot 2%+ halyo tyare avg ${bn(ba,1)}% (${bn2} trades).`:''}</div>
<div class="row" style="align-items:flex-start;gap:18px"><div><b>Spot move (entry thi exit) pramane</b><div class="sc"><table><tr><th>Spot move</th><th>Trades</th><th>Win %</th><th>Avg pts</th><th>Avg % debit</th></tr>${bk.map(b=>`<tr><td>${b.name}</td><td>${b.n}</td><td>${b.win}%</td><td style="${bc(b.avg)}">${bn(b.avg,1)}</td><td style="${bc(b.avgpct)}">${bn(b.avgpct,1)}%</td></tr>`).join('')}</table></div></div>
<div><b>Varsh pramane</b><div class="sc"><table><tr><th>Year</th><th>Trades</th><th>Win %</th><th>Avg pts</th><th>Total pts</th></tr>${g.years.map(y=>`<tr><td><b>${y.y}</b></td><td>${y.n}</td><td>${y.win}%</td><td style="${bc(y.avg)}">${bn(y.avg,1)}</td><td style="${bc(y.tot)}">${bn(y.tot,1)}</td></tr>`).join('')}</table></div></div></div>
<div style="margin-top:10px"><b>Trades (${g.trades.length})</b></div><div class="sc" style="overflow:auto;max-height:420px"><table><tr><th>Entry</th><th>Exit</th><th>Strike</th><th>Spot</th><th>Near DTE</th><th>Debit</th><th>Exit val</th><th>P&amp;L pts</th><th>% debit</th><th>Spot move %</th><th>Worst %</th><th>Best %</th></tr>${g.trades.map(x=>`<tr><td>${x[0]}</td><td>${x[1]}</td><td>${x[2]}</td><td>${bn(x[3],1)}</td><td>${x[11]}</td><td>${x[4]}</td><td>${x[5]}</td><td style="${bc(x[6])}">${x[6]}</td><td style="${bc(x[7])}">${x[7]}</td><td>${x[8]}</td><td style="color:var(--neg)">${x[9]}</td><td style="color:var(--pos)">${x[10]}</td></tr>`).join('')}</table></div></div>`}
else h+=`<div class="card">Selected row ma trade nathi.</div>`;
$('#csout').innerHTML=h}
$('#csout').addEventListener('click',e=>{const r=e.target.closest('tr[data-i]');if(r&&CS){CSI=+r.dataset.i;csdraw()}});
$('#csrun').onclick=async()=>{csm('Chalu 6e... 1-3 minute lagi shake.');$('#csrun').disabled=true;try{const q={};for(const k in CSI_)q[k]=$('#'+CSI_[k]).value;CS=await api('/api/cst',q);CSI=0;csdraw();csm('')}catch(e){csm(e.message)}$('#csrun').disabled=false};
$('#csdl').onclick=()=>{if(!CS)return csm('Pehla Run Study karo.');const g=CS.grid[CSI];if(!g)return;const H=['entry','exit','strike','spot','debit','exit_val','pnl','pct_debit','spot_move_pct','worst_pct','best_pct','near_dte'],ix=[0,1,2,3,4,5,6,7,8,9,10,11];dlf(`monthly_study_off${g.off}_hold${g.hold}.csv`,[H.join(',')].concat(g.trades.map(x=>ix.map(k=>x[k]).join(','))).join('\n'))};

/* ---- Date picker (monthly calendar) + Market context (events, news) ---- */
const esc=x=>String(x==null?'':x).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const nfx=(v,d=2)=>v==null?'-':(+v).toLocaleString('en-IN',{minimumFractionDigits:d,maximumFractionDigits:d});
const DPS=[{sel:'#date',sym:['#sym']},{sel:'#sdate',sym:['#ssym']},{sel:'#dgdate',sym:['#dgsym']},{sel:'#pdate',sym:['#pss','#pns']}];
const CATC={RBI:'#4f9fe0',FED:'#c3a6ff',BUDGET:'#ffb300',ELECTION:'#3ddc97',GEO:'#ff6b6b',US:'#ff8fb1',POLICY:'#7cc7ff',COVID:'#ff9f40',OTHER:'#9aa3c7'};
const NEWSQ='Nifty OR Sensex OR RBI OR Fed OR crude OR "stock market"';
let EVL=null;
async function evLoad(force){if(EVL&&!force)return EVL;try{EVL=(await api('/api/events',{})).events}catch(e){EVL=[]}return EVL}
const dpOpts=d=>[...d.s.options].map(o=>o.value);
function dpSyncOne(d){const o=dpOpts(d);if(!o.length)return;if(d.inp.min!==o[0])d.inp.min=o[0];if(d.inp.max!==o[o.length-1])d.inp.max=o[o.length-1];const v=d.s.value;if(v&&d.inp.value!==v)d.inp.value=v}
function dpPick(d,val){if(!val)return;const o=dpOpts(d);if(!o.length)return;const t=o.includes(val)?val:(o.filter(x=>x<=val).pop()||o.find(x=>x>=val));d.note.textContent=t!==val?`(${val} holiday/weekend: ${t} vaparyu)`:'';d.s.value=t;d.inp.value=t;d.s.dispatchEvent(new Event('change',{bubbles:true}))}
function dpStep(d,k){const o=dpOpts(d),i=o.indexOf(d.s.value),j=i+k;if(i<0||j<0||j>=o.length)return;d.note.textContent='';d.s.value=o[j];d.s.dispatchEvent(new Event('change',{bubbles:true}))}
function ctxMake(d){const b=document.createElement('div');b.className='card ctx';const cats=Object.keys(CATC).map(c=>`<option>${c}</option>`).join('');
b.innerHTML=`<details open><summary><b>Market context</b> <span class="ctxh m"></span></summary><div style="margin-top:8px"><div class="ctxm"></div><div class="ctxf"></div><div class="ctxe"></div><div class="ctxu m"></div>
<div class="row" style="margin-top:8px"><label>Event par jao<select class="ctxj" style="min-width:300px"></select></label><label>Category<select class="ctxc"><option value="">All</option>${cats}</select></label>
<button type="button" class="ctxnews gh">Load news</button><a class="ctxg" target="_blank" rel="noopener noreferrer">Google News ma khol</a><button type="button" class="ctxadd gh">+ Add event</button></div>
<div class="ctxaf" style="display:none;margin-top:6px"><div class="row"><label>Date<input type="date" class="aed"></label><label>Category<select class="aec">${cats}</select></label><label>Impact<select class="aei"><option value="H">High</option><option value="M" selected>Medium</option><option value="L">Low</option></select></label><label>Title<input type="text" class="aet" style="min-width:300px" placeholder="Event / news nu nam"></label><button type="button" class="aes">Save event</button></div></div>
<div class="row" style="margin-top:6px"><label>News search<input type="text" class="ctxq" style="min-width:360px"></label></div><div class="ctxn"></div></div></details>`;
const q=b.querySelector('.ctxq');try{q.value=localStorage.getItem('ol-newsq')||NEWSQ}catch(e){q.value=NEWSQ}
b.querySelector('.ctxj').onchange=e=>{if(e.target.value)dpPick(d,e.target.value)};
b.querySelector('.ctxc').onchange=()=>ctxJump(d);
b.querySelector('.ctxadd').onclick=()=>{const f=b.querySelector('.ctxaf');f.style.display=f.style.display==='none'?'':'none';b.querySelector('.aed').value=d.s.value};
b.querySelector('.aes').onclick=async()=>{const t=b.querySelector('.aet').value.trim();if(!t)return;try{await api('/api/event_add',{date:b.querySelector('.aed').value,cat:b.querySelector('.aec').value,impact:b.querySelector('.aei').value,title:t});b.querySelector('.aet').value='';await evLoad(true);DPS.forEach(x=>x.box&&ctxRefresh(x,true))}catch(e){alert(e.message)}};
b.querySelector('.ctxnews').onclick=()=>ctxNews(d);
b.addEventListener('click',async e=>{const c=e.target.closest('.chip');if(c&&c.dataset.d)return dpPick(d,c.dataset.d);const x=e.target.closest('.evdel');if(x){try{await api('/api/event_del',{id:x.dataset.id});await evLoad(true);DPS.forEach(y=>y.box&&ctxRefresh(y,true))}catch(er){alert(er.message)}}});
return b}
function ctxJump(d){const b=d.box,cat=b.querySelector('.ctxc').value,mn=d.inp.min||'',mx=d.inp.max||'9999';const L=(EVL||[]).filter(e=>(!cat||e.cat===cat)&&e.react>=mn&&e.react<=mx).sort((a,c)=>c.react<a.react?-1:1);
b.querySelector('.ctxj').innerHTML='<option value="">- event select karo -</option>'+L.map(e=>`<option value="${esc(e.react)}">${esc(e.react)}  ${esc(e.cat)}  ${esc(e.title.slice(0,70))}</option>`).join('')}
function ctxRender(d,c){const b=d.box,col=v=>v==null?'':v>=0?'var(--pos)':'var(--neg)',sg=v=>v==null?'':(v>=0?'+':'')+nfx(v)+'%';
b.querySelector('.ctxh').textContent=`${c.date} (${c.dow})`;
b.querySelector('.ctxm').innerHTML=c.moves.map(m=>`<span class="ctxk"><span class="m">${esc(m.sym)}</span> <b>${nfx(m.close)}</b> <span style="color:${col(m.chg)}">${sg(m.chg)}</span></span>`).join('')+(c.vix?`<span class="ctxk"><span class="m">INDIA VIX</span> <b>${nfx(c.vix.v)}</b> <span style="color:${col(c.vix.chg==null?null:-c.vix.chg)}">${sg(c.vix.chg)}</span></span>`:'')+`<span class="ctxk m">prev: ${esc(c.prev||'-')}</span>`;
b.querySelector('.ctxf').innerHTML=c.flags.map(x=>`<span class="bdg">${esc(x)}</span>`).join('');
const one=e=>`<div class="evi"><span class="ecat" style="background:${CATC[e.cat]||CATC.OTHER}">${esc(e.cat)}</span><b>${esc(e.title)}</b> <span class="m">${e.impact==='H'?'High impact':e.impact==='L'?'Low impact':''}${e.date!==c.date?' (event date '+esc(e.date)+')':''}</span>${e.src==='custom'?`<button type="button" class="evdel" data-id="${esc(e.id)}" title="Delete">&times;</button>`:''}</div>`;
b.querySelector('.ctxe').innerHTML=c.events.length?c.events.map(one).join(''):'<div class="m">Aa divase koi event list ma nathi. Upar "+ Add event" thi tame potano event umeri shako.</div>';
const chip=e=>`<span class="chip" data-d="${esc(e.react)}" title="${esc(e.title)}"><span style="color:${CATC[e.cat]||CATC.OTHER}">&#9679;</span> ${esc(e.react.slice(5))} ${esc(e.cat)}: ${esc(e.title.slice(0,38))}</span>`;
b.querySelector('.ctxu').innerHTML=(c.recent.length?'Pachhla 7 divas: '+c.recent.map(chip).join(''):'')+(c.upcoming.length?'<div style="margin-top:4px">Aavta 7 divas: '+c.upcoming.map(chip).join('')+'</div>':'');
const q=b.querySelector('.ctxq').value||NEWSQ,[y,m,dd]=c.date.split('-');b.querySelector('.ctxg').href=`https://www.google.com/search?q=${encodeURIComponent(q)}&tbm=nws&tbs=cdr:1,cd_min:${+m}/${+dd}/${y},cd_max:${+m}/${+dd}/${y}`;
ctxJump(d)}
async function ctxNews(d){const b=d.box,q=b.querySelector('.ctxq').value.trim(),out=b.querySelector('.ctxn');try{localStorage.setItem('ol-newsq',q)}catch(e){}out.innerHTML='<span class="m">News load thay 6e...</span>';
try{const r=await api('/api/news',{date:d.s.value,q});out.innerHTML=(r.err?`<div class="m">${esc(r.err)}</div>`:'')+(r.items.length&&!r.exact?'<div class="m">Aa j divas na news na malya, aaju-baju na divas na dekhay 6e.</div>':'')+r.items.map(x=>`<div><a href="${esc(x.link)}" target="_blank" rel="noopener noreferrer">${esc(x.title)}</a> <span class="m">${esc(x.source)} ${esc(x.time)}</span></div>`).join('')}catch(e){out.innerHTML=`<div class="m">${esc(e.message)}</div>`}}
let ctxT={};
function ctxRefresh(d,now){d.last=d.s.value;clearTimeout(ctxT[d.sel]);ctxT[d.sel]=setTimeout(async()=>{const v=d.s.value;if(!v)return;const sy=[...new Set(d.sym.map(x=>($(x)||{}).value).filter(Boolean))].join(',')||'NIFTY';
try{await evLoad();const c=await api('/api/context',{date:v,syms:sy});if(v===d.s.value)ctxRender(d,c)}catch(e){d.box.querySelector('.ctxh').textContent='';d.box.querySelector('.ctxm').innerHTML=`<span class="m">${esc(e.message)}</span>`}},now?0:250)}
DPS.forEach(d=>{d.s=$(d.sel);if(!d.s)return;const lab=d.s.closest('label'),w=document.createElement('div');w.className='dpw';
w.innerHTML='<button type="button" class="dpb gh" data-s="-1" title="Pachhlo trading divas">&lsaquo;</button><input type="date" class="dpi"><button type="button" class="dpb gh" data-s="1" title="Aavto trading divas">&rsaquo;</button><span class="dpn"></span>';
d.s.style.display='none';lab.appendChild(w);d.inp=w.querySelector('.dpi');d.note=w.querySelector('.dpn');
d.inp.onchange=()=>dpPick(d,d.inp.value);w.addEventListener('click',e=>{const bt=e.target.closest('.dpb');if(bt)dpStep(d,+bt.dataset.s)});
d.box=ctxMake(d);const card=lab.closest('.card');card.parentNode.insertBefore(d.box,card.nextSibling);
d.s.addEventListener('change',()=>{dpSyncOne(d);ctxRefresh(d)})});
setInterval(()=>DPS.forEach(d=>{if(!d.s||!d.inp)return;dpSyncOne(d);if(d.s.value&&d.s.value!==d.last)ctxRefresh(d)}),400);
evLoad();
cSync();
loadSyms();

</script></body></html>"""

def _nn(o):
    """NaN/Infinity JSON ma invalid che (browser 'Unexpected token' / khali) -> None"""
    if isinstance(o, float): return o if math.isfinite(o) else None
    if isinstance(o, dict): return {k: _nn(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)): return [_nn(v) for v in o]
    return o

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def _auth_ok(self):
        h = self.headers.get("Authorization", "")
        if not h.startswith("Basic "):
            return False
        try:
            u, p = base64.b64decode(h[6:]).decode().split(":", 1)
            return u == AUTH_USER and p == AUTH_PASS
        except Exception:
            return False

    def do_GET(self):
        if not self._auth_ok():
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="Options Lab"')
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        u = urlparse(self.path)
        if u.path == "/":
            return self._send(200, HTML.encode(), "text/html; charset=utf-8")
        fn = ROUTES.get(u.path)
        if not fn:
            return self._send(404, b'{"error":"not found"}', "application/json")
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        con = None
        try:
            sc = SCOPE[u.path](q)
            con = db(*sc) if sc else None
            body = json.dumps(_nn(fn(q, con)), allow_nan=False)
            code = 200
        except MemoryError:
            body, code = json.dumps({"error": "Server ni memory puri thai gai. Nano From-To range lo ke offsets/hold days ochha karo."}), 400
        except Exception as e:
            msg = str(e)
            if "memory" in msg.lower() or "OutOfMemory" in type(e).__name__:
                msg = "Memory puri thai gai (" + msg[:120] + "). Nano From-To range lo."
            body, code = json.dumps({"error": msg}), 400
        finally:
            try:
                if con: con.close()
            except Exception: pass
            import gc; gc.collect()
        self._send(code, body.encode(), "application/json")

    def _send(self, code, b, ct):
        self.send_response(code)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        try: self.wfile.write(b)
        except (BrokenPipeError, ConnectionResetError): pass
def serve(port):
    port = int(os.environ.get("PORT", port))
    print(f"Dashboard: http://0.0.0.0:{port}")
    ThreadingHTTPServer(("0.0.0.0", port), H).serve_forever()
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["build", "serve", "update", "compact"])
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--from", dest="start", help="update mate start date YYYY-MM-DD (default: last date thi, pehli vaar 2019-01-01)")
    ap.add_argument("--keep-csv", action="store_true", help="CSV delete na karo")
    ap.add_argument("--bse", action="store_true", help="update ma BSE pan")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--retry-missing", action="store_true")
    ap.add_argument("--keep-days", type=int, default=60, help="compact: chhella N divas daily file rakho")
    ap.add_argument("--level", type=int, default=19, help="compact: ZSTD level (1-22)")
    ap.add_argument("--otm", type=float, default=0, help="compact: fakat strike +-N%% (underlying thi) rakho (lossy)")
    ap.add_argument("--no-stocks", action="store_true", help="compact: stock options kadhi nakho (lossy)")
    ap.add_argument("--minpx", type=float, default=0, help="compact: close < N vala rows kadhi nakho (lossy)")
    ap.add_argument("--delete", action="store_true", help="compact: verify pachhi juni daily files delete")
    a = ap.parse_args()
    if a.cmd == "build": build(a.keep_csv)
    elif a.cmd == "compact": compact(a.keep_days, a.level, a.otm, a.no_stocks, a.minpx, a.delete)
    elif a.cmd == "update": update(a.start, a.keep_csv, a.workers, a.bse, a.retry_missing)
    else: serve(a.port)
