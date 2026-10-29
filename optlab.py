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

RAW, PQ = "bhavcopy", os.path.join("data", "parquet")

# ---------------------------------------------------------------- BUILD
IPQ = os.path.join("data", "idx")
NEW_SQL = """SELECT '{x}' AS exch, CAST(TradDt AS DATE) AS date, TckrSymb AS sym,
    CASE FinInstrmTp WHEN 'IDO' THEN 'idx' ELSE 'stk' END AS kind, CAST(XpryDt AS DATE) AS exp,
    CAST(StrkPric AS DOUBLE) AS strike, OptnTp AS ot,
    COALESCE(NULLIF(TRY_CAST(ClsPric AS DOUBLE),0), TRY_CAST(SttlmPric AS DOUBLE), 0) AS close,
    TRY_CAST(UndrlygPric AS DOUBLE) AS und, COALESCE(TRY_CAST(OpnIntrst AS BIGINT),0) AS oi,
    COALESCE(TRY_CAST(TtlTradgVol AS BIGINT),0) AS vol, TRY_CAST(NewBrdLotQty AS INTEGER) AS lot
    FROM read_csv(?, all_varchar=true, header=true, ignore_errors=true)
    WHERE FinInstrmTp IN ('IDO','STO') AND OptnTp IN ('CE','PE')"""
OLD_SQL = """SELECT '{x}' AS exch, CAST(strptime(TIMESTAMP,'%d-%b-%Y') AS DATE) AS date, SYMBOL AS sym,
    CASE INSTRUMENT WHEN 'OPTIDX' THEN 'idx' ELSE 'stk' END AS kind,
    CAST(strptime(EXPIRY_DT,'%d-%b-%Y') AS DATE) AS exp, CAST(STRIKE_PR AS DOUBLE) AS strike, OPTION_TYP AS ot,
    COALESCE(NULLIF(TRY_CAST(CLOSE AS DOUBLE),0), TRY_CAST(SETTLE_PR AS DOUBLE), 0) AS close,
    CAST(NULL AS DOUBLE) AS und, COALESCE(TRY_CAST(OPEN_INT AS BIGINT),0) AS oi,
    COALESCE(TRY_CAST(CONTRACTS AS BIGINT),0) AS vol, CAST(NULL AS INTEGER) AS lot
    FROM read_csv(?, all_varchar=true, header=true, ignore_errors=true)
    WHERE INSTRUMENT IN ('OPTIDX','OPTSTK') AND OPTION_TYP IN ('CE','PE')"""

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
    ds = [_fd(f) for f in glob.glob(os.path.join(PQ, "NSE_*.parquet"))]
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

# ---------------------------------------------------------------- QUERY
def _fd(f):
    t = re.search(r"_(\d{8})\.parquet$", f).group(1)
    return f"{t[:4]}-{t[4:6]}-{t[6:]}"

def _lst(fs):
    return "[" + ",".join("'" + f.replace("\\", "/").replace("'", "''") + "'" for f in fs) + "]"

def db(a=None, b=None, last=0):
    """Fakat jaruri date-range ni Parquet files vanche (2019 thi aaj sudhi na data ma pan fast)."""
    fs = glob.glob(os.path.join(PQ, "*.parquet"))
    if a: fs = [f for f in fs if _fd(f) >= a]
    if b: fs = [f for f in fs if _fd(f) <= b]
    if last:
        keep = sorted({_fd(f) for f in fs})[-last:]
        fs = [f for f in fs if _fd(f) in keep]
    if not fs:
        raise RuntimeError("Aa date range ma Parquet data nathi. Pehla 'python optlab.py update' chalavo.")
    con = duckdb.connect()
    con.execute(f"CREATE VIEW opt AS SELECT * FROM read_parquet({_lst(fs)}, union_by_name=true)")
    ix = [f for f in glob.glob(os.path.join(IPQ, "*.parquet")) if (not a or _fd(f) >= a) and (not b or _fd(f) <= b)]
    if ix:
        con.execute(f"CREATE VIEW idx AS SELECT * FROM read_parquet({_lst(ix)}, union_by_name=true)")
    else:
        con.execute("CREATE VIEW idx AS SELECT NULL::DATE AS date, NULL::VARCHAR AS name, NULL::DOUBLE AS close WHERE false")
    return con

def api_symbols(q, con):
    r = con.execute("SELECT sym, any_value(kind), any_value(exch), any_value(lot) FROM opt GROUP BY sym ORDER BY 2, 1").fetchall()
    return [{"sym": a, "kind": b, "exch": c, "lot": d} for a, b, c, d in r]

def api_dates(q, con):
    return sorted({_fd(f) for f in glob.glob(os.path.join(PQ, "*.parquet"))})

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
    lot = con.execute("SELECT max(lot) FROM opt WHERE sym=? AND date=?", [sym, d0]).fetchone()[0]
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
    ds = [_fd(f) for f in glob.glob(os.path.join(PQ, "*.parquet")) if _fd(f) < d]
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

ROUTES = {"/api/info": api_info, "/api/symbols": api_symbols, "/api/dates": api_dates, "/api/expiries": api_expiries, "/api/calendar": api_calendar, "/api/strategy": api_strategy, "/api/diag": api_diag}
SCOPE = {"/api/symbols": lambda q: (None, None, 3), "/api/dates": lambda q: None,
         "/api/expiries": lambda q: (q["date"], q["date"], 0), "/api/calendar": lambda q: (q["date"], q["e1"], 0),
         "/api/info": lambda q: (q["date"], q["date"], 0),
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
td.hi{background:var(--hlbg)!important;color:var(--hltx)!important;font-weight:700}th.best{color:var(--hlbg)}
.pn{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:10px;margin-bottom:14px}.pn .card{margin:0}
.kv{display:grid;grid-template-columns:1fr 1fr;gap:6px}.kv div{background:var(--in);border:1px solid var(--bd);border-radius:6px;padding:6px}.kv span{display:block;font-size:10px;color:var(--mu)}
#msg{margin:8px 0;color:#ffb86b}#tab-dia table{font-size:var(--dfs,13px)}.sec{margin:0 0 8px;color:var(--mu);font-size:11px;text-transform:uppercase;letter-spacing:.06em}.gh{background:#262b4d}</style></head><body>
<div class="row" style="margin-bottom:10px"><button id="t1">Calendar</button><button id="t2" style="background:#262b4d">Strategy</button><button id="t3" style="background:#262b4d">Diagonal</button></div>
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
<script>
const $=s=>document.querySelector(s);
async function api(p,q){const r=await fetch(p+'?'+new URLSearchParams(q));const j=await r.json();if(j.error)throw new Error(j.error);return j}
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
function tab(n){['cal','str','dia'].forEach((x,i)=>$('#tab-'+x).style.display=i===n?'':'none');if(n===1&&!$('#ssym').options.length)loadS();if(n===2&&!$('#dgsym').options.length)loadDg()}
$('#t1').onclick=()=>tab(0);$('#t2').onclick=()=>tab(1);$('#t3').onclick=()=>tab(2);
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
cSync();
loadSyms();

</script></body></html>"""

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/":
            return self._send(200, HTML.encode(), "text/html; charset=utf-8")
        fn = ROUTES.get(u.path)
        if not fn:
            return self._send(404, b'{"error":"not found"}', "application/json")
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        try:
            sc = SCOPE[u.path](q)
            con = db(*sc) if sc else None
            body = json.dumps(fn(q, con))
            code = 200
        except Exception as e:
            body, code = json.dumps({"error": str(e)}), 400
        self._send(code, body.encode(), "application/json")
    def _send(self, code, b, ct):
        self.send_response(code); self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)

def serve(port):
    print(f"Dashboard: http://localhost:{port}   (band karva Ctrl+C)")
    ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["build", "serve", "update"])
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--from", dest="start", help="update mate start date YYYY-MM-DD (default: last date thi, pehli vaar 2019-01-01)")
    ap.add_argument("--keep-csv", action="store_true", help="CSV delete na karo")
    ap.add_argument("--bse", action="store_true", help="update ma BSE pan")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--retry-missing", action="store_true")
    a = ap.parse_args()
    if a.cmd == "build": build(a.keep_csv)
    elif a.cmd == "update": update(a.start, a.keep_csv, a.workers, a.bse, a.retry_missing)
    else: serve(a.port)
