#!/usr/bin/env python3
"""
NSE / BSE F&O bhavcopy + NSE index (VIX) downloader. Date range, parallel, resume-safe.

  python download_bhavcopy.py 01-01-19 27-10-26 --exchange NSE
  python download_bhavcopy.py 01-10-26 27-10-26            (NSE + BSE + IDX)

Juna (Jul-2024 pehla) ane nava (UDiFF) bane NSE format automatic try thay 6e.
Saturday/Sunday skip. Je divas ni file nathi (holiday) te yaad rakhe 6e (bhavcopy/_missing_*.txt),
etle fari chalavo to te fari request nathi thati. Fari check karva: --retry-missing
Normally tame 'python optlab.py update' vapro, je aa script vape 6e ane Parquet ma convert kare 6e.
"""
import argparse, io, os, sys, time, zipfile
from datetime import datetime, timedelta, date
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from concurrent.futures import ThreadPoolExecutor

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9",
}
MON = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
CUTOVER = date(2024, 7, 8)     # NSE F&O UDiFF format shuru

def pdate(s):
    for f in ("%d-%m-%y", "%d-%m-%Y", "%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, f).date()
        except ValueError:
            pass
    raise SystemExit(f"Date samjayi nahi: {s} (DD-MM-YY vapro)")

def get(url, tries=3):
    """bytes, ke None (file nathi: 403/404). Bija error par retry, pachi IOError."""
    for i in range(tries):
        try:
            with urlopen(Request(url, headers=HEADERS), timeout=45) as r:
                return r.read()
        except HTTPError as e:
            if e.code in (403, 404):
                return None
            time.sleep(2 * (i + 1))
        except (URLError, TimeoutError, ConnectionError):
            time.sleep(2 * (i + 1))
    raise IOError("network/server error: " + url)

def _zip(raw):
    if not raw:
        return None
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            return z.read(z.namelist()[0])
    except zipfile.BadZipFile:
        return None

def _html(raw):
    return not raw or raw[:20].lstrip().lower().startswith((b"<!doctype", b"<html"))

def nse(d):
    ymd, m = d.strftime("%Y%m%d"), MON[d.month - 1]
    new = [f"https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{ymd}_F_0000.csv.zip"]
    old = [f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/{d:%Y}/{m}/fo{d:%d}{m}{d:%Y}bhav.csv.zip",
           f"https://archives.nseindia.com/content/historical/DERIVATIVES/{d:%Y}/{m}/fo{d:%d}{m}{d:%Y}bhav.csv.zip"]
    for url in (new + old) if d >= CUTOVER else (old + new):
        data = _zip(get(url))
        if data and not _html(data):
            return data
    return None

def bse(d):
    ymd = d.strftime("%Y%m%d")
    raw = get(f"https://www.bseindia.com/download/Bhavcopy/Derivative/BhavCopy_BSE_FO_0_0_0_{ymd}_F_0000.CSV")
    return None if _html(raw) else raw

def idx(d):
    """NSE index close file (Nifty, Bank Nifty, India VIX ...)"""
    raw = get(f"https://nsearchives.nseindia.com/content/indices/ind_close_all_{d:%d%m%Y}.csv")
    return None if _html(raw) else raw

FN = {"NSE": nse, "BSE": bse, "IDX": idx}
PQBASE = {"NSE": os.path.join("data", "parquet", "NSE_"), "BSE": os.path.join("data", "parquet", "BSE_"),
          "IDX": os.path.join("data", "idx", "IDX_")}

def run(s, e, names, out="bhavcopy", workers=4, retry_missing=False):
    today = date.today(); recent = today - timedelta(days=5)
    for name in names:
        folder = os.path.join(out, name); os.makedirs(folder, exist_ok=True)
        mf = os.path.join(out, f"_missing_{name}.txt")
        missing = set() if retry_missing or not os.path.exists(mf) else set(open(mf).read().split())
        days, d = [], s
        while d <= e:
            if d.weekday() < 5:
                days.append(d)
            d += timedelta(days=1)
        csv = lambda d: os.path.join(folder, f"{name}_FO_{d:%Y%m%d}.csv")
        todo = [d for d in days if not os.path.exists(csv(d)) and not os.path.exists(f"{PQBASE[name]}{d:%Y%m%d}.parquet")
                and d.isoformat() not in missing]

        def job(d):
            try:
                data = FN[name](d)
            except Exception as ex:
                return d, "error", str(ex)
            if data:
                open(csv(d), "wb").write(data)
                return d, "saved", None
            return d, "missing", None

        print(f"\n== {name}: {len(days)} weekdays, {len(todo)} download baki ==")
        saved = miss = err = 0; new_missing = []
        with ThreadPoolExecutor(workers) as ex:
            for i, (d, st, msg) in enumerate(ex.map(job, todo), 1):
                if st == "saved": saved += 1
                elif st == "missing":
                    miss += 1
                    if d < recent: new_missing.append(d.isoformat())
                else:
                    err += 1; print(f"{d}: ERROR {msg}")
                if i % 20 == 0 or i == len(todo):
                    print(f"  {name} {d} ({i}/{len(todo)})  saved {saved}, holiday/nathi {miss}, error {err}")
                time.sleep(0.1)
        if new_missing:
            with open(mf, "a") as fh:
                fh.write("\n".join(new_missing) + "\n")
        if todo and miss > 0.7 * len(todo) and len(todo) > 15:
            print(f"  WARNING: {name} ma ghani files 'nathi' aavi. NSE/BSE block kare to etle hoy. Thodi vaar pachi --workers 2 sathe fari karo.")

def merge(folder, out_file):
    files = sorted(f for f in os.listdir(folder) if f.lower().endswith(".csv"))
    if not files:
        return
    with open(out_file, "wb") as o:
        for i, f in enumerate(files):
            with open(os.path.join(folder, f), "rb") as fh:
                lines = fh.read().splitlines(keepends=True)
            o.write(b"".join(lines if i == 0 else lines[1:]))
    print(f"  merged -> {out_file}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("start"); ap.add_argument("end")
    ap.add_argument("--exchange", choices=["NSE", "BSE", "IDX", "BOTH"], default="BOTH")
    ap.add_argument("--out", default="bhavcopy")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--retry-missing", action="store_true")
    ap.add_argument("--merge", action="store_true", help="badhi CSV no ek merged CSV (nanu range mate j)")
    a = ap.parse_args()
    s, e = pdate(a.start), pdate(a.end)
    if e < s:
        raise SystemExit("End date start thi pehla 6e.")
    names = ["NSE", "BSE", "IDX"] if a.exchange == "BOTH" else [a.exchange]
    run(s, e, names, a.out, a.workers, a.retry_missing)
    if a.merge:
        for n in names:
            merge(os.path.join(a.out, n), os.path.join(a.out, f"merged_{n}.csv"))

if __name__ == "__main__":
    main()
