
#!/usr/bin/env python3
"""Download historical BingX swap 1m OHLCV, without fabricating bid/ask spreads.
No exchange keys, order endpoints, database, or trading operations.
Output is deliberately NOT accepted by the strict phase-6 backtester until
actual archived historical spread observations are joined and verified.
"""
import argparse,csv,hashlib,json,re,time,math,urllib.parse,urllib.request
from datetime import datetime,timezone,timedelta
from pathlib import Path

BASE='https://open-api.bingx.com/openApi/swap/v3/quote/klines'
FIELDS=('time','symbol','open','high','low','close','volume')


def parse_utc(value):
    dt=datetime.fromisoformat(value.replace('Z','+00:00'))
    if dt.tzinfo is None:raise ValueError('Time must contain UTC offset or Z')
    dt=dt.astimezone(timezone.utc)
    if dt.second or dt.microsecond:raise ValueError('Use whole-minute times')
    return int(dt.timestamp()*1000)


def decode_rows(data,symbol,start,end):
    rows={}
    if not isinstance(data,list):raise ValueError('API returned non-list data')
    for item in data:
        if isinstance(item,list):
            if len(item)<6:raise ValueError('Malformed list candle')
            t,o,h,l,c,v=item[:6]
        elif isinstance(item,dict):
            t=item.get('time',item.get('openTime'))
            o,h,l,c,v=[item[k] for k in ('open','high','low','close','volume')]
        else:raise ValueError('Unknown candle format')
        t=int(t)
        if t<10**11:t*=1000
        if t%60000:raise ValueError('Unaligned candle timestamp')
        if not start<=t<end:continue
        vals=list(map(float,(o,h,l,c,v)))
        if not all(map(math.isfinite,vals)) or min(vals[:4])<=0 or vals[4]<0 or vals[2]>min(vals[0],vals[3]) or vals[1]<max(vals[0],vals[3]) or vals[2]>vals[1]:
            raise ValueError('Invalid OHLCV')
        stamp=datetime.fromtimestamp(t/1000,timezone.utc).isoformat()
        row=dict(zip(FIELDS,(stamp,symbol,*vals)))
        if t in rows and rows[t]!=row:raise ValueError('Conflicting duplicate candles')
        rows[t]=row
    return rows


def request_page(symbol,start,end,limit=1000,tries=3,opener=None):
    params={'symbol':symbol,'interval':'1m','startTime':start,'endTime':end-1,'limit':limit}
    url=BASE+'?'+urllib.parse.urlencode(params)
    opener=opener or urllib.request.urlopen
    for attempt in range(tries):
        try:
            req=urllib.request.Request(url,headers={'User-Agent':'V42HistoricalResearch/1.0'})
            with opener(req,timeout=20) as resp:body=json.load(resp)
            if str(body.get('code'))!='0':raise RuntimeError(f"BingX API {body.get('code')}: {body.get('msg','')}")
            return decode_rows(body.get('data'),symbol,start,end)
        except Exception:
            if attempt==tries-1:raise
            time.sleep(1.5*(2**attempt))


def download(symbol,start_ms,end_ms,request=request_page,limit=1000):
    if start_ms>=end_ms:raise ValueError('start must precede end')
    collected={};cursor=start_ms
    while cursor<end_ms:
        end=min(end_ms,cursor+limit*60000)
        page=request(symbol,cursor,end,limit=limit)
        for t,row in page.items():
            if t in collected and collected[t]!=row:raise ValueError('Conflicting overlapping candle')
            collected[t]=row
        cursor=end
        pages_done=(cursor-start_ms+limit*60000-1)//(limit*60000)
        total_pages=(end_ms-start_ms+limit*60000-1)//(limit*60000)
        if pages_done==1 or pages_done%10==0 or cursor>=end_ms:
            print(f'[download] {symbol} pages {pages_done}/{total_pages} candles {len(collected)}',flush=True)
    expected=set(range(start_ms,end_ms,60000));missing=sorted(expected-set(collected))
    return [collected[k] for k in sorted(collected)],missing


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--symbols',nargs='+',required=True)
    p.add_argument('--start',required=True,help='UTC ISO timestamp, e.g. 2026-08-01T00:00:00Z')
    p.add_argument('--end',required=True)
    p.add_argument('--out',required=True)
    args=p.parse_args();start=parse_utc(args.start);end=parse_utc(args.end)
    if (end-start)%60000:raise ValueError('Invalid duration')
    dest=Path(args.out);dest.mkdir(parents=True,exist_ok=True)
    manifest={'source':'BingX public swap v3 quote klines','api_endpoint':BASE,'interval':'1m','start':args.start,'end':args.end,'spread_method':'NOT_AVAILABLE','note':'OHLCV ONLY: spread_pct intentionally absent; strict full backtest will reject this input.','symbols':[]}
    for sym in args.symbols:
        sym=sym.upper()
        if not re.fullmatch(r'[A-Z0-9]{2,25}-USDT',sym):raise ValueError('Invalid symbol '+sym)
        rows,missing=download(sym,start,end)
        path=dest/(sym+'.csv')
        with path.open('w',newline='',encoding='utf-8') as f:
            w=csv.DictWriter(f,fieldnames=FIELDS);w.writeheader();w.writerows(rows)
        manifest['symbols'].append({'symbol':sym,'rows':len(rows),'missing_bars':len(missing),'missing_first_utc':[datetime.fromtimestamp(t/1000,timezone.utc).isoformat() for t in missing[:20]],'filename':path.name,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
        print(f'{sym}: {len(rows)} bars, {len(missing)} missing; {path}',flush=True)
    (dest/'ohlcv_manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
    if any(s['missing_bars'] for s in manifest['symbols']):raise SystemExit('DATA INCOMPLETE: missing 1m candles; do not use for strict backtest')

if __name__=='__main__':main()
