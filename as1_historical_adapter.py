from __future__ import annotations
import argparse,csv,math,statistics
from collections import deque
from datetime import datetime,timezone
from pathlib import Path

CORE="AS1"; START=1000.0; ORDER=20.0; MAX_INV=100.0; FEE=0.0002
GAMMA=0.10; KAPPA=2500.0; HORIZON=300.0; MIN_HALF=4.0; MAX_HALF=40.0
VOL_WINDOW=300; REQUOTE=2.0; QUEUE_MULT=1.5; RESET_BPS=0.5

def dt(s):
    x=datetime.fromisoformat(s.replace("Z","+00:00")); return x if x.tzinfo else x.replace(tzinfo=timezone.utc)
def bps(a,b): return (a/b-1)*10000 if b and b>0 else 0.0

class MM:
    def __init__(self):
        self.cash=START; self.inv=0.0; self.fees=0.0; self.buy=self.sell=0
        self.bid=self.ask=None; self.bid_touch=self.ask_touch=0.0; self.quote_t=None
        self.last_quote=None; self.last_mid=None; self.rets=deque(maxlen=VOL_WINDOW)
    def sigma2(self):
        if len(self.rets)<20:return 0.0
        return statistics.pvariance(self.rets)/0.2*HORIZON
    def quote(self,bid,ask,mid):
        s2=self.sigma2(); invn=max(-1,min(1,self.inv*mid/MAX_INV))
        res=mid*math.exp(-invn*GAMMA*s2)
        total=GAMMA*s2+(2/GAMMA)*math.log(1+GAMMA/KAPPA)
        half=max(0,total/2)*10000
        half=min(max(half,FEE*10000+MIN_HALF),MAX_HALF)
        h=half/10000; qb=res*math.exp(-h); qa=res*math.exp(h); eps=mid*1e-8
        qb=min(qb,ask-eps);qa=max(qa,bid+eps)
        if self.inv*mid>=MAX_INV:qb=None
        if self.inv*mid<=-MAX_INV:qa=None
        return qb,qa
    def requote(self,t,bid,ask,mid):
        qb,qa=self.quote(bid,ask,mid)
        if self.bid is None or qb is None or abs(bps(qb,self.bid))>=RESET_BPS:self.bid_touch=0
        if self.ask is None or qa is None or abs(bps(qa,self.ask))>=RESET_BPS:self.ask_touch=0
        self.bid,self.ask,self.quote_t=qb,qa,t
    def fill(self,side,px,touched,mid):
        qty=ORDER/px
        if touched<qty*QUEUE_MULT:return False
        inv_after=self.inv+qty if side=="BUY" else self.inv-qty
        if abs(inv_after*mid)>MAX_INV*1.02:return False
        fee=qty*px*FEE
        if side=="BUY":self.cash-=qty*px+fee;self.inv+=qty;self.buy+=1;self.bid_touch=0
        else:self.cash+=qty*px-fee;self.inv-=qty;self.sell+=1;self.ask_touch=0
        self.fees+=fee;return True
    def on(self,r):
        t=dt(r["timestamp"]); typ=r["event"].upper()
        if typ=="BOOK":
            bid=float(r["bid"]);ask=float(r["ask"]);mid=(bid+ask)/2
            if self.last_mid and self.last_mid>0:
                x=math.log(mid/self.last_mid)
                if math.isfinite(x) and abs(x)<.10:self.rets.append(x)
            self.last_mid=mid
            if self.last_quote is None or (t-self.last_quote).total_seconds()>=REQUOTE:
                self.requote(t,bid,ask,mid);self.last_quote=t
        elif typ=="TRADE" and self.last_mid:
            px=float(r["trade_price"]);qty=float(r["trade_qty"]);maker=str(r["buyer_maker"]).lower() in ("1","true","yes")
            if maker and self.bid and px<=self.bid:
                self.bid_touch+=qty
                if self.fill("BUY",self.bid,self.bid_touch,self.last_mid): self.last_quote=None
            if (not maker) and self.ask and px>=self.ask:
                self.ask_touch+=qty
                if self.fill("SELL",self.ask,self.ask_touch,self.last_mid): self.last_quote=None
    def pnl(self): return self.cash+self.inv*(self.last_mid or 0)-START

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--events",required=True,help="timestamp,event,bid,ask,trade_price,trade_qty,buyer_maker")
    ap.add_argument("--symbol",default="BTC-USDT");ap.add_argument("--out",default="data/as1_core_outcomes.csv");args=ap.parse_args()
    mm=MM()
    with open(args.events,newline="",encoding="utf-8-sig") as f:
        for r in sorted(csv.DictReader(f),key=lambda x:x["timestamp"]):mm.on(r)
    # Router uses one aggregate AS1 period outcome; microstructure backtest is portfolio-based, not trade-by-trade.
    ret=mm.pnl()/START*100
    Path(args.out).parent.mkdir(parents=True,exist_ok=True)
    with open(args.out,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=["timestamp","symbol","core","net_return_pct"]);w.writeheader()
        w.writerow({"timestamp":datetime.now(timezone.utc).isoformat(),"symbol":args.symbol.upper(),"core":CORE,"net_return_pct":round(ret,6)})
    print({"pnl_usdt":round(mm.pnl(),4),"return_pct":round(ret,4),"buy_fills":mm.buy,"sell_fills":mm.sell})
if __name__=="__main__":main()
