import os,json,math,hashlib,uuid
from datetime import datetime,timezone
import psycopg

DBURL=os.getenv('DATABASE_URL','').strip()
TRAIN_FRAC=float(os.getenv('DISCOVERY_TRAIN_FRAC','0.70'))
MIN_TRAIN=int(os.getenv('DISCOVERY_MIN_TRAIN','60'))
MIN_OOS=int(os.getenv('DISCOVERY_MIN_OOS','20'))
MAX_SAVE=int(os.getenv('DISCOVERY_MAX_SAVE','400'))
CLIP=float(os.getenv('DISCOVERY_CLIP_NET_PCT','5.0'))
MIN_TRAIN_AVG=float(os.getenv('DISCOVERY_MIN_TRAIN_AVG','0.12'))
MIN_OOS_AVG=float(os.getenv('DISCOVERY_MIN_OOS_AVG','0.08'))
MIN_TRAIN_PF=float(os.getenv('DISCOVERY_MIN_TRAIN_PF','1.15'))
MIN_OOS_PF=float(os.getenv('DISCOVERY_MIN_OOS_PF','1.10'))
MIN_FILL=float(os.getenv('DISCOVERY_MIN_FILL','0.50'))
PAPER_PASS_N=int(os.getenv('DISCOVERY_PAPER_PASS_N','30'))
LIVE_N=int(os.getenv('DISCOVERY_LIVE_CANDIDATE_N','100'))
LIVE_MIN_SPAN_H=float(os.getenv('DISCOVERY_LIVE_MIN_SPAN_HOURS','168'))
MIN_OBSERVED_H=float(os.getenv('DISCOVERY_MIN_OBSERVED_HOURS','24'))
HORIZONS=(15,30,60,120,240)
ENTRIES=('market','delay1','delay3','delay5','atr025','atr050','opposite')
DIM={
 'trend':"split_part(o.pattern_key,'|',2)", 'volume':"split_part(o.pattern_key,'|',3)",
 'spread':"split_part(o.pattern_key,'|',4)", 'regime':"split_part(o.pattern_key,'|',5)",
 'atr':"split_part(o.pattern_key,'|',6)", 'session':"split_part(o.pattern_key,'|',7)"}
GROUPS=[
 ('COARSE',()),('MOD_TREND',('trend',)),('MOD_VOLUME',('volume',)),('MOD_SPREAD',('spread',)),
 ('MOD_REGIME',('regime',)),('MOD_ATR',('atr',)),('MOD_SESSION',('session',)),
 ('PAIR_TREND_REGIME',('trend','regime')),('PAIR_TREND_ATR',('trend','atr')),
 ('PAIR_ATR_SESSION',('atr','session')),('PAIR_REGIME_SESSION',('regime','session')),
 ('PAIR_VOLUME_SPREAD',('volume','spread')),
 ('FINE',('trend','volume','spread','regime','atr','session'))]

def jd(x): return json.dumps(x,separators=(',',':'),ensure_ascii=False,default=str)
def f(x,d=0.0): return float(x) if x is not None else d
def pf(gp,gl):
 gp=f(gp); gl=f(gl)
 return 99.0 if gl<=0 and gp>0 else (0.0 if gl<=0 else gp/gl)
def conn():
 if not DBURL: raise RuntimeError('DATABASE_URL missing')
 return psycopg.connect(DBURL,autocommit=True,connect_timeout=10)
def read(c,sql,args=()):
 if not sql.lstrip().lower().startswith(('select','with')): raise RuntimeError('READ_GUARD')
 return c.execute(sql,args)
def write(c,sql,args=()):
 s=' '.join(sql.lower().split())
 if 'core_discovery_' not in s: raise RuntimeError('WRITE_GUARD_PREFIX')
 for bad in ('insert into market_research_','update market_research_','delete from market_research_',
             'alter table market_research_','drop table market_research_','truncate market_research_'):
  if bad in s: raise RuntimeError('WRITE_GUARD_RAW')
 return c.execute(sql,args)

def init(c):
 for sql in [
  """create table if not exists core_discovery_runs(
   run_id text primary key,started_at timestamptz not null,finished_at timestamptz,
   source_min_at timestamptz,source_max_at timestamptz,source_outcomes integer,split_at timestamptz,
   candidates_evaluated integer default 0,candidates_saved integer default 0,qualified integer default 0,
   paper_collecting integer default 0,errors jsonb not null default '[]'::jsonb,config jsonb not null default '{}'::jsonb)""",
  """create table if not exists core_discovery_candidates(
   candidate_id text primary key,rule_key text not null,specificity text not null,event_type text not null,
   entry text not null,horizon_min integer not null,rule jsonb not null,metrics jsonb not null,status text not null,
   first_seen_at timestamptz not null default now(),last_seen_at timestamptz not null default now(),
   paper_start_at timestamptz,paper_metrics jsonb,last_run_id text not null,updated_at timestamptz not null default now())""",
  """create index if not exists core_discovery_candidates_status_idx on core_discovery_candidates(status,updated_at desc)""",
  """create table if not exists core_discovery_candidate_history(
   run_id text not null,candidate_id text not null,snapshot_at timestamptz not null default now(),status text not null,
   rule jsonb not null,metrics jsonb not null,paper_metrics jsonb,primary key(run_id,candidate_id))""",
  """create table if not exists core_discovery_rejections(
   candidate_id text primary key,rule jsonb not null,last_reason text not null,metrics jsonb not null,
   first_seen_at timestamptz not null default now(),last_seen_at timestamptz not null default now(),seen_count integer not null default 1)"""]:
  write(c,sql)

def source_window(c):
 lo,hi,n=read(c,"select min(finalized_at),max(finalized_at),count(*)::int from market_research_v8_outcomes where event_type<>'NORMAL_CONTROL'").fetchone()
 if not lo or not hi: raise RuntimeError('NO_OUTCOMES')
 return lo,hi,int(n),lo+(hi-lo)*TRAIN_FRAC

def metric(prefix):
 return f"""
 count(*) filter(where bucket='{prefix}')::int {prefix}_total,
 count(*) filter(where bucket='{prefix}' and filled)::int {prefix}_n,
 count(*) filter(where bucket='{prefix}' and filled and net>0)::int {prefix}_wins,
 avg(net) filter(where bucket='{prefix}' and filled) {prefix}_avg,
 avg(greatest(-{CLIP},least({CLIP},net))) filter(where bucket='{prefix}' and filled) {prefix}_robust_avg,
 percentile_cont(0.5) within group(order by net) filter(where bucket='{prefix}' and filled) {prefix}_median,
 sum(net) filter(where bucket='{prefix}' and filled and net>0) {prefix}_gp,
 abs(sum(net)) filter(where bucket='{prefix}' and filled and net<0) {prefix}_gl,
 avg(mfe) filter(where bucket='{prefix}' and filled) {prefix}_mfe,
 avg(mae) filter(where bucket='{prefix}' and filled) {prefix}_mae
 """

def aggregate(c,gname,dims,split):
 ds=''.join(f', {DIM[d]} as {d}' for d in dims); dg=''.join(f', {d}' for d in dims)
 sql=f"""with x as(
  select o.event_type,o.finalized_at{ds},fh.key::int horizon_min,en.key entry,
   coalesce((en.value->>'filled')::boolean,false) filled,
   case when coalesce((en.value->>'filled')::boolean,false) then nullif(en.value->>'net','')::double precision end net,
   case when coalesce((en.value->>'filled')::boolean,false) then nullif(en.value->>'mfe','')::double precision end mfe,
   case when coalesce((en.value->>'filled')::boolean,false) then nullif(en.value->>'mae','')::double precision end mae,
   case when o.finalized_at<=%s then 'train' else 'oos' end bucket
  from market_research_v8_outcomes o
  cross join lateral jsonb_each(o.payload->'follow') fh
  cross join lateral jsonb_each(fh.value->'entries') en
  where o.event_type<>'NORMAL_CONTROL' and fh.key~'^[0-9]+$' and fh.key::int=any(%s) and en.key=any(%s))
 select event_type{dg},entry,horizon_min,{metric('train')},{metric('oos')},min(finalized_at) first_at,max(finalized_at) last_at
 from x group by event_type{dg},entry,horizon_min"""
 cur=read(c,sql,(split,list(HORIZONS),list(ENTRIES)))
 cols=[d.name for d in cur.description]
 return [dict(zip(cols,row)) for row in cur.fetchall()]

def candidate(row,gname,dims):
 rule={'event_type':row['event_type']}; rule.update({d:row[d] for d in dims})
 entry=row['entry']; hz=int(row['horizon_min'])
 sig=jd({'specificity':gname,'rule':rule,'entry':entry,'horizon_min':hz})
 cid='CORE-'+hashlib.sha1(sig.encode()).hexdigest()[:12].upper()
 tn,on=int(row['train_n'] or 0),int(row['oos_n'] or 0); tt,ot=int(row['train_total'] or 0),int(row['oos_total'] or 0)
 tw,ow=int(row['train_wins'] or 0),int(row['oos_wins'] or 0)
 tpf,opf=pf(row['train_gp'],row['train_gl']),pf(row['oos_gp'],row['oos_gl'])
 tf,of=(tn/tt if tt else 0),(on/ot if ot else 0); tra,ora=f(row['train_robust_avg']),f(row['oos_robust_avg']); omed=f(row['oos_median'],-999)
 span=(row['last_at']-row['first_at']).total_seconds()/3600 if row['last_at'] and row['first_at'] else 0
 reasons=[]
 if tn<MIN_TRAIN: reasons.append('TRAIN_N')
 if on<MIN_OOS: reasons.append('OOS_N')
 if tf<MIN_FILL: reasons.append('TRAIN_FILL')
 if of<MIN_FILL: reasons.append('OOS_FILL')
 if tra<MIN_TRAIN_AVG: reasons.append('TRAIN_EXPECTANCY')
 if ora<MIN_OOS_AVG: reasons.append('OOS_EXPECTANCY')
 if tpf<MIN_TRAIN_PF: reasons.append('TRAIN_PF')
 if opf<MIN_OOS_PF: reasons.append('OOS_PF')
 if omed<-0.05: reasons.append('OOS_MEDIAN')
 if span<MIN_OBSERVED_H: reasons.append('OBSERVED_HOURS')
 score=min(tra,5)*math.log1p(tn)*.35+min(ora,5)*math.log1p(on)*.55+min(opf,3)*.10
 m={'train':{'total':tt,'n':tn,'wins':tw,'win_pct':100*tw/tn if tn else 0,'fill_pct':100*tf,'avg_net_pct':f(row['train_avg']),'robust_avg_net_pct':tra,'median_net_pct':f(row['train_median']),'pf':tpf,'avg_mfe_pct':f(row['train_mfe']),'avg_mae_pct':f(row['train_mae'])},
    'oos':{'total':ot,'n':on,'wins':ow,'win_pct':100*ow/on if on else 0,'fill_pct':100*of,'avg_net_pct':f(row['oos_avg']),'robust_avg_net_pct':ora,'median_net_pct':omed,'pf':opf,'avg_mfe_pct':f(row['oos_mfe']),'avg_mae_pct':f(row['oos_mae'])},
    'observed_hours':span,'discovery_score':score,'gate_pass':not reasons,'gate_reasons':reasons}
 return {'candidate_id':cid,'rule_key':sig,'specificity':gname,'event_type':row['event_type'],'entry':entry,'horizon_min':hz,'rule':rule,'metrics':m,'passed':not reasons,'score':score}

def upsert(c,x,run_id,source_max):
 old=read(c,'select status,paper_start_at,paper_metrics from core_discovery_candidates where candidate_id=%s',(x['candidate_id'],)).fetchone()
 old_status=old[0] if old else None; paper_start=old[1] if old else None; paper_metrics=old[2] if old else None
 if x['passed'] and paper_start is None: paper_start=source_max
 status='PAPER_COLLECTING' if x['passed'] else 'WATCH'
 if old_status in ('PAPER_PASS','LIVE_CANDIDATE') and x['passed']: status=old_status
 write(c,"""insert into core_discovery_candidates(candidate_id,rule_key,specificity,event_type,entry,horizon_min,rule,metrics,status,first_seen_at,last_seen_at,paper_start_at,paper_metrics,last_run_id,updated_at)
 values(%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,now(),now(),%s,%s::jsonb,%s,now())
 on conflict(candidate_id) do update set rule_key=excluded.rule_key,specificity=excluded.specificity,event_type=excluded.event_type,entry=excluded.entry,horizon_min=excluded.horizon_min,rule=excluded.rule,metrics=excluded.metrics,status=excluded.status,last_seen_at=now(),paper_start_at=coalesce(core_discovery_candidates.paper_start_at,excluded.paper_start_at),paper_metrics=coalesce(core_discovery_candidates.paper_metrics,excluded.paper_metrics),last_run_id=excluded.last_run_id,updated_at=now()""",
 (x['candidate_id'],x['rule_key'],x['specificity'],x['event_type'],x['entry'],x['horizon_min'],jd(x['rule']),jd(x['metrics']),status,paper_start,jd(paper_metrics) if paper_metrics is not None else None,run_id))
 return status

def reject(c,x):
 reason=','.join(x['metrics']['gate_reasons']) or 'LOW_RANK'
 write(c,"""insert into core_discovery_rejections(candidate_id,rule,last_reason,metrics) values(%s,%s::jsonb,%s,%s::jsonb)
 on conflict(candidate_id) do update set rule=excluded.rule,last_reason=excluded.last_reason,metrics=excluded.metrics,last_seen_at=now(),seen_count=core_discovery_rejections.seen_count+1""",
 (x['candidate_id'],jd(x['rule']),reason,jd(x['metrics'])))

def paper(c,cid):
 row=read(c,'select rule,entry,horizon_min,paper_start_at,metrics from core_discovery_candidates where candidate_id=%s',(cid,)).fetchone()
 if not row or not row[3]: return None
 rule,entry,hz,start,metrics=row; wh=['o.event_type=%s','o.finalized_at>%s','fh.key=%s','en.key=%s']; args=[rule['event_type'],start,str(hz),entry]
 for d in DIM:
  if d in rule: wh.append(DIM[d]+'=%s'); args.append(rule[d])
 sql=f"""with p as(select coalesce((en.value->>'filled')::boolean,false) filled,case when coalesce((en.value->>'filled')::boolean,false) then nullif(en.value->>'net','')::double precision end net
 from market_research_v8_outcomes o cross join lateral jsonb_each(o.payload->'follow') fh cross join lateral jsonb_each(fh.value->'entries') en where {' and '.join(wh)})
 select count(*)::int,count(*) filter(where filled)::int,count(*) filter(where filled and net>0)::int,avg(net) filter(where filled),avg(greatest(-%s,least(%s,net))) filter(where filled),percentile_cont(.5) within group(order by net) filter(where filled),sum(net) filter(where filled and net>0),abs(sum(net)) filter(where filled and net<0) from p"""
 total,n,wins,av,rav,med,gp,gl=read(c,sql,tuple(args+[CLIP,CLIP])).fetchone(); total=int(total or 0); n=int(n or 0); wins=int(wins or 0)
 pm={'total':total,'n':n,'wins':wins,'win_pct':100*wins/n if n else 0,'fill_pct':100*n/total if total else 0,'avg_net_pct':f(av),'robust_avg_net_pct':f(rav),'median_net_pct':f(med),'pf':pf(gp,gl),'paper_start_at':start.isoformat()}
 gate_pass=bool((metrics or {}).get('gate_pass',False)); status='PAPER_COLLECTING' if gate_pass else 'WATCH'; span=f((metrics or {}).get('observed_hours'))
 if gate_pass and n>=PAPER_PASS_N and pm['robust_avg_net_pct']>0 and pm['pf']>=1.10: status='PAPER_PASS'
 if gate_pass and n>=LIVE_N and pm['robust_avg_net_pct']>=MIN_OOS_AVG and pm['pf']>=1.25 and span>=LIVE_MIN_SPAN_H: status='LIVE_CANDIDATE'
 write(c,'update core_discovery_candidates set paper_metrics=%s::jsonb,status=%s,updated_at=now() where candidate_id=%s',(jd(pm),status,cid))
 return status,pm

def main():
 run_id='DISC-'+uuid.uuid4().hex[:12].upper(); started=datetime.now(timezone.utc); errors=[]
 cfg={'mode':'RESEARCH_ONLY_NO_ORDERS','raw_tables':'READ_ONLY_BY_CODE_GUARD','train_frac':TRAIN_FRAC,'min_train':MIN_TRAIN,'min_oos':MIN_OOS,'clip_net_pct':CLIP,'min_train_avg':MIN_TRAIN_AVG,'min_oos_avg':MIN_OOS_AVG,'min_train_pf':MIN_TRAIN_PF,'min_oos_pf':MIN_OOS_PF,'min_fill':MIN_FILL,'min_observed_hours':MIN_OBSERVED_H,'horizons':HORIZONS,'entries':ENTRIES,'groups':[g[0] for g in GROUPS]}
 with conn() as c:
  init(c); lo,hi,source_n,split=source_window(c)
  write(c,'insert into core_discovery_runs(run_id,started_at,source_min_at,source_max_at,source_outcomes,split_at,config) values(%s,%s,%s,%s,%s,%s,%s::jsonb)',(run_id,started,lo,hi,source_n,split,jd(cfg)))
  allc=[]; evaluated=0
  for g,dims in GROUPS:
   try:
    rows=aggregate(c,g,dims,split); evaluated+=len(rows)
    for r in rows:
     x=candidate(r,g,dims)
     if x['metrics']['train']['n']>=max(20,MIN_TRAIN//2) and x['metrics']['oos']['n']>=max(8,MIN_OOS//2): allc.append(x)
   except Exception as e: errors.append({'group':g,'error':type(e).__name__+':'+str(e)[:240]})
  passed=[x for x in allc if x['passed']]; ranked=sorted(allc,key=lambda x:x['score'],reverse=True); keep={x['candidate_id']:x for x in ranked[:MAX_SAVE]}
  for x in passed: keep[x['candidate_id']]=x
  saved=0
  for x in keep.values():
   try:
    upsert(c,x,run_id,hi)
    write(c,"""insert into core_discovery_candidate_history(run_id,candidate_id,status,rule,metrics,paper_metrics)
     select %s,candidate_id,status,rule,metrics,paper_metrics from core_discovery_candidates where candidate_id=%s on conflict(run_id,candidate_id) do nothing""",(run_id,x['candidate_id']))
    saved+=1
   except Exception as e: errors.append({'candidate':x['candidate_id'],'error':type(e).__name__+':'+str(e)[:240]})
  for x in ranked[:min(200,len(ranked))]:
   if not x['passed']:
    try: reject(c,x)
    except Exception as e: errors.append({'reject':x['candidate_id'],'error':type(e).__name__+':'+str(e)[:240]})
  ids=[r[0] for r in read(c,'select candidate_id from core_discovery_candidates where paper_start_at is not null order by updated_at desc limit 100').fetchall()]
  for cid in ids:
   try: paper(c,cid)
   except Exception as e: errors.append({'paper':cid,'error':type(e).__name__+':'+str(e)[:240]})
  counts=dict(read(c,'select status,count(*)::int from core_discovery_candidates group by status').fetchall())
  write(c,'update core_discovery_runs set finished_at=now(),candidates_evaluated=%s,candidates_saved=%s,qualified=%s,paper_collecting=%s,errors=%s::jsonb where run_id=%s',(evaluated,saved,len(passed),int(counts.get('PAPER_COLLECTING',0)),jd(errors),run_id))
  top=read(c,"""select candidate_id,specificity,event_type,entry,horizon_min,status,metrics->'oos'->>'n',metrics->'oos'->>'robust_avg_net_pct',metrics->'oos'->>'pf',paper_metrics from core_discovery_candidates order by (metrics->>'discovery_score')::double precision desc nulls last limit 10""").fetchall()
  print('DISCOVERY_DONE '+jd({'run_id':run_id,'source_outcomes':source_n,'source_min_at':lo.isoformat(),'source_max_at':hi.isoformat(),'split_at':split.isoformat(),'evaluated':evaluated,'saved':saved,'qualified':len(passed),'status_counts':counts,'errors':errors[:10],'top':top}),flush=True)
if __name__=='__main__': main()
