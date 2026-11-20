import os,json,math,hashlib,uuid
from datetime import datetime,timezone,timedelta
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
FAMILY_STANDBY=int(os.getenv('DISCOVERY_FAMILY_STANDBY','4'))
RETIRE_MIN_N=int(os.getenv('DISCOVERY_RETIRE_MIN_N','20'))
RETIRE_MAX_ROBUST=float(os.getenv('DISCOVERY_RETIRE_MAX_ROBUST','-0.05'))
RETIRE_MAX_PF=float(os.getenv('DISCOVERY_RETIRE_MAX_PF','0.90'))
RETIRE_MAX_MEDIAN=float(os.getenv('DISCOVERY_RETIRE_MAX_MEDIAN','-0.10'))
HOLDOUT_HOURS=float(os.getenv('DISCOVERY_HOLDOUT_HOURS','24'))
HOLDOUT_MIN_N=int(os.getenv('DISCOVERY_HOLDOUT_MIN_N','20'))
HOLDOUT_PASS_ROBUST=float(os.getenv('DISCOVERY_HOLDOUT_PASS_ROBUST','0.08'))
HOLDOUT_PASS_PF=float(os.getenv('DISCOVERY_HOLDOUT_PASS_PF','1.10'))
HOLDOUT_FAIL_ROBUST=float(os.getenv('DISCOVERY_HOLDOUT_FAIL_ROBUST','-0.05'))
HOLDOUT_FAIL_PF=float(os.getenv('DISCOVERY_HOLDOUT_FAIL_PF','0.90'))
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
  """alter table core_discovery_candidates add column if not exists family_key text""",
  """alter table core_discovery_candidates add column if not exists family_rank integer""",
  """alter table core_discovery_candidates add column if not exists family_role text""",
  """alter table core_discovery_candidates add column if not exists retired_at timestamptz""",
  """alter table core_discovery_candidates add column if not exists retirement_reason text""",
  """alter table core_discovery_candidates add column if not exists replaced_by text""",
  """alter table core_discovery_candidates add column if not exists failure_streak integer not null default 0""",
  """alter table core_discovery_candidates add column if not exists validation_epoch text""",
  """alter table core_discovery_candidates add column if not exists holdout_status text""",
  """alter table core_discovery_candidates add column if not exists holdout_metrics jsonb""",
  """alter table core_discovery_candidates add column if not exists holdout_checked_at timestamptz""",
  """alter table core_discovery_candidates add column if not exists forward_start_at timestamptz""",
  """alter table core_discovery_candidates add column if not exists prelock_paper_metrics jsonb""",
  """create index if not exists core_discovery_candidates_status_idx on core_discovery_candidates(status,updated_at desc)""",
  """create index if not exists core_discovery_candidates_family_idx on core_discovery_candidates(family_key,family_role,family_rank)""",
  """create table if not exists core_discovery_candidate_history(
   run_id text not null,candidate_id text not null,snapshot_at timestamptz not null default now(),status text not null,
   rule jsonb not null,metrics jsonb not null,paper_metrics jsonb,primary key(run_id,candidate_id))""",
  """create table if not exists core_discovery_rejections(
   candidate_id text primary key,rule jsonb not null,last_reason text not null,metrics jsonb not null,
   first_seen_at timestamptz not null default now(),last_seen_at timestamptz not null default now(),seen_count integer not null default 1)""",
  """create table if not exists core_discovery_family_state(
   family_key text primary key,event_type text not null,side_mode text not null,
   active_candidate_id text,active_since timestamptz,last_switch_at timestamptz,
   switch_count integer not null default 0,updated_at timestamptz not null default now())""",
  """create table if not exists core_discovery_lifecycle_events(
   id bigserial primary key,run_id text not null,ts timestamptz not null default now(),
   family_key text,candidate_id text,event_type text,event text not null,
   from_role text,to_role text,reason text,replacement_candidate_id text,details jsonb not null default '{}'::jsonb)""",
  """create table if not exists core_discovery_validation_lock(
   epoch_id text primary key,created_at timestamptz not null default now(),
   dev_start_at timestamptz not null,dev_end_at timestamptz not null,train_end_at timestamptz not null,
   holdout_start_at timestamptz not null,holdout_end_at timestamptz not null,status text not null,
   config jsonb not null default '{}'::jsonb,updated_at timestamptz not null default now())""",
  """create table if not exists core_discovery_holdout_assignments(
   epoch_id text not null,family_key text not null,candidate_id text not null,assigned_at timestamptz not null default now(),
   status text not null default 'COLLECTING',metrics jsonb,checked_at timestamptz,
   primary key(epoch_id,family_key))"""
 ]:
  write(c,sql)

def source_window(c):
 lo,hi,n=read(c,"select min(finalized_at),max(finalized_at),count(*)::int from market_research_v8_outcomes where event_type<>'NORMAL_CONTROL'").fetchone()
 if not lo or not hi: raise RuntimeError('NO_OUTCOMES')
 lock=read(c,"""select epoch_id,dev_start_at,dev_end_at,train_end_at,holdout_start_at,holdout_end_at,status
                 from core_discovery_validation_lock order by created_at desc limit 1""").fetchone()
 if not lock:
  epoch='EPOCH-'+uuid.uuid4().hex[:10].upper(); dev_end=hi; split=lo+(dev_end-lo)*TRAIN_FRAC
  holdout_start=dev_end; holdout_end=holdout_start+timedelta(hours=HOLDOUT_HOURS)
  cfg={'mode':'FROZEN_FUTURE_HOLDOUT','holdout_hours':HOLDOUT_HOURS,'train_frac':TRAIN_FRAC}
  write(c,"""insert into core_discovery_validation_lock(epoch_id,dev_start_at,dev_end_at,train_end_at,holdout_start_at,holdout_end_at,status,config)
             values(%s,%s,%s,%s,%s,%s,'COLLECTING',%s::jsonb)""",(epoch,lo,dev_end,split,holdout_start,holdout_end,jd(cfg)))
  # Anything seen before the lock is audit history only. It must not drive the new forward-validation cycle.
  write(c,"""update core_discovery_candidates set
             prelock_paper_metrics=coalesce(prelock_paper_metrics,paper_metrics),paper_metrics=null,forward_start_at=null,
             holdout_status=null,holdout_metrics=null,holdout_checked_at=null,validation_epoch=%s,failure_streak=0,
             status=case when coalesce((metrics->>'gate_pass')::boolean,false) then 'PAPER_COLLECTING' else 'WATCH' end,updated_at=now()
             where retired_at is null""",(epoch,))
  return lo,hi,int(n),dev_end,split,holdout_start,holdout_end,epoch,'COLLECTING'
 epoch,dev_start,dev_end,split,holdout_start,holdout_end,status=lock
 state='READY' if hi>=holdout_end else 'COLLECTING'
 if status!=state and status!='EVALUATED':
  write(c,'update core_discovery_validation_lock set status=%s,updated_at=now() where epoch_id=%s',(state,epoch))
 elif status=='EVALUATED': state='EVALUATED'
 return dev_start,hi,int(n),dev_end,split,holdout_start,holdout_end,epoch,state

def metric(prefix):
 return f"""
 count(*) filter(where bucket='{prefix}')::int {prefix}_total,
 count(*) filter(where bucket='{prefix}' and filled)::int {prefix}_n,
 count(*) filter(where bucket='{prefix}' and filled and net>0)::int {prefix}_wins,
 avg(net) filter(where bucket='{prefix}' and filled) {prefix}_avg,
 avg(greatest(-{CLIP},least({CLIP},net))) filter(where bucket='{prefix}' and filled) {prefix}_robust_avg,
 percentile_cont(0.5) within group(order by net) filter(where bucket='{prefix}' and filled) {prefix}_median,
 sum(net) filter(where bucket='{prefix}' and filled and net>0) {prefix}_gp,
 abs(sum(net) filter(where bucket='{prefix}' and filled and net<0)) {prefix}_gl,
 avg(mfe) filter(where bucket='{prefix}' and filled) {prefix}_mfe,
 avg(mae) filter(where bucket='{prefix}' and filled) {prefix}_mae
 """

def family_of(event_type,entry):
 return event_type+'|'+('OPPOSITE' if entry=='opposite' else 'NATIVE')

def lifecycle_event(c,run_id,candidate_id,family_key,event_type,event,from_role,to_role,reason,replacement=None,details=None):
 write(c,"""insert into core_discovery_lifecycle_events(run_id,family_key,candidate_id,event_type,event,from_role,to_role,reason,replacement_candidate_id,details)
 values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)""",
       (run_id,family_key,candidate_id,event_type,event,from_role,to_role,reason,replacement,jd(details or {})))

def retire_candidate(c,cid,run_id,reason,replacement=None):
 row=read(c,"select family_key,event_type,family_role,retired_at,paper_metrics,metrics from core_discovery_candidates where candidate_id=%s",(cid,)).fetchone()
 if not row:return False
 family_key,event_type,role,retired_at,pm,metrics=row
 if retired_at:return False
 write(c,"""update core_discovery_candidates
           set status='RETIRED',family_role='RETIRED',retired_at=now(),retirement_reason=%s,replaced_by=%s,updated_at=now()
           where candidate_id=%s""",(reason,replacement,cid))
 lifecycle_event(c,run_id,cid,family_key,event_type,'RETIRED',role,'RETIRED',reason,replacement,{'paper_metrics':pm,'metrics':metrics})
 return True

def candidate_quality(metrics):
 m=metrics or {}
 return f(m.get('discovery_score'))


def aggregate(c,gname,dims,split,dev_end):
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
  where o.event_type<>'NORMAL_CONTROL' and o.finalized_at<=%s and fh.key~'^[0-9]+$' and fh.key::int=any(%s) and en.key=any(%s))
 select event_type{dg},entry,horizon_min,{metric('train')},{metric('oos')},min(finalized_at) first_at,max(finalized_at) last_at
 from x group by event_type{dg},entry,horizon_min"""
 cur=read(c,sql,(split,dev_end,list(HORIZONS),list(ENTRIES)))
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
 return {'candidate_id':cid,'rule_key':sig,'specificity':gname,'event_type':row['event_type'],'entry':entry,'horizon_min':hz,'family_key':family_of(row['event_type'],entry),'rule':rule,'metrics':m,'passed':not reasons,'score':score}

def upsert(c,x,run_id):
 old=read(c,'select status,paper_start_at,paper_metrics,family_role,retired_at from core_discovery_candidates where candidate_id=%s',(x['candidate_id'],)).fetchone()
 old_status=old[0] if old else None; paper_start=old[1] if old else None; paper_metrics=old[2] if old else None
 old_role=old[3] if old else None; retired_at=old[4] if old else None
 status='PAPER_COLLECTING' if x['passed'] else 'WATCH'
 if old_status in ('PAPER_PASS','LIVE_CANDIDATE') and x['passed']: status=old_status
 if retired_at is not None: status='RETIRED'
 write(c,"""insert into core_discovery_candidates(candidate_id,rule_key,specificity,event_type,entry,horizon_min,family_key,rule,metrics,status,first_seen_at,last_seen_at,paper_start_at,paper_metrics,last_run_id,updated_at)
 values(%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,now(),now(),%s,%s::jsonb,%s,now())
 on conflict(candidate_id) do update set rule_key=excluded.rule_key,specificity=excluded.specificity,event_type=excluded.event_type,entry=excluded.entry,horizon_min=excluded.horizon_min,
 family_key=excluded.family_key,rule=excluded.rule,metrics=excluded.metrics,
 status=case when core_discovery_candidates.retired_at is not null then 'RETIRED' else excluded.status end,
 last_seen_at=now(),paper_start_at=coalesce(core_discovery_candidates.paper_start_at,excluded.paper_start_at),
 paper_metrics=coalesce(core_discovery_candidates.paper_metrics,excluded.paper_metrics),last_run_id=excluded.last_run_id,updated_at=now()""",
 (x['candidate_id'],x['rule_key'],x['specificity'],x['event_type'],x['entry'],x['horizon_min'],x['family_key'],jd(x['rule']),jd(x['metrics']),status,paper_start,jd(paper_metrics) if paper_metrics is not None else None,run_id))
 return status

def reject(c,x):
 reason=','.join(x['metrics']['gate_reasons']) or 'LOW_RANK'
 write(c,"""insert into core_discovery_rejections(candidate_id,rule,last_reason,metrics) values(%s,%s::jsonb,%s,%s::jsonb)
 on conflict(candidate_id) do update set rule=excluded.rule,last_reason=excluded.last_reason,metrics=excluded.metrics,last_seen_at=now(),seen_count=core_discovery_rejections.seen_count+1""",
 (x['candidate_id'],jd(x['rule']),reason,jd(x['metrics'])))

def window_metrics(c,cid,start,end=None):
 row=read(c,'select rule,entry,horizon_min from core_discovery_candidates where candidate_id=%s',(cid,)).fetchone()
 if not row:return None
 rule,entry,hz=row; wh=['o.event_type=%s','o.finalized_at>%s','fh.key=%s','en.key=%s']; args=[rule['event_type'],start,str(hz),entry]
 if end is not None:
  wh.append('o.finalized_at<=%s'); args.append(end)
 for d in DIM:
  if d in rule: wh.append(DIM[d]+'=%s'); args.append(rule[d])
 sql=f"""with p as(select o.finalized_at,coalesce((en.value->>'filled')::boolean,false) filled,
 case when coalesce((en.value->>'filled')::boolean,false) then nullif(en.value->>'net','')::double precision end net
 from market_research_v8_outcomes o cross join lateral jsonb_each(o.payload->'follow') fh cross join lateral jsonb_each(fh.value->'entries') en where {' and '.join(wh)})
 select count(*)::int,count(*) filter(where filled)::int,count(*) filter(where filled and net>0)::int,
 avg(net) filter(where filled),avg(greatest(-%s,least(%s,net))) filter(where filled),
 percentile_cont(.5) within group(order by net) filter(where filled),
 sum(net) filter(where filled and net>0),abs(sum(net) filter(where filled and net<0)),
 min(finalized_at) filter(where filled),max(finalized_at) filter(where filled) from p"""
 total,n,wins,av,rav,med,gp,gl,first_at,last_at=read(c,sql,tuple(args+[CLIP,CLIP])).fetchone()
 total=int(total or 0); n=int(n or 0); wins=int(wins or 0)
 span=((last_at-first_at).total_seconds()/3600) if first_at and last_at else 0.0
 return {'total':total,'n':n,'wins':wins,'win_pct':100*wins/n if n else 0,'fill_pct':100*n/total if total else 0,
         'avg_net_pct':f(av),'robust_avg_net_pct':f(rav),'median_net_pct':f(med),'pf':pf(gp,gl),
         'window_start_at':start.isoformat(),'window_end_at':end.isoformat() if end else None,'observed_hours':span,
         'first_outcome_at':first_at.isoformat() if first_at else None,'last_outcome_at':last_at.isoformat() if last_at else None}

def paper(c,cid):
 row=read(c,'select metrics,family_role,retired_at,forward_start_at,holdout_status from core_discovery_candidates where candidate_id=%s',(cid,)).fetchone()
 if not row or row[2] is not None or row[1]!='ACTIVE' or not row[3]:return None
 metrics,_,_,start,holdout_status=row; pm=window_metrics(c,cid,start)
 if pm is None:return None
 pm['paper_start_at']=start.isoformat()
 n=int(pm.get('n') or 0); span=f(pm.get('observed_hours')); gate_pass=bool((metrics or {}).get('gate_pass',False))
 status='PAPER_COLLECTING' if gate_pass else 'WATCH'
 if gate_pass and n>=PAPER_PASS_N and pm['robust_avg_net_pct']>0 and pm['pf']>=1.10: status='PAPER_PASS'
 if gate_pass and holdout_status in ('PASS','SKIPPED_REPLACEMENT') and n>=LIVE_N and pm['robust_avg_net_pct']>=MIN_OOS_AVG and pm['pf']>=1.25 and span>=LIVE_MIN_SPAN_H: status='LIVE_CANDIDATE'
 write(c,'update core_discovery_candidates set paper_metrics=%s::jsonb,status=%s,updated_at=now() where candidate_id=%s',(jd(pm),status,cid))
 return status,pm

def ensure_holdout_assignments(c,run_id,epoch):
 rows=read(c,"""select f.family_key,f.event_type,f.active_candidate_id
                 from core_discovery_family_state f where f.active_candidate_id is not null order by f.family_key""").fetchall()
 added=0
 for fk,event_type,cid in rows:
  exists=read(c,'select 1 from core_discovery_holdout_assignments where epoch_id=%s and family_key=%s',(epoch,fk)).fetchone()
  if exists:continue
  write(c,"""insert into core_discovery_holdout_assignments(epoch_id,family_key,candidate_id,status)
             values(%s,%s,%s,'COLLECTING')""",(epoch,fk,cid))
  write(c,"""update core_discovery_candidates set validation_epoch=%s,holdout_status='COLLECTING',
             holdout_metrics=null,holdout_checked_at=null,forward_start_at=null,paper_metrics=null,failure_streak=0,updated_at=now() where candidate_id=%s""",(epoch,cid))
  lifecycle_event(c,run_id,cid,fk,event_type,'HOLDOUT_ASSIGNED','ACTIVE','ACTIVE','FROZEN_FUTURE_WINDOW')
  added+=1
 return added

def holdout_verdict(pm):
 n=int((pm or {}).get('n') or 0)
 if n<HOLDOUT_MIN_N:return 'INSUFFICIENT'
 rav=f(pm.get('robust_avg_net_pct')); ppf=f(pm.get('pf')); med=f(pm.get('median_net_pct'))
 if rav<=HOLDOUT_FAIL_ROBUST or ppf<HOLDOUT_FAIL_PF:return 'FAIL'
 if rav>=HOLDOUT_PASS_ROBUST and ppf>=HOLDOUT_PASS_PF and med>-0.05:return 'PASS'
 return 'NEUTRAL'

def promote_replacement(c,run_id,fk,event_type,old,reason):
 repl=read(c,"""select candidate_id from core_discovery_candidates
                 where family_key=%s and retired_at is null and candidate_id<>%s
                   and coalesce((metrics->>'gate_pass')::boolean,false)=true
                 order by case when family_role='STANDBY' then 0 else 1 end,
                          (metrics->>'discovery_score')::double precision desc nulls last limit 1""",(fk,old)).fetchone()
 active=repl[0] if repl else None
 write(c,"""update core_discovery_family_state set active_candidate_id=%s,active_since=case when %s is null then null else now() end,
           last_switch_at=now(),switch_count=switch_count+1,updated_at=now() where family_key=%s""",(active,active,fk))
 if active:
  write(c,"""update core_discovery_candidates set family_role='ACTIVE',family_rank=1,failure_streak=0,
             forward_start_at=now(),paper_metrics=null,holdout_status=coalesce(holdout_status,'SKIPPED_REPLACEMENT'),updated_at=now() where candidate_id=%s""",(active,))
  lifecycle_event(c,run_id,active,fk,event_type,'PROMOTED','STANDBY','ACTIVE',reason,None,{'replaced':old,'forward_from_promotion':True})
  write(c,'update core_discovery_candidates set replaced_by=%s where candidate_id=%s',(active,old))
 return active

def evaluate_holdout(c,run_id,epoch,holdout_start,holdout_end,current_hi):
 if current_hi<holdout_end:
  n=read(c,"select count(*)::int from core_discovery_holdout_assignments where epoch_id=%s and status='COLLECTING'",(epoch,)).fetchone()[0]
  return {'state':'COLLECTING','pending':int(n or 0),'passed':0,'failed':0,'neutral':0,'insufficient':0,'switches':0}
 rows=read(c,"""select a.family_key,a.candidate_id,f.event_type
                 from core_discovery_holdout_assignments a join core_discovery_family_state f using(family_key)
                 where a.epoch_id=%s and a.status='COLLECTING' order by a.family_key""",(epoch,)).fetchall()
 out={'state':'EVALUATING','pending':len(rows),'passed':0,'failed':0,'neutral':0,'insufficient':0,'switches':0}
 for fk,cid,event_type in rows:
  pm=window_metrics(c,cid,holdout_start,holdout_end); verdict=holdout_verdict(pm or {})
  write(c,"""update core_discovery_holdout_assignments set status=%s,metrics=%s::jsonb,checked_at=now()
             where epoch_id=%s and family_key=%s""",(verdict,jd(pm or {}),epoch,fk))
  write(c,"""update core_discovery_candidates set validation_epoch=%s,holdout_status=%s,holdout_metrics=%s::jsonb,
             holdout_checked_at=now(),updated_at=now() where candidate_id=%s""",(epoch,verdict,jd(pm or {}),cid))
  lifecycle_event(c,run_id,cid,fk,event_type,'HOLDOUT_'+verdict,'ACTIVE','ACTIVE','FROZEN_HOLDOUT_'+verdict,None,pm or {})
  if verdict=='PASS':out['passed']+=1
  elif verdict=='FAIL':
   out['failed']+=1
   cur=read(c,'select active_candidate_id from core_discovery_family_state where family_key=%s',(fk,)).fetchone()
   if cur and cur[0]==cid:
    retire_candidate(c,cid,run_id,'FROZEN_HOLDOUT_FAIL')
    promote_replacement(c,run_id,fk,event_type,cid,'FROZEN_HOLDOUT_FAIL'); out['switches']+=1
  elif verdict=='NEUTRAL':out['neutral']+=1
  else:out['insufficient']+=1
  if verdict!='FAIL':
   cur=read(c,'select active_candidate_id from core_discovery_family_state where family_key=%s',(fk,)).fetchone()
   if cur and cur[0]==cid:
    write(c,'update core_discovery_candidates set forward_start_at=%s,paper_metrics=null,failure_streak=0,updated_at=now() where candidate_id=%s',(holdout_end,cid))
 left=read(c,"select count(*)::int from core_discovery_holdout_assignments where epoch_id=%s and status='COLLECTING'",(epoch,)).fetchone()[0]
 if int(left or 0)==0:
  write(c,"update core_discovery_validation_lock set status='EVALUATED',updated_at=now() where epoch_id=%s",(epoch,)); out['state']='EVALUATED'
 return out

def seed_family_roles(c,run_id):
 families=[r[0] for r in read(c,"select distinct family_key from core_discovery_candidates where family_key is not null and retired_at is null").fetchall()]
 for fk in families:
  all_rows=read(c,"""select candidate_id,event_type,entry,status,metrics,paper_metrics,family_role,retired_at
                     from core_discovery_candidates where family_key=%s and retired_at is null""",(fk,)).fetchall()
  if not all_rows:continue
  eligible=[r for r in all_rows if bool((r[4] or {}).get('gate_pass',False))]
  if not eligible:continue
  event_type=eligible[0][1]; side='OPPOSITE' if eligible[0][2]=='opposite' else 'NATIVE'
  st=read(c,'select active_candidate_id from core_discovery_family_state where family_key=%s',(fk,)).fetchone()
  active=st[0] if st else None
  all_ids={r[0] for r in all_rows}
  # Keep the existing active core until manage_families confirms failure twice.
  if active not in all_ids:
   ranked0=sorted(eligible,key=lambda r:candidate_quality(r[4]),reverse=True)
   active=ranked0[0][0]
   existed=bool(st)
   write(c,"""insert into core_discovery_family_state(family_key,event_type,side_mode,active_candidate_id,active_since,last_switch_at,switch_count,updated_at)
              values(%s,%s,%s,%s,now(),now(),0,now())
              on conflict(family_key) do update set event_type=excluded.event_type,side_mode=excluded.side_mode,
              active_candidate_id=excluded.active_candidate_id,active_since=now(),last_switch_at=now(),
              switch_count=core_discovery_family_state.switch_count+1,updated_at=now()""",
         (fk,event_type,side,active))
   lifecycle_event(c,run_id,active,fk,event_type,'ACTIVATED',None,'ACTIVE','RECOVERY' if existed else 'INITIAL')
  ranked=sorted(eligible,key=lambda r:candidate_quality(r[4]),reverse=True)
  standbys=[r[0] for r in ranked if r[0]!=active][:FAMILY_STANDBY]
  write(c,"update core_discovery_candidates set family_role=null,family_rank=null where family_key=%s and retired_at is null",(fk,))
  write(c,"update core_discovery_candidates set family_role='ACTIVE',family_rank=1 where candidate_id=%s",(active,))
  for i,cid in enumerate(standbys,2):
   write(c,"update core_discovery_candidates set family_role='STANDBY',family_rank=%s where candidate_id=%s",(i,cid))

def fail_reason(status,metrics,pm):
 if status=='WATCH' or not bool((metrics or {}).get('gate_pass',False)):return 'OOS_GATE_LOST'
 pm=pm or {}; n=int(pm.get('n') or 0)
 if n<RETIRE_MIN_N:return None
 rav=f(pm.get('robust_avg_net_pct')); ppf=f(pm.get('pf')); med=f(pm.get('median_net_pct'))
 if rav<=RETIRE_MAX_ROBUST:return 'PAPER_NEGATIVE_EXPECTANCY'
 if ppf<RETIRE_MAX_PF:return 'PAPER_PF_FAILED'
 if med<=RETIRE_MAX_MEDIAN and rav<=0:return 'PAPER_MEDIAN_FAILED'
 return None

def manage_families(c,run_id):
 families=[r[0] for r in read(c,'select family_key from core_discovery_family_state order by family_key').fetchall()]
 switches=0; retired=0; warnings=0
 for fk in families:
  st=read(c,'select event_type,side_mode,active_candidate_id from core_discovery_family_state where family_key=%s',(fk,)).fetchone()
  if not st:continue
  event_type,side,active=st
  if active:
   row=read(c,'select status,metrics,paper_metrics,family_role,retired_at,failure_streak,forward_start_at from core_discovery_candidates where candidate_id=%s',(active,)).fetchone()
   reason='MISSING_ACTIVE' if not row else (None if row[4] is not None or not row[6] else fail_reason(row[0],row[1],row[2]))
   if row and row[4] is not None:reason='ALREADY_RETIRED'
   confirmed=False
   if row and row[4] is None:
    if reason:
     streak=int(row[5] or 0)+1; write(c,'update core_discovery_candidates set failure_streak=%s,updated_at=now() where candidate_id=%s',(streak,active))
     warnings+=1; confirmed=streak>=2
    elif int(row[5] or 0)!=0:
     write(c,'update core_discovery_candidates set failure_streak=0,updated_at=now() where candidate_id=%s',(active,))
   elif reason:confirmed=True
   if reason and confirmed:
    old=active
    if row and row[4] is None and retire_candidate(c,old,run_id,reason):retired+=1
    active=promote_replacement(c,run_id,fk,event_type,old,'ACTIVE_FAILED'); switches+=1

  current=read(c,'select active_candidate_id from core_discovery_family_state where family_key=%s',(fk,)).fetchone()
  active=current[0] if current else None
  rows=read(c,"""select candidate_id,metrics from core_discovery_candidates
                 where family_key=%s and retired_at is null and coalesce((metrics->>'gate_pass')::boolean,false)=true
                 order by (metrics->>'discovery_score')::double precision desc nulls last""",(fk,)).fetchall()
  ranked=sorted(rows,key=lambda r:candidate_quality(r[1]),reverse=True)
  standbys=[r[0] for r in ranked if r[0]!=active][:FAMILY_STANDBY]
  write(c,"update core_discovery_candidates set family_role=null,family_rank=null where family_key=%s and retired_at is null",(fk,))
  if active:write(c,"update core_discovery_candidates set family_role='ACTIVE',family_rank=1 where candidate_id=%s",(active,))
  for i,cid in enumerate(standbys,2):
   write(c,"update core_discovery_candidates set family_role='STANDBY',family_rank=%s,forward_start_at=null,paper_metrics=null where candidate_id=%s",(i,cid))
 return {'switches':switches,'retired':retired,'warnings':warnings}

def main():
 run_id='DISC-'+uuid.uuid4().hex[:12].upper(); started=datetime.now(timezone.utc); errors=[]
 with conn() as c:
  init(c); lo,hi,source_n,dev_end,split,holdout_start,holdout_end,epoch,validation_state=source_window(c)
  cfg={'mode':'RESEARCH_ONLY_NO_ORDERS','raw_tables':'READ_ONLY_BY_CODE_GUARD','validation':'FROZEN_FUTURE_HOLDOUT',
       'epoch':epoch,'dev_end_at':dev_end,'holdout_start_at':holdout_start,'holdout_end_at':holdout_end,'holdout_hours':HOLDOUT_HOURS,
       'holdout_min_n':HOLDOUT_MIN_N,'train_frac':TRAIN_FRAC,'min_train':MIN_TRAIN,'min_oos':MIN_OOS,'clip_net_pct':CLIP,
       'min_train_avg':MIN_TRAIN_AVG,'min_oos_avg':MIN_OOS_AVG,'min_train_pf':MIN_TRAIN_PF,'min_oos_pf':MIN_OOS_PF,
       'min_fill':MIN_FILL,'min_observed_hours':MIN_OBSERVED_H,'family_standby':FAMILY_STANDBY,'retire_min_n':RETIRE_MIN_N,
       'retire_max_robust':RETIRE_MAX_ROBUST,'retire_max_pf':RETIRE_MAX_PF,'retire_max_median':RETIRE_MAX_MEDIAN,
       'horizons':HORIZONS,'entries':ENTRIES,'groups':[g[0] for g in GROUPS]}
  write(c,'insert into core_discovery_runs(run_id,started_at,source_min_at,source_max_at,source_outcomes,split_at,config) values(%s,%s,%s,%s,%s,%s,%s::jsonb)',(run_id,started,lo,hi,source_n,split,jd(cfg)))
  allc=[]; evaluated=0
  for g,dims in GROUPS:
   try:
    rows=aggregate(c,g,dims,split,dev_end); evaluated+=len(rows)
    for r in rows:
     x=candidate(r,g,dims)
     if x['metrics']['train']['n']>=max(20,MIN_TRAIN//2) and x['metrics']['oos']['n']>=max(8,MIN_OOS//2):allc.append(x)
   except Exception as e:errors.append({'group':g,'error':type(e).__name__+':'+str(e)[:240]})
  passed=[x for x in allc if x['passed']]; ranked=sorted(allc,key=lambda x:x['score'],reverse=True); keep={x['candidate_id']:x for x in ranked[:MAX_SAVE]}
  for x in passed:keep[x['candidate_id']]=x
  saved=0
  for x in keep.values():
   try:
    upsert(c,x,run_id)
    write(c,"""insert into core_discovery_candidate_history(run_id,candidate_id,status,rule,metrics,paper_metrics)
     select %s,candidate_id,status,rule,metrics,paper_metrics from core_discovery_candidates where candidate_id=%s on conflict(run_id,candidate_id) do nothing""",(run_id,x['candidate_id']))
    saved+=1
   except Exception as e:errors.append({'candidate':x['candidate_id'],'error':type(e).__name__+':'+str(e)[:240]})
  for x in ranked[:min(200,len(ranked))]:
   if not x['passed']:
    try:reject(c,x)
    except Exception as e:errors.append({'reject':x['candidate_id'],'error':type(e).__name__+':'+str(e)[:240]})

  try:seed_family_roles(c,run_id)
  except Exception as e:errors.append({'family_seed':type(e).__name__+':'+str(e)[:240]})
  try:assigned=ensure_holdout_assignments(c,run_id,epoch)
  except Exception as e:assigned=0; errors.append({'holdout_assign':type(e).__name__+':'+str(e)[:240]})
  try:holdout_result=evaluate_holdout(c,run_id,epoch,holdout_start,holdout_end,hi)
  except Exception as e:holdout_result={'state':'ERROR'}; errors.append({'holdout_eval':type(e).__name__+':'+str(e)[:240]})

  # Only ACTIVE cores with a clean forward_start_at may consume forward data. Standbys never peek at future data.
  ids=[r[0] for r in read(c,"""select candidate_id from core_discovery_candidates
                               where family_role='ACTIVE' and retired_at is null and forward_start_at is not null
                               order by family_key""").fetchall()]
  for cid in ids:
   try:paper(c,cid)
   except Exception as e:errors.append({'paper':cid,'error':type(e).__name__+':'+str(e)[:240]})

  family_result={'switches':0,'retired':0,'warnings':0}
  if holdout_result.get('state')=='EVALUATED':
   try:family_result=manage_families(c,run_id)
   except Exception as e:errors.append({'family_manage':type(e).__name__+':'+str(e)[:240]})

  counts=dict(read(c,'select status,count(*)::int from core_discovery_candidates group by status').fetchall())
  roles=dict(read(c,"select coalesce(family_role,'UNTRACKED'),count(*)::int from core_discovery_candidates group by coalesce(family_role,'UNTRACKED')").fetchall())
  fam=read(c,"select count(*)::int,count(*) filter(where active_candidate_id is not null)::int from core_discovery_family_state").fetchone()
  write(c,'update core_discovery_runs set finished_at=now(),candidates_evaluated=%s,candidates_saved=%s,qualified=%s,paper_collecting=%s,errors=%s::jsonb where run_id=%s',
        (evaluated,saved,len(passed),int(counts.get('PAPER_COLLECTING',0)),jd(errors),run_id))
  top=read(c,"""select candidate_id,family_key,family_role,family_rank,specificity,event_type,entry,horizon_min,status,
                     holdout_status,holdout_metrics,forward_start_at,paper_metrics,metrics->'oos'->>'n',
                     metrics->'oos'->>'robust_avg_net_pct',metrics->'oos'->>'pf'
              from core_discovery_candidates where family_role in ('ACTIVE','STANDBY') and retired_at is null
              order by family_key,family_rank limit 50""").fetchall()
  print('DISCOVERY_DONE '+jd({'run_id':run_id,'source_outcomes':source_n,'source_min_at':lo.isoformat(),'source_max_at':hi.isoformat(),
   'dev_end_at':dev_end.isoformat(),'split_at':split.isoformat(),'epoch':epoch,'validation_state':validation_state,
   'holdout':{'start':holdout_start.isoformat(),'end':holdout_end.isoformat(),'assigned':assigned,'result':holdout_result},
   'evaluated':evaluated,'saved':saved,'qualified':len(passed),'status_counts':counts,'role_counts':roles,
   'families':{'total':int(fam[0] or 0),'active':int(fam[1] or 0)},'family_result':family_result,'errors':errors[:10],'tracked':top}),flush=True)
if __name__=='__main__':main()
