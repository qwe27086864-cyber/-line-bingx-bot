#!/usr/bin/env python3
"""Phase-8 read-only readiness check. NEVER places orders or changes existing data."""
import argparse,csv,hashlib,json,os,socket,zipfile
from pathlib import Path

REQUIRED={'time','symbol','open','high','low','close','volume','spread_pct'}

def inspect_zip(path):
    with zipfile.ZipFile(path) as z:
        bad=z.testzip()
        names=set(z.namelist())
    required={'v42_original_signal_bridge.py','v42_run_offline_phase5.py','v42_download_bingx_history.py','v42_validate_history.py'}
    return {'valid_zip':bad is None,'files':len(names),'missing_components':sorted(required-names)}

def inspect_csv(path):
    with open(path,encoding='utf-8-sig',newline='') as f:
        r=csv.DictReader(f); headers=set(r.fieldnames or []); count=sum(1 for _ in r)
    return {'file':str(path),'rows':count,'fields':sorted(headers),'missing_for_signal_bridge':sorted(REQUIRED-headers),
            'sha256':hashlib.sha256(Path(path).read_bytes()).hexdigest()}

def readiness(zip_path,folder):
    data={'stage':'phase8','status':'NOT_READY_FOR_VERIFIED_HISTORICAL_PERFORMANCE',
          'phase7_package':inspect_zip(zip_path), 'history_inputs':[],
          'historical_spread_required':True,'historical_spread_must_be_observed':True,
          'source_original_app_present':False,'source_original_scanner_present':False,
          'blocking_issues':[]}
    folder=Path(folder)
    for path in sorted(folder.glob('*.csv')):data['history_inputs'].append(inspect_csv(path))
    if not data['history_inputs']:data['blocking_issues'].append('No historical CSV files in input folder')
    if any(x['missing_for_signal_bridge'] for x in data['history_inputs']):
        data['blocking_issues'].append('OHLCV files do not include observed historical spread_pct; phase-7 downloader output cannot be used directly by the strict bridge')
    data['source_original_app_present']=(folder/'original_app.py').is_file()
    data['source_original_scanner_present']=(folder/'original_scanner_job.py').is_file()
    if not (data['source_original_app_present'] and data['source_original_scanner_present']):
        data['blocking_issues'].append('Pinned original V4.2R1 source files are not packaged in the phase-7 archive')
    if data['phase7_package']['missing_components']:data['blocking_issues'].append('Missing phase-7 components')
    data['ready']=not data['blocking_issues']
    return data

if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('--package',required=True);ap.add_argument('--data',required=True);ap.add_argument('--out',required=True)
    a=ap.parse_args();result=readiness(a.package,a.data)
    Path(a.out).write_text(json.dumps(result,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    print(json.dumps(result,indent=2,ensure_ascii=False))
