#!/usr/bin/env python3
"""Fetch two pinned public GitHub source files; never execute them.
Uses GitHub raw URLs only. No tokens, private keys or exchange connections.
"""
import argparse, hashlib, json, urllib.request
from pathlib import Path

OWNER='qwe27086864-cyber'
REPO='-line-bingx-bot'
COMMIT='0169359859f4126f1d54b842ef4d44d9c6150aaa'
FILES={'app.py':'original_app.py','scanner_job.py':'original_scanner_job.py'}

def retrieve(destination, opener=None):
    opener=opener or urllib.request.urlopen
    destination=Path(destination); destination.mkdir(parents=True,exist_ok=True)
    manifest={'repository':f'{OWNER}/{REPO}','commit':COMMIT,'files':{}}
    for original,output in FILES.items():
        url=f'https://raw.githubusercontent.com/{OWNER}/{REPO}/{COMMIT}/{original}'
        req=urllib.request.Request(url,headers={'User-Agent':'V42HistoricalBacktest/1.0'})
        with opener(req,timeout=30) as response: data=response.read()
        if len(data)<1000 or not data.startswith(b'from flask' if original=='app.py' else b'import time'):
            raise ValueError(f'Invalid downloaded source: {original}')
        data.decode('utf-8')
        (destination/output).write_bytes(data)
        manifest['files'][output]={'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data),'url':url}
    (destination/'source_manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
    return manifest

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',default='v42_original_sources');args=p.parse_args()
    try:
        result=retrieve(args.out)
    except Exception as exc:
        p.exit(1,f'Download failed: {exc}\n')
    print(json.dumps(result,indent=2))
