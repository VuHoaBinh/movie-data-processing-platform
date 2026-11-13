"""Wait for the batch matrix, then measure streams, verify outputs, write the report, restore demo services."""
import argparse
import csv
import json
import os
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from measure_report import COMPOSE, RESULT, ROOT


def main():
    p=argparse.ArgumentParser();p.add_argument('--batch-pid',type=int,required=True);args=p.parse_args()
    print('Waiting for batch PID',args.batch_pid,flush=True)
    while True:
        try:os.kill(args.batch_pid,0)
        except ProcessLookupError:break
        time.sleep(2)
    print('Batch process finished; checking the complete matrix',flush=True)
    try:
        with (RESULT/'batch.csv').open() as handle:
            counts=Counter((r['engine'],int(r['scale'])) for r in csv.DictReader(handle) if r['returncode']=='0')
        expected={(e,s) for e in ('spark','flink') for s in (1,5,10)}
        if set(counts)!=expected or any(n!=3 for n in counts.values()):
            raise ValueError(f'Batch matrix incomplete: {counts}')
        subprocess.run([sys.executable,'-u','benchmarks/measure_stream_report.py'],cwd=ROOT,check=True)
        subprocess.run(COMPOSE+['run','--rm','--no-deps','measure-client','python','/app/benchmarks/compare_report_outputs.py'],cwd=ROOT,check=True)
        with (RESULT/'batch_parity.csv').open() as handle:
            parity=list(csv.DictReader(handle))
        if len(parity)!=42 or not all(r['match']=='True' for r in parity):
            raise ValueError('Batch output parity failed; see batch_parity.csv')
        subprocess.run(COMPOSE+['run','--rm','--no-deps','measure-client','python','/app/benchmarks/test_benchmarks.py'],cwd=ROOT,check=True)
        subprocess.run([sys.executable,'benchmarks/build_performance_report.py'],cwd=ROOT,check=True)
        (RESULT/'completed.json').write_text(json.dumps({'completed':True,'unix_time':time.time()}))
    finally:
        print('Restoring original demo services without deleting data or checkpoints',flush=True)
        with (RESULT/'restore.log').open('w') as log:
            subprocess.run(['docker','compose','start','flink-jobmanager','flink-taskmanager','speed-materializer',
                            'flink-speed-job','spark-streaming','stream-archiver','event-producer'],
                           cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,check=True)
        print('Demo services started',flush=True)

if __name__=='__main__':main()
