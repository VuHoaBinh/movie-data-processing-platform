"""Reproducible isolated equal-budget batch measurements; raw CSV and full logs retained."""
import csv
import json
import shutil
import subprocess
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / 'benchmarks/tmp/report'
RESULT = ROOT / 'benchmarks/results/report'
COMPOSE = ['docker', 'compose', '-f', 'compose.yaml', '-f', 'benchmarks/compose.measure.yaml']

def prepare(scale):
    target = BASE / f'x{scale}'
    target.mkdir(parents=True, exist_ok=True)
    if (target / 'ready.json').exists():
        return target
    counts = {}
    for table, id_columns in [('movies', ()), ('users', ('user_id',)),
                               ('ratings', ('rating_id', 'user_id')), ('watch_history', ('watch_id', 'user_id'))]:
        with (target / f'{table}.csv').open('w', newline='') as output:
            writer = None
            count = 0
            for copy in range(1 if table == 'movies' else scale):
                with (ROOT / 'data' / f'{table}.csv').open(newline='') as source:
                    reader = csv.DictReader(source)
                    if writer is None:
                        writer = csv.DictWriter(output, fieldnames=reader.fieldnames)
                        writer.writeheader()
                    for row in reader:
                        if copy:
                            for column in id_columns:
                                row[column] = f'X{copy}_{row[column]}'
                        writer.writerow(row)
                        count += 1
            counts[table] = count
        print('data', scale, table, count, flush=True)
    (target / 'ready.json').write_text(json.dumps(counts, indent=2))
    return target

def monitor(name, stop, path):
    with path.open('w', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(['unix_time', 'cpu_percent', 'memory_mib'])
        while not stop.is_set():
            proc = subprocess.run(['docker', 'stats', '--no-stream', '--format', '{{json .}}', name], capture_output=True, text=True)
            for line in proc.stdout.splitlines():
                try:
                    d = json.loads(line)
                    used = d['MemUsage'].split('/')[0].strip()
                    units = {'GiB': 1024, 'MiB': 1, 'KiB': 1/1024, 'B': 1/1048576}
                    for suffix, factor in units.items():
                        if used.endswith(suffix):
                            memory = float(used[:-len(suffix)]) * factor
                            break
                    writer.writerow([time.time(), float(d['CPUPerc'].rstrip('%')), memory])
                    handle.flush()
                except (ValueError, KeyError):
                    pass
            stop.wait(.2)

def measure(engine, scale, run):
    label = f'{engine}_x{scale}_r{run}'
    name = 'movie-measure-' + engine
    target = f'/app/benchmarks/tmp/report/x{scale}'
    if engine == 'spark':
        output = BASE / f'x{scale}/spark-output'
        shutil.rmtree(output, ignore_errors=True)
        args = ['python', '-m', 'src.spark_batch_job', '--no-hive', '--raw-dir', target,
                '--replay-day', 'none', '--archive-dir', '/tmp/empty-archive',
                '--csv-output-dir', target + '/spark-output']
    else:
        warehouse = BASE / f'x{scale}' / 'warehouse'
        warehouse.mkdir(exist_ok=True)
        warehouse.chmod(0o777)
        args = ['/docker-entrypoint.sh', 'flink', 'run', '-t', 'local',
                '-Dparallelism.default=2', '-Dtaskmanager.numberOfTaskSlots=2',
                '-Dtaskmanager.memory.process.size=2048m', '-Djobmanager.memory.process.size=768m',
                '-Dtaskmanager.network.sort-shuffle.min-buffers=64',
                '-py', '/app/flink/flink_batch_sql_job.py', '--data-dir', target]
    stop = threading.Event()
    samples = RESULT / f'{label}_resources.csv'
    thread = threading.Thread(target=monitor, args=(name, stop, samples))
    thread.start()
    start = time.perf_counter()
    print('START', label, flush=True)
    try:
        with (RESULT / f'{label}.log').open('w') as log:
            proc = subprocess.run(COMPOSE + ['run', '--rm', '--no-deps', '--name', name, 'measure-' + engine] + args,
                                  cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, timeout=7200)
        code = proc.returncode
    except subprocess.TimeoutExpired:
        subprocess.run(['docker', 'stop', '-t', '0', name], capture_output=True)
        code = 124
    wall = time.perf_counter() - start
    stop.set()
    thread.join()
    with samples.open() as handle:
        stats = list(csv.DictReader(handle))
    row = dict(engine=engine, scale=scale, run=run, seconds=round(wall, 3), returncode=code,
               peak_mib=max((float(r['memory_mib']) for r in stats), default=0),
               peak_cpu_percent=max((float(r['cpu_percent']) for r in stats), default=0), samples=len(stats))
    path = RESULT / 'batch.csv'
    exists = path.exists()
    with path.open('a', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row))
        if not exists: writer.writeheader()
        writer.writerow(row)
    print('DONE', json.dumps(row), flush=True)
    if code: print("FAILED; retained log and continuing remaining trials", flush=True)
    return row

def main():
    RESULT.mkdir(parents=True, exist_ok=True)
    for scale in (1, 5, 10):
        prepare(scale)
        for run in (1, 2, 3):
            for engine in ('spark', 'flink'):
                path = RESULT / 'batch.csv'
                if path.exists():
                    with path.open() as handle:
                        if any(r['engine']==engine and int(r['scale'])==scale and int(r['run'])==run and r['returncode']=='0'
                               for r in csv.DictReader(handle)):
                            continue
                measure(engine, scale, run)

if __name__ == '__main__': main()
