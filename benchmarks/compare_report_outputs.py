"""Bounded-memory, key-by-key verification of equal-budget batch CSV outputs."""
import csv
import json
import sqlite3
import tempfile
from pathlib import Path
from src.compare_batch_engines import KEYS, normalize, same


def compare(scale, table, keys, columns):
    directory=Path(f'/app/benchmarks/tmp/report/x{scale}')
    result=dict(scale=scale,table=table,spark_rows=0,flink_rows=0,missing=0,extra=0,mismatched=0,max_abs_diff=0.0)
    with tempfile.TemporaryDirectory() as temp:
        db=sqlite3.connect(str(Path(temp)/'rows.sqlite'))
        db.execute('PRAGMA journal_mode=OFF');db.execute('PRAGMA synchronous=OFF')
        db.execute('CREATE TABLE rows (k TEXT PRIMARY KEY, v TEXT NOT NULL)')
        with (directory/f'spark-output/{table}.csv').open(newline='') as source:
            # Spark CSV escapes embedded quotes/backslashes with a backslash by default.
            reader=csv.DictReader(source, escapechar='\\')
            if reader.fieldnames!=columns:
                raise ValueError((table, reader.fieldnames,columns))
            def encode():
                for row in reader:
                    result['spark_rows']+=1
                    yield json.dumps([normalize(row[k]) for k in keys]),json.dumps([normalize(row[c]) for c in columns])
            db.executemany('INSERT INTO rows VALUES (?,?)',encode())
        db.commit()
        files=sorted((directory/f'warehouse/flink/analytics/{table}').rglob('*'))
        for path in files:
            if not path.is_file() or path.name.startswith(('.','_')): continue
            with path.open(newline='') as source:
                for row in csv.DictReader(source,fieldnames=columns):
                    result['flink_rows']+=1
                    key=json.dumps([normalize(row[k]) for k in keys])
                    stored=db.execute('DELETE FROM rows WHERE k=? RETURNING v',(key,)).fetchone()
                    if stored is None:
                        result['extra']+=1
                        continue
                    left=json.loads(stored[0]);right=[normalize(row[c]) for c in columns]
                    if not all(same(a,b,1e-6) for a,b in zip(left,right)):
                        result['mismatched']+=1
                    for a,b in zip(left,right):
                        if isinstance(a,float) and isinstance(b,float):
                            result['max_abs_diff']=max(result['max_abs_diff'],abs(a-b))
        result['missing']=db.execute('SELECT COUNT(*) FROM rows').fetchone()[0]
        db.close()
    result['match']=result['spark_rows']==result['flink_rows'] and not any(result[k] for k in ('missing','extra','mismatched'))
    return result


def main():
    output=Path('/app/benchmarks/results/report/batch_parity.csv')
    with output.open('w',newline='') as handle:
        writer=None
        for scale in (1,5,10):
            schema_path=Path(f'/app/benchmarks/tmp/report/x{scale}/warehouse/flink/analytics/_schema.json')
            if not schema_path.exists(): continue
            schema=json.loads(schema_path.read_text())
            for table,keys in KEYS.items():
                if table=='quality_report': continue
                result=compare(scale,table,keys,schema[table])
                if writer is None:
                    writer=csv.DictWriter(handle,fieldnames=list(result));writer.writeheader()
                writer.writerow(result);handle.flush()
                print(json.dumps(result),flush=True)

if __name__=='__main__': main()
