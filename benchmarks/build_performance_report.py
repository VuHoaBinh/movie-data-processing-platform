"""Build report text only from retained successful measurements; refuse incomplete cohorts."""
import csv
import json
import statistics as st
from collections import defaultdict
from pathlib import Path

BASE = Path(__file__).resolve().parent/'results/report'

def main():
    groups=defaultdict(list)
    with (BASE/'batch.csv').open() as handle:
        for row in csv.DictReader(handle): groups[(row['engine'],int(row['scale']))].append(row)
    summaries=[]
    for engine in ('spark','flink'):
        for scale in (1,5,10):
            rows=groups[(engine,scale)]
            valid=[r for r in rows if r['returncode']=='0']
            if len(valid)!=3: raise ValueError(f'Need 3 successful trials: {engine} x{scale}, found {len(valid)}')
            times=[float(r['seconds']) for r in valid]
            summaries.append(dict(engine=engine,scale=scale,runs=3,mean_seconds=st.mean(times),std_seconds=st.stdev(times),
                                  peak_gib=max(float(r['peak_mib']) for r in valid)/1024,
                                  peak_cpu_percent=max(float(r['peak_cpu_percent']) for r in valid)))
    streams=[json.loads(p.read_text()) for p in BASE.glob('stream_*.json') if p.name!='stream_summary.json']
    latency={}; capacities={}
    for engine in ('spark','flink'):
        rows=[r for r in streams if r['engine']==engine and r['rate']==100]
        if len(rows)!=3 or not all(r['exact_counts'] and r['trending_exact'] for r in rows):
            raise ValueError(f'Incomplete or incorrect baseline stream results: {engine}')
        latency[engine]=dict(p50_ms=st.mean(r['p50_ms'] for r in rows),p95_ms=st.mean(r['p95_ms'] for r in rows),
                             sample_events=sum(r['latency_events'] for r in rows))
        candidates=[]
        for rate in sorted({r['rate'] for r in streams}):
            cohort=[r for r in streams if r['engine']==engine and r['rate']==rate]
            if len(cohort)==3 and all(r['stable'] for r in cohort):candidates.append(rate)
        capacities[engine]=max(candidates) if candidates else None
    (BASE/'summary.json').write_text(json.dumps(dict(batch=summaries,latency=latency,highest_tested_stable_rate=capacities),indent=2))
    def b(engine,scale):return next(r for r in summaries if r['engine']==engine and r['scale']==scale)
    batch_lines=[];stream_lines=[]
    for engine,label in [('spark','Spark'),('flink','Flink')]:
        cells=[f"${b(engine,s)['mean_seconds']:.2f} \\pm {b(engine,s)['std_seconds']:.2f}$" for s in (1,5,10)]
        peak=max(b(engine,s)['peak_gib'] for s in (1,5,10))
        batch_lines.append(label+' & '+' & '.join(cells)+f' & {peak:.2f} GiB '+r'\\'+ '\n'+r'\hline')
        stream_lines.append(('Spark Structured Streaming' if engine=='spark' else 'Flink SQL')+
                            f" & {latency[engine]['p50_ms']:.2f} ms & {latency[engine]['p95_ms']:.2f} ms & "+
                            (str(capacities[engine]) if capacities[engine] else 'Không xác định')+r' sự kiện/giây \\'+'\n'+r'\hline')
    observations=[]
    for engine,label in [('spark','Spark'),('flink','Flink')]:
        cohort=[r for r in streams if r['engine']==engine and r['rate']==5000]
        failed=[r for r in cohort if not r['stable']]
        if failed:
            counts=', '.join(f"{r['processed']}/{r['sent']}" for r in failed)
            observations.append(f"{label}: {len(failed)}/3 lượt ở mức 5.000 sự kiện/giây không đạt tiêu chí; "
                                f"số lượt xem đã ghi sau thời gian chờ ở các lượt không đạt là {counts}.")
            for r in failed:
                log=BASE/f"stream_{engine}_5000_r{r['run']}_movie-measure-stream-{engine}-materializer.log"
                if log.exists() and 'CRC mismatch' in log.read_text(errors='replace'):
                    observations.append('Trong ít nhất một lượt Flink không đạt, materializer dừng sau lỗi kết nối '
                                        'Cassandra có thông báo CRC mismatch; chưa xác định được nguyên nhân gốc.')
                    break
        else:
            observations.append(f'{label}: cả ba lượt ở mức 5.000 sự kiện/giây đạt tiêu chí trong phạm vi phép thử.')
    text=(text.replace('BATCH_ROWS','\n'.join(batch_lines)).replace('STREAM_ROWS','\n'.join(stream_lines))
          .replace('LOAD_OBSERVATIONS',' '.join(observations))
          .replace('CPU_PEAK_SPARK',f"{max(b('spark',s)['peak_cpu_percent'] for s in (1,5,10)):.2f}")
          .replace('CPU_PEAK_FLINK',f"{max(b('flink',s)['peak_cpu_percent'] for s in (1,5,10)):.2f}"))
    (BASE/'performance_report.tex').write_text(text)
    md='# Kết quả đo hiệu năng thực tế\n\n'
    md+='Batch: 3 lượt/tổ hợp, trung bình ± độ lệch chuẩn; ngân sách 2 CPU, 4 GiB/engine.\n\n'
    md+='| Engine | ×1 (s) | ×5 (s) | ×10 (s) | RAM cao nhất (GiB) | CPU cao nhất (%) |\n|---|---:|---:|---:|---:|---:|\n'
    for engine in ('spark','flink'):
        md+='| '+engine+' | '+' | '.join(f"{b(engine,s)['mean_seconds']:.2f} ± {b(engine,s)['std_seconds']:.2f}" for s in (1,5,10))
        md+=f" | {max(b(engine,s)['peak_gib'] for s in (1,5,10)):.2f} | {max(b(engine,s)['peak_cpu_percent'] for s in (1,5,10)):.2f} |\n"
    md+='\n| Engine | BR2 p50 (ms) | BR2 p95 (ms) | Sự kiện trong mẫu (3 lượt) | Tải cao nhất đạt tiêu chí (sự kiện/s) |\n|---|---:|---:|---:|---:|\n'
    for engine in ('spark','flink'):
        l=latency[engine];md+=f"| {engine} | {l['p50_ms']:.2f} | {l['p95_ms']:.2f} | {l['sample_events']} | {capacities[engine]} |\n"
    md+='\nPhương pháp, phạm vi và giới hạn diễn giải: xem [đoạn LaTeX](performance_report.tex). '
    md+='Số liệu gốc: [batch](batch.csv), [streaming](stream_summary.json), [đối chiếu đầu ra](batch_parity.csv).\n'
    md+='\nCác lượt thăm dò trước khi giới hạn rõ heap/native memory của Flink được lưu trong `pilot/`, không gộp vào thống kê chính thức.\n'
    (BASE/'PERFORMANCE_REPORT.md').write_text(md)
    print(md)

if __name__=='__main__':main()
