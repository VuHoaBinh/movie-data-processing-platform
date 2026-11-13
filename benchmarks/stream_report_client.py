"""Controlled stream load and Cassandra observation, run inside the application image."""
import argparse
import csv
import json
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from kafka import KafkaProducer
from cassandra.cluster import Cluster


def percentile(values, q):
    values = sorted(values)
    if not values: return None
    p = (len(values)-1)*q
    a = int(p)
    return values[a] + (values[min(a+1, len(values)-1)]-values[a])*(p-a)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--engine', required=True)
    p.add_argument('--topic', required=True)
    p.add_argument('--rate', type=int, required=True)
    p.add_argument('--run', type=int, required=True)
    p.add_argument('--day-offset', type=int, default=None)
    p.add_argument('--duration', type=int, default=60)
    p.add_argument('--label', required=True)
    args = p.parse_args()
    out = Path('/app/benchmarks/results/report')
    cluster = Cluster(['cassandra'])
    session = cluster.connect('movie_benchmark')
    producer = KafkaProducer(bootstrap_servers='kafka:29092', value_serializer=lambda d: json.dumps(d).encode(),
                             acks='all', linger_ms=5)
    suffix = '_spark' if args.engine == 'spark' else ''
    day_offset = args.day_offset if args.day_offset is not None else args.run*10 + args.rate//100
    day = datetime(2030, 1, 1, tzinfo=timezone.utc) + timedelta(days=day_offset)
    base = int(day.timestamp()*1000)
    running = threading.Event()
    running.set()
    sent = [0]
    history, observations, failures = [], {}, []
    delivery_errors = []
    started = time.monotonic()
    def observe():
        while running.is_set():
            t = time.monotonic() - started
            try:
                rows = list(session.execute(f'SELECT views FROM live_movie_engagement{suffix} WHERE day=%s', [day.date()]))
                done = sum(r.views for r in rows)
                history.append({'elapsed': round(t,3), 'sent': sent[0], 'processed': done})
                for r in session.execute(f'SELECT window_start, rank, movie_id, live_views, unique_viewers, last_sent_at, updated_at FROM live_trending{suffix} WHERE day=%s', [day.date()]):
                    if r.last_sent_at and r.updated_at:
                        key = (str(r.window_start), r.rank)
                        version = (r.updated_at.isoformat(), r.movie_id, r.live_views, r.unique_viewers)
                        if key in observations and observations[key]['version'] == version:
                            continue
                        observations[key] = dict(window_start=str(r.window_start), rank=r.rank, movie_id=r.movie_id,
                                                 views=r.live_views, unique_viewers=r.unique_viewers, version=version, processed_at_ms=int(time.time()*1000),
                                                 latency_ms=(r.updated_at-r.last_sent_at).total_seconds()*1000)
            except Exception as error:
                failures.append(str(error))
            time.sleep(1)
    thread = threading.Thread(target=observe)
    thread.start()
    count = args.rate * args.duration
    expected = defaultdict(int)
    expected_windows = defaultdict(lambda: defaultdict(lambda: [0, set()]))
    sent_events = defaultdict(list)
    for i in range(count):
        due = started + i/args.rate
        delay = due-time.monotonic()
        if delay > 0: time.sleep(delay)
        # A simulated minute per real second, with five-minute windows and a one-minute watermark.
        event_ms = base + int(i/args.rate*60*1000)
        movie = f'BM{i%100:04}'
        expected[movie] += 1
        window_ms = (event_ms // 300000) * 300000
        entry = expected_windows[window_ms][movie]
        entry[0] += 1
        entry[1].add(f'BU{i%1000:05}')
        event = dict(event_type='watch', watch_id=f'{args.label}-{i}', user_id=f'BU{i%1000:05}',
                     movie_id=movie, title=f'Benchmark Movie {i%100:04}', genre='Drama',
                     subscription_plan='Basic', device='Web', watch_duration=30, movie_duration=120,
                     watch_timestamp=datetime.fromtimestamp(event_ms/1000, timezone.utc).isoformat(),
                     event_time_ms=event_ms, sent_at_ms=int(time.time()*1000))
        window = str(datetime.fromtimestamp(window_ms/1000, timezone.utc).replace(tzinfo=None))
        sent_events[(window, movie)].append((event['watch_id'], event['sent_at_ms']))
        producer.send(args.topic, event, partition=i%2).add_errback(lambda error: delivery_errors.append(str(error)))
        # Mirror watermarks on both input partitions; records remain ordered in their partition.
        if i % max(1, args.rate//2) == 0:
            hb = dict(event_type='heartbeat', event_time_ms=event_ms, sent_at_ms=int(time.time()*1000))
            for partition in (0, 1): producer.send(args.topic, hb, partition=partition)
        sent[0] = i+1
    producer.flush()
    send_seconds = time.monotonic()-started
    drain_started = time.monotonic()
    final_rows = []
    for k in range(120):
        event_ms = base + (args.duration*60+120+k*60)*1000
        hb = dict(event_type='heartbeat', event_time_ms=event_ms, sent_at_ms=int(time.time()*1000))
        for partition in (0, 1): producer.send(args.topic, hb, partition=partition)
        producer.flush()
        final_rows = list(session.execute(f'SELECT movie_id, views FROM live_movie_engagement{suffix} WHERE day=%s', [day.date()]))
        total = sum(r.views for r in final_rows)
        trends = list(session.execute(f'SELECT window_start, rank FROM live_trending{suffix} WHERE day=%s', [day.date()]))
        if total == count and len(trends) == (args.duration//5)*10 and k >= 15:
            break
        time.sleep(1)
    # Allow the observer to sample the last completed writes before stopping it.
    time.sleep(2)
    running.clear()
    thread.join()
    drain_seconds = time.monotonic()-drain_started
    rows = {r.movie_id:r.views for r in final_rows}
    matched = rows == dict(expected)
    expected_trends = {}
    for window_ms, entries in expected_windows.items():
        window = str(datetime.fromtimestamp(window_ms/1000, timezone.utc).replace(tzinfo=None))
        ranked = sorted(entries, key=lambda movie: (-entries[movie][0], -len(entries[movie][1]), movie))[:10]
        for rank, movie in enumerate(ranked, 1):
            expected_trends[(window, rank)] = (movie, entries[movie][0], len(entries[movie][1]))
    actual_trends = {key: (row['movie_id'], row['views'], row['unique_viewers']) for key, row in observations.items()}
    trends_matched = expected_trends == actual_trends
    row_latency = [r['latency_ms'] for r in observations.values()]
    event_samples = []
    for row in observations.values():
        for watch_id, sent_ms in sent_events[(row['window_start'], row['movie_id'])]:
            event_samples.append(dict(watch_id=watch_id, window_start=row['window_start'],movie_id=row['movie_id'],
                                      sent_at_ms=sent_ms, processed_at_ms=row['processed_at_ms'],
                                      latency_ms=row['processed_at_ms']-sent_ms))
    latency = [row['latency_ms'] for row in event_samples]
    active = [r for r in history if 15 <= r['elapsed'] <= send_seconds]
    tail = [r for r in active if r['elapsed'] >= send_seconds-15]
    head = [r for r in active if r['elapsed'] <= 30]
    def lag_s(seq):
        return sum(max(0, r['sent']-r['processed'])/args.rate for r in seq)/len(seq) if seq else None
    head_lag, tail_lag = lag_s(head), lag_s(tail)
    load_reached = count/send_seconds >= args.rate*.95
    stable = (matched and trends_matched and load_reached and not delivery_errors and not failures
              and head_lag is not None and tail_lag is not None and tail_lag <= 10 and tail_lag <= head_lag+3)
    result = dict(engine=args.engine, rate=args.rate, run=args.run, duration=args.duration, topic=args.topic, day=str(day.date()),
                  sent=count, processed=sum(rows.values()), send_seconds=send_seconds,
                  produced_per_second=count/send_seconds, drain_seconds=drain_seconds,
                  exact_counts=matched, trending_exact=trends_matched, trending_rows=len(observations), p50_ms=percentile(latency,.5),
                  p95_ms=percentile(latency,.95), head_lag_seconds=head_lag, tail_lag_seconds=tail_lag,
                  latency_events=len(event_samples), row_p50_ms=percentile(row_latency,.5), row_p95_ms=percentile(row_latency,.95),
                  load_reached=load_reached, stable=stable, delivery_errors=delivery_errors[:5], observation_errors=failures[:5])
    for name, records in [('throughput',history), ('latency',list(observations.values())),('event_latency',event_samples)]:
        if records:
            with (out/f'{args.label}_{name}.csv').open('w',newline='') as handle:
                writer=csv.DictWriter(handle, fieldnames=list(records[0]))
                writer.writeheader(); writer.writerows(records)
    (out/f'{args.label}.json').write_text(json.dumps(result,indent=2))
    print(json.dumps(result),flush=True)
    producer.close(); cluster.shutdown()

if __name__=='__main__': main()
