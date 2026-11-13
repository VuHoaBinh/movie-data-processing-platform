"""Equal total budget (2 CPUs/4 GiB), fresh Kafka topics/checkpoints, three trials per offered load."""
import csv
import json
import subprocess
import threading
import time
from pathlib import Path
from measure_report import COMPOSE, RESULT, ROOT, monitor


def call(args, **kwargs):
    return subprocess.run(args, cwd=ROOT, check=True, **kwargs)


def trial(engine, rate, run):
    label = f'stream_{engine}_{rate}_r{run}'
    result_path = RESULT / f'{label}.json'
    if result_path.exists(): return json.loads(result_path.read_text())
    trial_stamp = int(time.time())
    tag = f'bm-{engine}-{rate}-{run}-{trial_stamp}'
    source, trend, users, movies = [tag+'-'+s for s in ('input','trend','users','movies')]
    create = ('from kafka.admin import KafkaAdminClient, NewTopic; '
              'a=KafkaAdminClient(bootstrap_servers="kafka:29092"); '
              f'a.create_topics([NewTopic(n,2,1) for n in {repr([source,trend,users,movies])}]); a.close()')
    call(COMPOSE+['run','--rm','--no-deps','measure-client','python','-c',create], stdout=subprocess.DEVNULL)
    name = f'movie-measure-stream-{engine}'
    env = dict(KAFKA_BOOTSTRAP_SERVERS='kafka:29092',KAFKA_WATCH_TOPIC=source,
               KAFKA_TREND_TOPIC=trend,KAFKA_USER_ENGAGEMENT_TOPIC=users,KAFKA_MOVIE_ENGAGEMENT_TOPIC=movies,
               CASSANDRA_HOST='cassandra',CASSANDRA_KEYSPACE='movie_benchmark',MATERIALIZER_GROUP=tag,
               TREND_WINDOW_SECONDS='300',WATERMARK_SECONDS='60',TRIGGER_SECONDS='5')
    eargs = [x for k,v in env.items() for x in ('-e',k+'='+v)]
    if engine == 'spark':
        args = ['python','-m','src.spark_speed_layer']
        extra = []
    else:
        args = ['/docker-entrypoint.sh','flink','run','-t','local',
                '-Dparallelism.default=2','-Dtaskmanager.numberOfTaskSlots=2',
                '-Dtaskmanager.memory.process.size=2048m','-Djobmanager.memory.process.size=768m',
                '-Dtaskmanager.network.sort-shuffle.min-buffers=64',
                '-py','/app/flink/flink_speed_sql_job.py']
        extra = []
        call(COMPOSE+['run','-d','--no-deps','--name',name+'-materializer']+eargs+['measure-materializer'],stdout=subprocess.DEVNULL)
    service = 'measure-flink-stream' if engine == 'flink' else 'measure-spark'
    call(COMPOSE+['run','-d','--no-deps','--name',name]+extra+eargs+[service]+args,stdout=subprocess.DEVNULL)
    stopped = threading.Event()
    threads = []
    for container in [name]+([name+'-materializer'] if engine=='flink' else []):
        thread=threading.Thread(target=monitor,args=(container,stopped,RESULT/f'{label}_{container}_resources.csv'))
        thread.start(); threads.append(thread)
    print('START',label,flush=True)
    try:
        # Allow connector initialisation and empty Kafka assignment before timing the offered workload.
        time.sleep(35)
        with (RESULT/f'{label}_client.log').open('w') as log:
            call(COMPOSE+['run','--rm','--no-deps','measure-client','python','/app/benchmarks/stream_report_client.py',
                         '--engine',engine,'--topic',source,'--rate',str(rate),'--run',str(run),
                         '--day-offset',str(trial_stamp % 30000),'--label',label],
                 stdout=log,stderr=subprocess.STDOUT,timeout=360)
        result=json.loads(result_path.read_text())
        print('DONE',json.dumps(result),flush=True)
    finally:
        stopped.set()
        for thread in threads: thread.join()
        for container in [name]+([name+'-materializer'] if engine=='flink' else []):
            with (RESULT/f'{label}_{container}.log').open('w') as log:
                subprocess.run(['docker','logs',container],stdout=log,stderr=subprocess.STDOUT)
            subprocess.run(['docker','rm','-f',container],stdout=subprocess.DEVNULL)
    return result


def main():
    RESULT.mkdir(parents=True,exist_ok=True)
    init = 'from src.cassandra_store import wait_for_cassandra,apply_schema; apply_schema(wait_for_cassandra(),keyspace="movie_benchmark")'
    call(COMPOSE+['run','--rm','--no-deps','measure-client','python','-c',init])
    for rate in (100, 1000, 5000):
        for run in (1,2,3):
            for engine in ('spark','flink'):
                trial(engine,rate,run)
    results = [json.loads(p.read_text()) for p in RESULT.glob('stream_*.json')]
    (RESULT/'stream_summary.json').write_text(json.dumps(results,indent=2))

if __name__=='__main__': main()
