FROM flink:2.1.0-scala_2.12-java17

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip curl \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --no-cache-dir apache-flink==2.1.0 \
    && curl -fsSL https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.1/flink-sql-connector-kafka-5.0.0-2.1.jar \
       -o /opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.1.jar \
    && curl -fsSL https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-parquet/2.1.0/flink-sql-parquet-2.1.0.jar \
       -o /opt/flink/lib/flink-sql-parquet-2.1.0.jar \
    && curl -fsSL https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar \
       -o /opt/flink/lib/hadoop-client-api-3.3.6.jar \
    && curl -fsSL https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar \
       -o /opt/flink/lib/hadoop-client-runtime-3.3.6.jar

ENV PYFLINK_CLIENT_EXECUTABLE=/usr/bin/python3
COPY flink/flink_speed_sql_job.py /opt/flink/usrlib/flink_speed_sql_job.py
COPY flink/flink_batch_sql_job.py /opt/flink/usrlib/flink_batch_sql_job.py
