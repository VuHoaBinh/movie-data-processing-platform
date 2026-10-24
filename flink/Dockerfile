FROM flink:2.1.0-scala_2.12-java17

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip curl \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --no-cache-dir --break-system-packages apache-flink==2.1.0 \
    && curl -fsSL https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/5.0.0-2.1/flink-connector-kafka-5.0.0-2.1.jar \
       -o /opt/flink/lib/flink-connector-kafka-5.0.0-2.1.jar

ENV PYFLINK_CLIENT_EXECUTABLE=/usr/bin/python3
COPY flink/flink_trending_job.py /opt/flink/usrlib/flink_trending_job.py
