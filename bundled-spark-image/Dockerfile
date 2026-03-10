FROM apache/spark:4.0.0

USER root

RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.13 python3.13-dev python3.13-venv curl wget && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.13 1 && \
    update-alternatives --set python3 /usr/bin/python3.13 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN python3 --version && \
    curl https://bootstrap.pypa.io/get-pip.py | python3

RUN pip install --no-cache-dir \
    delta-spark==4.0.0 \
    numpy \
    pandas

ENV DELTA_VERSION=4.0.0 \
    HADOOP_VERSION=3.4.1 \
    SCALA_VERSION=2.13

# Use wget instead of ADD, then explicitly fix permissions
RUN wget -q -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar && \
    wget -q -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar && \
    wget -q -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    wget -q -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.31.78/bundle-2.31.78.jar && \
    chmod 644 /opt/spark/jars/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
    /opt/spark/jars/delta-storage-${DELTA_VERSION}.jar \
    /opt/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar \
    /opt/spark/jars/bundle-2.31.78.jar && \
    chown 185:185 /opt/spark/jars/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
    /opt/spark/jars/delta-storage-${DELTA_VERSION}.jar \
    /opt/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar \
    /opt/spark/jars/bundle-2.31.78.jar

USER 185