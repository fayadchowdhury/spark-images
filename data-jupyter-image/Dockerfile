FROM quay.io/jupyter/all-spark-notebook:spark-4.0.0

USER root

ARG DELTA_VERSION=4.0.0
ARG SCALA_VERSION=2.13
ARG HADOOP_VERSION=3.4.1

RUN cd /usr/local/spark/jars && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.31.78/bundle-2.31.78.jar

RUN pip install --no-cache-dir \
    pyspark==4.0.0 \
    delta-spark==4.0.0 \
    boto3==1.28.0 \
    minio==7.1.15

RUN mkdir -p /usr/local/spark/conf

RUN echo "# Delta Lake Configuration" > /usr/local/spark/conf/spark-defaults.conf && \
    echo "spark.sql.extensions                     io.delta.sql.DeltaSparkSessionExtension" >> /usr/local/spark/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.spark_catalog          org.apache.spark.sql.delta.catalog.DeltaCatalog" >> /usr/local/spark/conf/spark-defaults.conf

RUN mkdir -p /usr/local/bin

COPY delta-lake-helper.py /usr/local/bin/delta-lake-helper.py
RUN chmod +x /usr/local/bin/delta-lake-helper.py

USER ${NB_UID}

ENV DELTA_VERSION=4.0.0
ENV SCALA_VERSION=2.13
ENV HADOOP_VERSION=3.4.1