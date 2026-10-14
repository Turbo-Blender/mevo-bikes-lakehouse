FROM apache/spark:3.5.6

USER root

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir \
    -r /tmp/requirements.txt

# S3A / MinIO
RUN curl -L \
    -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN curl -L \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Delta Lake (3.3.x is built for Spark 3.5 / Scala 2.12)
RUN curl -L \
    -o /opt/spark/jars/delta-spark_2.12-3.3.2.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar

RUN curl -L \
    -o /opt/spark/jars/delta-storage-3.3.2.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar

WORKDIR /opt/spark-apps

USER spark