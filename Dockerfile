FROM python:3.13.3-bullseye

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip3 install -r requirements.txt

ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
WORKDIR ${SPARK_HOME}

ENV SPARK_VERSION=3.5.5
ENV SPARK_MAJOR_VERSION=3.5
ENV ICEBERG_VERSION=1.8.1
ENV PATH="/root/.cargo/bin:${PATH}"

# Download Spark
RUN mkdir -p ${SPARK_HOME} \
 && curl https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
 && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Download Iceberg Spark runtime
RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar

 # Download AWS bundle
 RUN curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# Download DuckDB JDBC driver (for Spark Iceberg catalog)
RUN curl -s https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/1.1.0/duckdb_jdbc-1.1.0.jar -Lo /opt/spark/jars/duckdb_jdbc-1.1.0.jar

COPY spark-defaults.conf /opt/spark/conf

RUN mkdir -p /home/iceberg/spark-events

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"

COPY .pyiceberg.yaml /root/.pyiceberg.yaml

 # Add a jupyterlab command
RUN echo '#! /bin/sh' >> /bin/notebook \
 && echo 'export PYSPARK_DRIVER_PYTHON=jupyter-lab' >> /bin/notebook \
 && echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/iceberg/notebooks --ip='*' --LabApp.token='' --LabApp.password='' --port=8888 --no-browser --allow-root\"" >> /bin/notebook \
 && echo "pyspark" >> /bin/notebook \
 && chmod u+x /bin/notebook

# Setting IPython startup directory
RUN mkdir -p /root/.ipython/profile_default/startup
COPY ipython/startup/00-prettytables.py /root/.ipython/profile_default/startup
COPY ipython/startup/README /root/.ipython/profile_default/startup

COPY entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh"]
CMD ["notebook"]
