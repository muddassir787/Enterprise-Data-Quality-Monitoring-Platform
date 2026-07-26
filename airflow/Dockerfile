FROM apache/airflow:2.6.3-python3.10

USER root

# Install OpenJDK-17, wget, procps and other build tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        wget \
        procps \
        curl \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME dynamically if needed, or point to standard Debian path
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${PATH}:${JAVA_HOME}/bin"

# Install Apache Spark Client (v3.4.1) for spark-submit capabilities
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV PATH="${PATH}:${SPARK_HOME}/bin"

# Switch back to airflow user to install python libraries
USER airflow

# Copy requirements file and install dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt
