FROM apache/spark:3.4.1

USER root

# Install system dependencies for python packages and tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        python3-pip \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install Great Expectations, psycopg2, sqlalchemy, and pandas
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir \
        great-expectations==0.18.12 \
        psycopg2-binary==2.9.9 \
        sqlalchemy==1.4.49 \
        pandas==1.5.3

# Download and add PostgreSQL JDBC driver to Spark jars folder
# This makes postgres integration out-of-the-box for Spark dataframes
RUN curl -sS https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -o /opt/spark/jars/postgresql-42.6.0.jar

# Set working directory
WORKDIR /opt/spark

# We remain as root user to avoid permission issues with mounted local volumes (e.g. data/)
USER root
