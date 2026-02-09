# ========================================
# Base Image - Airflow 2.10.4
# ========================================
FROM apache/airflow:2.10.4-python3.11

# Set working directory
WORKDIR /opt/airflow

# ========================================
# Install System Dependencies
# ========================================
# Switch to root user
USER root

# Install system packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        vim \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# ====================================
# Install Python Packages
# ====================================
COPY --chown=airflow:0 requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt

# ====================================
# Environment Variables
# ====================================
ENV PYTHONPATH=/opt/airflow

# ====================================
# Create Directories
# ====================================
RUN mkdir -p /opt/airflow/logs \
    && mkdir -p /opt/airflow/dags \
    && mkdir -p /opt/airflow/plugins \
    && mkdir -p /opt/airflow/data \
    && mkdir -p /opt/airflow/movies_dbt 

# ====================================
# Metadata
# ====================================
LABEL description="Airflow 2.10.4 with dbt-snowflake for Movies Pipeline"
LABEL version="1.0"
LABEL python.version="3.11"
LABEL airflow.version="2.10.4"