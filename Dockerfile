# ========================================
# Choose Base Image
# ========================================
FROM apache/airflow:3.1.6-python3.11

# Set working directory
WORKDIR /opt/airflow

# ========================================
# Install System Dependencies
# ========================================
# switch to the root user when running
USER root
# Install system packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        vim \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# switch back to the airflow user after installation is complete
USER airflow


# ====================================
#  Install Python Packages
# ====================================
COPY --chown=airflow:0 requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt

# ====================================
# ENVIRONMENT VARIABLES
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
# Add Metadata
# ====================================
LABEL description="Airflow Python 3.11 with dbt-snowflake for Movies Pipeline"
LABEL version="1.0"
LABEL python.version="3.11"
LABEL airflow.version="3.1.6"