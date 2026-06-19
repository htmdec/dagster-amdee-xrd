FROM python:3.12-slim
# Checkout and install dagster libraries needed to run the gRPC server
# exposing your repository to dagster-webserver and dagster-daemon, and to load the DagsterInstance
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN python -m pip install -U pip setuptools
RUN pip install --no-cache-dir \
    dagster \
    dagster-postgres \
    dagster-k8s \
    matplotlib \
    numpy \
    scipy \
    girder-client

# Add repository code
COPY src /app/src
COPY pyproject.toml /app/
WORKDIR /app
RUN pip install --no-cache-dir .
RUN mkdir -p /opt/dagster/dagster_home
