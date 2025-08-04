FROM python:3.12-slim
# Checkout and install dagster libraries needed to run the gRPC server
# exposing your repository to dagster-webserver and dagster-daemon, and to load the DagsterInstance
ENV DAGSTER_VERSION=1.8.12 DAGSTER_LIBS_VERSION=0.24.12
# Run dagster gRPC server on port 4000
EXPOSE 4000

RUN pip install --no-cache-dir \
    dagster==${DAGSTER_VERSION} \
    dagster-postgres==${DAGSTER_LIBS_VERSION} \
    dagster-docker==${DAGSTER_LIBS_VERSION} \
    matplotlib \
    numpy \
    scipy \
    girder-client

# Add repository code
COPY dagster_cloud.yaml /app/
COPY src /app/src
COPY pyproject.toml /app/
WORKDIR /app
RUN pip install --no-cache-dir .

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "amdee_xrd"]
