# ----------------------------------------------------
# STAGE 1 - BUILDER (instala drivers do SQL Server)
# ----------------------------------------------------
FROM debian:bullseye-slim as builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        gnupg \
        ca-certificates \
        apt-transport-https \
        unixodbc \
        unixodbc-dev \
        build-essential && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Importa chave da Microsoft
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
    | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg

# Adiciona repositório oficial do SQL Server
RUN echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/11/prod bullseye main" \
    > /etc/apt/sources.list.d/mssql-release.list

# Instala drivers
RUN apt-get update && \
    ACCEPT_EULA=Y apt-get install -y \
        msodbcsql18 \
        mssql-tools18 && \
    rm -rf /var/lib/apt/lists/*


# ----------------------------------------------------
# STAGE 2 - AIRFLOW
# ----------------------------------------------------
FROM apache/airflow:2.9.3-python3.10

USER root

# Copia drivers do SQL Server
COPY --from=builder /usr/lib/ /usr/lib/
COPY --from=builder /etc/odbcinst.ini /etc/odbcinst.ini
COPY --from=builder /opt/mssql-tools18/ /opt/mssql-tools18/

ENV PATH="/opt/mssql-tools18/bin:${PATH}"

# Copia requirements antes de mudar para airflow
COPY requirements.txt /requirements.txt

# ✅ Troca para usuário airflow ANTES do pip install
USER airflow

# ✅ Agora instalamos com pip sem erro
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt" \
    apache-airflow-providers-microsoft-mssql \
    apache-airflow-providers-postgres \
    -r /requirements.txt
