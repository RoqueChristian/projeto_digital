# Usa a versão mais recente do Airflow com Python 3.11 para melhor performance e segurança.
FROM apache/airflow:2.9.2-python3.11


USER root


RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    python3-dev \
    gnupg \
    wget \
    unixodbc-dev \
    libpq-dev \
    git && \ 
    rm -rf /var/lib/apt/lists/*


# A imagem base do Airflow com Python 3.11 usa Debian 12 (Bookworm), que tem um método mais direto para o driver ODBC.
RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg

RUN echo "deb [arch=amd64] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list


RUN apt-get update -y && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 && \
    rm -rf /var/lib/apt/lists/*

USER airflow


# Define a URL do arquivo de restrições para garantir builds consistentes e sem conflitos.
ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Instala os provedores usando o arquivo de restrições para garantir compatibilidade.
RUN pip install --no-cache-dir "apache-airflow-providers-microsoft-mssql[odbc]" --constraint "${CONSTRAINT_URL}"

# Instala as dependências do projeto, também usando as restrições.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt --constraint "${CONSTRAINT_URL}"