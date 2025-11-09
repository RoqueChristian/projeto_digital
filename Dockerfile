# Usa a versão mais recente do Airflow com Python 3.11
FROM apache/airflow:2.9.2-python3.11

# =========================================================================
# 1. INSTALAÇÃO DE DEPENDÊNCIAS DO SISTEMA (root)
# =========================================================================
USER root

# Atualiza e instala ferramentas essenciais
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

# ----------------------------------------------------
# INSTALAÇÃO DO DRIVER MS ODBC 18 (SQL Server)
# ----------------------------------------------------
# A imagem base do Airflow com Python 3.11 usa Debian 12 (Bookworm)
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg

RUN echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list

# Instala o driver ODBC 18
RUN apt-get update -y && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 && \
    rm -rf /var/lib/apt/lists/*

# =========================================================================
# 2. INSTALAÇÃO DE PROVIDERS PYTHON (airflow user)
# =========================================================================
USER airflow

# Define a URL do arquivo de restrições para garantir builds consistentes
ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# CRÍTICO: Instala o provedor MSSQL E o pyodbc na mesma linha, usando as constraints.
# Isso garante que o Python encontre o driver de sistema msodbcsql18.
RUN pip install --no-cache-dir \
    "apache-airflow-providers-microsoft-mssql" \
    "pyodbc" \
    --constraint "${CONSTRAINT_URL}"
    
# Se você tiver outras dependências (como pandas, numpy), você pode reinstalar:
# Exemplo:
# RUN pip install --no-cache-dir pandas --constraint "${CONSTRAINT_URL}"

# Removi o uso do requirements.txt para simplificar e focar no erro de MSSQL/pyodbc.

# Garante que o usuário airflow tenha as permissões corretas
ENTRYPOINT ["/entrypoint"]
CMD ["--help"]