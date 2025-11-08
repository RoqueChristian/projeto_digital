# 1. IMAGEM BASE (Airflow 2.x)
FROM apache/airflow:2.8.3-python3.11


USER root

# 2. INSTALAÇÃO DE DEPENDÊNCIAS DO SISTEMA
# unixodbc-dev: Necessário para o pyodbc funcionar
# libpq-dev: Necessário para compilar corretamente o psycopg2 (PostgreSQL)
# curl, git: Ferramentas úteis no container
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    unixodbc-dev \
    libpq-dev \
    curl \
    git && \
    rm -rf /var/lib/apt/lists/*

# 3. INSTALAÇÃO DO DRIVER ODBC PARA SQL SERVER
# Adiciona o repositório da Microsoft e instala o driver ODBC 18
RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg && \
    AZURE_REPO=$(grep -E '^(UBUNTU|DEBIAN)_CODENAME=' /etc/os-release | cut -d'=' -f2) && \
    echo "deb [arch=amd64] https://packages.microsoft.com/debian/${AZURE_REPO}/prod ${AZURE_REPO} main" > /etc/apt/sources.list.d/mssql-tools.list

RUN apt-get update -y && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
    msodbcsql18 && \
    rm -rf /var/lib/apt/lists/*

# --- INSTALAÇÃO DE DEPENDÊNCIAS PYTHON (USER airflow) ---

# 4. Voltar ao usuário airflow
USER airflow

# 5. COPIAR E INSTALAR DEPENDÊNCIAS PYTHON
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt