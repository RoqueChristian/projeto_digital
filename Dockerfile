FROM apache/airflow:2.8.3-python3.11

USER root

# 1. INSTALAR FERRAMENTAS ESSENCIAIS PARA DOWNLOADS E CHAVES
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    unixodbc-dev \
    libpq-dev \
    git && \
    rm -rf /var/lib/apt/lists/*

# 2. CONFIGURAR REPOSITÓRIO E CHAVE DA MICROSOFT PARA SQL SERVER
# Passo 2A: Importar a chave GPG da Microsoft
RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg

# Passo 2B: Configurar o repositório para Debian 11 (Bullseye)
RUN echo "deb [arch=amd64,armhf,arm64] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-tools.list

# 3. INSTALAR DRIVER ODBC 18 DO SQL SERVER
RUN apt-get update -y && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
    msodbcsql18 \
    libssl1.1 || apt-get install -y libssl3 && \
    rm -rf /var/lib/apt/lists/*

# --- INSTALAÇÃO DE DEPENDÊNCIAS PYTHON (USER airflow) ---

# Voltar ao usuário airflow
USER airflow

# 4. COPIAR E INSTALAR DEPENDÊNCIAS PYTHON
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt