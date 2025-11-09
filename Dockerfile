# Usa a versão mais recente do Airflow com Python 3.11
FROM apache/airflow:2.9.2-python3.11

# =========================================================================
# 1. INSTALAÇÃO DE DEPENDÊNCIAS DO SISTEMA (root)
# =========================================================================
USER root

# Instalação unificada de dependências do sistema e do driver ODBC 18.
# Usamos o comando 'set -ex' para garantir que o script pare em caso de erro.
RUN set -ex; \
    \
    # 1. Instalação de Ferramentas Essenciais e unixODBC
    apt-get update -y; \
    apt-get install -y --no-install-recommends \
        curl \
        build-essential \
        python3-dev \
        gnupg \
        wget \
        # CRÍTICO: Instala o Gerenciador de Drivers ODBC
        unixodbc-dev \
        libpq-dev \
        git; \
    \
    # 2. Configuração do Repositório da Microsoft (Para Debian 12/Bookworm)
    # Baixa e adiciona a chave GPG da Microsoft
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg; \
    \
    # Adiciona a lista de repositórios para o Driver ODBC 18 (Debian 12/Bookworm)
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list; \
    \
    # 3. Instalação do Driver ODBC 18
    apt-get update -y; \
    # CRÍTICO: ACEITA O EULA e instala o driver ODBC 18
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql18 \
        mssql-tools; \
    \
    # Limpeza final
    apt-get autoremove -y; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*;

# =========================================================================
# 2. INSTALAÇÃO DE PROVIDERS PYTHON (airflow user)
# =========================================================================
USER airflow

ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir \
    "apache-airflow-providers-microsoft-mssql" \
    "pyodbc" \
    --constraint "${CONSTRAINT_URL}"
    

ENTRYPOINT ["/entrypoint"]
CMD ["--help"]