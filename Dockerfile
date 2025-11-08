# --- INSTALAÇÃO DE DRIVERS NATIVOS (USER root) ---

USER root

# 1. INSTALAR FERRAMENTAS ESSENCIAIS PARA DOWNLOADS E CHAVES
# curl e gnupg são necessários para os passos seguintes
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    unixodbc-dev \
    libpq-dev \
    git && \
    rm -rf /var/lib/apt/lists/*

# 2. CONFIGURAR REPOSITÓRIO E CHAVE DA MICROSOFT PARA SQL SERVER
# Vamos usar DEBIAN_VERSION (que é a base do Airflow) para simplificar a detecção
# Versões recentes do Airflow usam Debian Bookworm (12), mas vamos usar Bullseye (11) ou Buster (10) como base segura, que é o que a Microsoft espera.

# Passo 2A: Importar a chave GPG da Microsoft
RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg

# Passo 2B: Detectar o codinome da distribuição e configurar o repositório
# Usando Bullseye (Debian 11) como padrão, que é a base da maioria das imagens Airflow 2.x
RUN echo "deb [arch=amd64,armhf,arm64] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-tools.list

# 3. INSTALAR DRIVER ODBC 18 DO SQL SERVER
RUN apt-get update -y && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
    msodbcsql18 \
    # Incluindo libssl1.1 que pode ser necessário dependendo da base
    libssl1.1 || apt-get install -y libssl3 && \
    rm -rf /var/lib/apt/lists/*