FROM apache/airflow:2.8.3-python3.10


USER root


RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    wget \
    unixodbc-dev \
    libpq-dev \
    git && \
    rm -rf /var/lib/apt/lists/*


RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg

RUN echo "deb [arch=amd64,armhf,arm64] https://packages.microsoft.com/debian/11/prod bullseye main" \
    > /etc/apt/sources.list.d/mssql-tools.list


RUN apt-get update -y && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends libssl1.1 || \
    (wget http://ftp.debian.org/debian/pool/main/o/openssl/libssl1.1_1.1.1w-0+deb11u1_amd64.deb -O libssl1.1.deb && \
    dpkg -i libssl1.1.deb) && \
    rm -rf /var/lib/apt/lists/*

USER airflow


RUN pip install --no-cache-dir \
    apache-airflow-providers-microsoft-mssql==3.8.0 \
    pyodbc==4.0.39

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt