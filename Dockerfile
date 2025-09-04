# Dockerfile

FROM python:3.11-slim

# Define o diretório de trabalho
WORKDIR /app

# Copia apenas o requirements.txt primeiro para aproveitar cache de camadas
COPY requirements.txt .

# Atualiza o sistema antes de instalar qualquer pacote
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends wget gnupg ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Instala dependências Python
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Instala o MongoDB Database Tools
RUN apt-get update && \
    wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | apt-key add - && \
    echo "deb [ arch=amd64 ] https://repo.mongodb.org/apt/debian buster/mongodb-org/4.4 main" | tee /etc/apt/sources.list.d/mongodb-org-4.4.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends mongodb-database-tools && \
    rm -rf /var/lib/apt/lists/*

# Copia o restante do código
COPY . .

# Copia configuração específica do Gunicorn
COPY gunicorn.conf.py /app/gunicorn.conf.py

# Comando default
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5005", "app:create_app()"]
