FROM python:3.9-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV MONGO_VERSION=6.0

RUN apt-get update && apt-get install -y \
    gnupg curl ca-certificates lsb-release wget \
    build-essential gcc g++ python3-dev libffi-dev libssl-dev \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Ajouter les clés MongoDB
RUN curl -fsSL https://pgp.mongodb.com/server-${MONGO_VERSION}.asc | gpg --dearmor -o /usr/share/keyrings/mongodb-server-${MONGO_VERSION}.gpg

# Ajouter les dépôts MongoDB
RUN echo "deb [ signed-by=/usr/share/keyrings/mongodb-server-${MONGO_VERSION}.gpg ] https://repo.mongodb.org/apt/debian $(lsb_release -sc)/mongodb-org/${MONGO_VERSION} main" \
    > /etc/apt/sources.list.d/mongodb-org-${MONGO_VERSION}.list

# Installer les outils
RUN apt-get update && apt-get install -y \
    mongodb-database-tools \
    mongodb-mongosh

COPY ./dump /dump
COPY restore_and_run.sh /restore_and_run.sh
RUN chmod +x /restore_and_run.sh

RUN pip install --no-cache-dir pandas pymongo polars

CMD ["/restore_and_run.sh"]
