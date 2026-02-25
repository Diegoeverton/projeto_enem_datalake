# Usa Ubuntu 22.04 LTS como imagem base
FROM ubuntu:22.04

# Prevenção de telas interativas durante a instalação (ex: fuso horário no apt-get)
ENV DEBIAN_FRONTEND=noninteractive

# Instala o Java (necessário para o Spark), Python 3 e pip
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Configura as variáveis de ambiente corretas do Java
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"


# Variáveis de ambiente (boas práticas)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Define o diretório de trabalho padrão
WORKDIR /app

# Copia e instala as dependências Python
COPY requirements.txt .

# No Ubuntu 22.04 (Python 3.10), as instalações globais via pip ainda são suportadas,
# então podemos rodar o pip normalmente sem a flag extra.
RUN pip3 install --no-cache-dir pyspark==3.5.1
RUN pip3 install --no-cache-dir -r requirements.txt

# Copia código
COPY src/ ./src/


# Cria o usuário não-root com UID fixo (1000) para evitar problemas de permissões
# com volumes montados no Linux (Permission Denied), pareando o Host e o Container.
RUN groupadd -g 1000 appgroup && \
    adduser --uid 1000 --gid 1000 --disabled-password --gecos "" appuser && \
    chown -R appuser:appgroup /app

USER appuser

# Comando padrão: Orquestra toda a esteira do Data Lake de extração a camada Prata
CMD ["bash", "src/run_pipeline.sh"]