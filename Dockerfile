FROM quay.io/astronomer/astro-runtime:13.0.0

USER root

RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

RUN mkdir -p /tmp/s3a && chmod -R 777 /tmp/s3a
# Troque aqui para o usuário correto da imagem Astronomer, exemplo "astro"
USER astro

RUN pip install --no-cache-dir pyspark pytest
