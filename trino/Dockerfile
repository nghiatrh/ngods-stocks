# syntax=docker/dockerfile:1
FROM eclipse-temurin:24-jdk-noble

ENV TRINO_HOME=/opt/trino
ENV BIN_DIR=/usr/bin

ENV DATA_STAGE_DIR=/var/lib/ngods/stage
ENV TRINO_DATA_DIR=/var/lib/trino/data
ENV INSTALL_DIR=/tmp/install

RUN mkdir -p ${TRINO_HOME} ${TRINO_DATA_DIR} ${DATA_STAGE_DIR} ${INSTALL_DIR}

# install core packages (Java 24 already provided by base image)
RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      unzip \
      make \
      python3 \
      python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR ${INSTALL_DIR}

# Install python deps
COPY conf/requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt \
    && rm requirements.txt

RUN curl https://repo1.maven.org/maven2/io/trino/trino-server/476/trino-server-476.tar.gz -Lo trino-server-476.tar.gz \
    && tar xvzf trino-server-476.tar.gz --directory ${TRINO_HOME} --strip-components 1 \
    && rm trino-server-476.tar.gz

WORKDIR ${TRINO_HOME}

COPY conf/trino/ ${TRINO_HOME}/etc

RUN chmod u+x ${TRINO_HOME}/bin/* 

EXPOSE 8060
EXPOSE 8063

CMD ["/opt/trino/bin/launcher", "run"]