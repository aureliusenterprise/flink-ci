ARG FLINK_VERSION=1.17-scala_2.12-java11
FROM flink:${FLINK_VERSION}

RUN apt-get update -y && \
    apt-get install -y curl && \
    apt-get install -y jq && \
    apt-get install -y ca-certificates && \
    apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev && \
    apt-get install -y liblzma-dev lzma && \
    apt-get install -y git && \
    apt-get install -y vim && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ARG PYTHON_VERSION=3.10.13

RUN wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    tar -xvf Python-${PYTHON_VERSION}.tgz && \
    cd Python-${PYTHON_VERSION} && \
    ./configure --without-tests --enable-shared && \
    make -j6 && \
    make install && \
    ldconfig /usr/local/lib && \
    cd .. && rm -f Python-${PYTHON_VERSION}.tgz && rm -rf Python-${PYTHON_VERSION} && \
    ln -s /usr/local/bin/python3 /usr/local/bin/python

COPY ./docker/prod/download_jars.sh /opt/flink/
COPY ./jars/manifest /opt/flink/jars/
COPY ./flink_jobs /opt/flink/py_libs/flink_jobs
COPY ./dist/*.whl /opt/flink/py_libs/

RUN python -m pip install --no-cache-dir /opt/flink/py_libs/*.whl && \
    rm -rf /opt/flink/py_libs/*.whl && \
    chmod 777 /opt/flink/download_jars.sh && \
    chown flink:flink -R /opt/flink

USER flink

RUN /opt/flink/download_jars.sh
