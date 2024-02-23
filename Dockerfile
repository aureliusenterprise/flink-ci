ARG FLINK_VERSION=1.17-scala_2.12-java11
FROM flink:${FLINK_VERSION}

# install python3: it has updated Python to 3.9 in Debian 11 and so install Python 3.7 from source
# it currently only supports Python 3.6, 3.7 and 3.8 in PyFlink officially.

ARG PYTHON_VERSION=3.10.13

RUN apt-get update -y && \
    apt-get install -y curl && \
    apt-get install -y jq && \
    apt-get install -y ca-certificates && \
    apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev && \
    wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    tar -xvf Python-${PYTHON_VERSION}.tgz && \
    cd Python-${PYTHON_VERSION} && \
    ./configure --without-tests --enable-shared && \
    make -j6 && \
    make install && \
    ldconfig /usr/local/lib && \
    cd .. && rm -f Python-${PYTHON_VERSION}.tgz && rm -rf Python-${PYTHON_VERSION} && \
    ln -s /usr/local/bin/python3 /usr/local/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ./jars/manifest /opt/flink/jars/
COPY ./flink_jobs /opt/flink/py_libs/
COPY ./flink_tasks /opt/flink/py_libs/
COPY ./pyproject.toml /opt/flink/py_libs/
COPY ./poetry.lock /opt/flink/py_libs/

ENV POETRY_VIRTUALENVS_CREATE = false
RUN cd /opt/flink/py_libs && python -m pip install poetry && python -m poetry install

#USER flink

#RUN bash -e 'while read -r url; do [ -z "$url" ] && continue filename=$(basename "$url") if [ -e "$filename" ]; then echo "File jars/$filename already exists, skipping download." continue fi wget -P /opt/flink/jars "$url" done < /opt/flink/jars/manifest'
