FROM flink:1.17.0

# install python3 and pip3
RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# install PyFlink
RUN pip3 install apache-flink==1.17.0

# install Kafka python
RUN pip install kafka-python==2.0.2

# install flink_jobs lib
COPY flink_jobs ./flink_jobs
RUN cd ./flink_jobs && pip install -e .

CMD []
