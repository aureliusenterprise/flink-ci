FROM flink:1.17.0


# install python3 and pip3
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev
# install basic linux network tools
RUN apt-get install net-tools -y

# clean up packages
RUN rm -rf /var/lib/apt/lists/*

RUN mkdir -p /usr/local/python/3.10.13/bin/
RUN ln -s /usr/bin/python3.10 /usr/local/python/3.10.13/bin/python3.10

# install PyFlink
RUN pip3 install apache-flink==1.17.0

CMD []
