ARG CONNECT_VERSION=latest
FROM confluentinc/cp-kafka-connect:${CONNECT_VERSION}

ARG CONNECT_ELASTICSEARCH_VERSION=latest
RUN confluent-hub install confluentinc/kafka-connect-elasticsearch:${CONNECT_ELASTICSEARCH_VERSION} --no-prompt
