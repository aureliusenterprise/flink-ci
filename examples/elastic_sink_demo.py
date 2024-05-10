from pathlib import Path

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.elasticsearch import (
    Elasticsearch7SinkBuilder,
    ElasticsearchEmitter,
    FlushBackoffType,
)

"""
requries to run with elastic7
in kibana in the dev tools perform the following command:

PUT foo
{
  "mappings": {
      "properties": {
        "id": { "type": "keyword" },
        "name": { "type": "keyword" }
      }
  }
}
"""


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()

    # write all the data to one file
    env.set_parallelism(1)

    # Add JARs to the classpath
    jars = [path.absolute().as_uri() for path in Path("./jars").glob("*.jar")]
    env.add_jars(*jars)

    ds = env.from_collection(
        [
            {
                "name": "ada",
                "id": "5",
            },
            {
                "name": "luna",
                "id": "6",
            },
            {
                "name": "una",
                "id": "7",
            },
            {
                "name": "luna",
                "id": "8",
            },
        ],
        type_info=Types.MAP(Types.STRING(), Types.STRING()),
    )

    es_sink = (
        Elasticsearch7SinkBuilder()
        .set_emitter(ElasticsearchEmitter.static_index("foo", "id"))
        .set_hosts(["localhost:9200"])
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .set_bulk_flush_max_actions(1)
        .set_bulk_flush_max_size_mb(2)
        .set_bulk_flush_interval(1000)
        .set_bulk_flush_backoff_strategy(FlushBackoffType.CONSTANT, 3, 3000)
        .set_connection_username("elastic")
        .set_connection_password("elasticpw")
        .set_connection_path_prefix("foo")
        .set_connection_request_timeout(30000)
        .set_connection_timeout(31000)
        .set_socket_timeout(32000)
        .build()
    )

    #     es_sink.get_java_function(), "buildBulkProcessorConfig")

    # j_network_client_config = get_field_value(es_sink.get_java_function(),
    #                                             "networkClientConfig")

    ds.sink_to(es_sink).name("es sink")

    env.execute("flink elestic connector")
