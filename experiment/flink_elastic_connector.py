from pathlib import Path

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.elasticsearch import (
    Elasticsearch7SinkBuilder,
    ElasticsearchEmitter,
)

if __name__ == "__main__":

    env = StreamExecutionEnvironment.get_execution_environment()

    # write all the data to one file
    env.set_parallelism(1)

    data = [
        {"name":"Tom", "value":1},
        {"name":"Tim", "value":2},
        {"name":"Jip", "value":3},
        {"name":"Sim", "value":4},
        {"name":"Sip", "value":5},
    ]

    # Add JARs to the classpath
    jars = [path.absolute().as_uri() for path in Path("./jars").glob("*.jar")]
    env.add_jars(*jars)

    row_type_info = Types.ROW_NAMED(["name", "value"], [Types.STRING(), Types.INT()])
    inputstream = env.from_collection(data, type_info=row_type_info)

    # The set_bulk_flush_max_actions instructs the sink to emit after every element,
    # otherwise they would be buffered
    es7_sink = (Elasticsearch7SinkBuilder() \
        .set_bulk_flush_max_actions(1)
        .set_emitter(ElasticsearchEmitter.static_index("flinktest", "name"))
        .set_hosts(["localhost:9200"])
        .set_connection_username("elastic")
        .set_connection_password("elasticpw")
        .build()
    )
    body_map_type = Types.MAP(Types.STRING(), Types.ROW([Types.STRING(), Types.INT()]))
    inputstream2 = inputstream.map(lambda x: x, body_map_type)
    inputstream2.sink_to(es7_sink).name("es7 sink")

    env.execute("flink elestic connector")
