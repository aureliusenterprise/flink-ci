# Remarks on the type system of pyflink

At the moment the capabilities are very limited, which makes it very difficult to work with standard connectors, which expect the type system to be used.
Have a look at some experiments in

```bash
flink_type_demo.py
```

My conclusion to encode json documents so far is to use non nested structures and all values are strings.
Thus, in case there is a nested document, create a new top level document, add a document ID, and extract all other
required fields also to be represented on top level. Keep the original message in an attribute msg and represent
the original message as a string.

This way all information is preserved and the required information for routing, searching, filtering etc. is available on top level.

# Remarks on elastiseach sink

The available Elasticsearch sink is only working on elastissearch version 7. It has been tested with elastisearch version 7.15.0.
It is essential that a lifecycle policy, and a index template is created.
The indexs template MUST NOT allow data streams. The current version of the sink will use elasticsearch data streams if possible,
however, they are not working in the tested version.
If elasticsearch data streams are disabled, then the integration works perfectly fine.
Be aware that in the index mapping it is possible to specify which type a specific attribute of the document has. Therefore,
it is possible to turn strings into integer or boolean.

An example of setting up an index pattern is provided in
```bash
queries/setup_index_patterns.es
```

This index pattern is also used by the following example
```bash
kafka_source_flink_sink_demo.py
```
In this example a Kafka consumer is the source of the data and a elasticsearch sink consumes the data.
You can feed in messages in the kafka topic ENRICHED_ENTITIES. An example json message is

```json
{"name":"anwo", "id":"15"}
```
It is important that the message being published has an attribute id and is a string.
It is also possible to publish nested documents like e.g.
```json
{"name":"anwo", "id":"25", "nested":[{"key": "hello", "value": 23}]}
```
The resulting document in elasticsearch is
```json
{
  "_index": "foo",
  "_type": "_doc",
  "_id": "25",
  "_version": 2,
  "_score": 1,
  "_source": {
    "msg": "{\"name\": \"anwo\", \"id\": \"25\", \"nested\": [{\"key\": \"hello\", \"value\": 23}]}",
    "id": "25"
  },
  "fields": {
    "msg": [
      "{\"name\": \"anwo\", \"id\": \"25\", \"nested\": [{\"key\": \"hello\", \"value\": 23}]}"
    ],
    "id": [
      25
    ]
  }
}
```
The original message is available as a string in field "msg", while the id has been made available on top level and as a field has been mapped to an integer.

The easier elastic sink demo is
```bash
elastic_sink_demo.py
```
which publishes a bunch of data to elasticsearch. Also here the same rules apply as explained above.
