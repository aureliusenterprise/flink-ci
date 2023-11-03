copied from Sem... needs adjustment

#aurelius/atlas/flink

## Current version

* one flink job which read from Kafka and write to Kafka.
* this job consists with one MapFunction (kafka topic -> kafka topic) AW: this does not fit to you other document. Please check and adjust to make it consistent!
* This MapFunction:
	1. read input
	2. transform and publish it directly to elastic (with hardcoded retries)
	3. get previous entity directly from elastic and put update input with it
	4. return updated input
	5. if any error directly produce to Kafka DeadLetterBox topic

## New version

* remove hardcoded configs
* move Elastic communication responsobilities for Flink (for index/publish) and special client for search
* remove ElasticClient type dependencies (for testing without elastic)
* move json parsing to Dataclasses with serialization and deserialization with wrappers
* Automated DeadLetterBox handling with wrapper, and split stream with fling tag for using flink to produce DeadLeater
* split jobs in two jobs for prevent repeating indexing because the errors :
	1. publish to elastic, use FlinkFunction output to index message to Elastic
	2. get previous entity from Elastic (via client)
* tests
## Specification

### Input

#### ENRICHED_ENTITIES kafka topic

json data from Kafka topic have a next format

    msg_creation_time: int
    event_time: int
    atlas_entity_audit: dict
    supertypes: list
    kafka_notification: Optional[dict] = None
    atlas_entity: Optional[dict] = None
    previouse_version: Optional[dict] = None     AW: is this already in the input? Are you sure?

### Output

#### ENRICHED_ENTITIES_SAVED  kafka topic

json data to Kafka topic have a next format

    msg_creation_time: int
    event_time: int
    atlas_entity_audit: dict
    supertypes: list
    kafka_notification: Optional[dict] = None
    atlas_entity: Optional[dict] = None
    previouse_version: Optional[dict] = None

json data to Kafka topic have a next format
#### deadletterbox kafka topic

	timestamp: int
    original_notification: str
    job: str
    description: str
    exception_class: str
    remark: str

### Process

#### 1. Check input

* if not follow the specification produce deadletterbox message
* if input has no kafka_notification produce deadletterbox message

#### 2. Process input
* if no altas entity atlas_entity, set previouse_version to None, produce ENRICHED_ENTITIES_SAVED and finish
* if no guid in input kafka_notification.entity make warn log
* index with doc_id = quid+msg_creation_time next document

		{"msgCreationTime": msg_creation_time, "eventTime": event_time, "body": atlas_entity }

* find in elastic document with same guid but indexed earlier and set previouse_version field with it
* produce updated input to ENRICHED_ENTITIES_SAVED and finish
* in case of any error produce deadletterbox message and finish
