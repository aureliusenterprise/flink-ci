from flink_jobs.job_runners.publish_state import GetPreviousEntity
from flink_jobs.flink_dataclasses import DeadLetterBoxMesage, KafkaNotification

from .test_plug_elastic_client import TestPlugElasticClient


def test_elastic_client_fail():
    """
    Test that GetPreviousEntity return dead letter if elastic client is faild
    """
    elastic_client = TestPlugElasticClient()
    elastic_client.set_fail(True)
    psp_func = GetPreviousEntity(elastic_client=elastic_client)

    kafka_notification = KafkaNotification(
        kafka_notification={"some": 1}, atlas_entity={"some": 1}, msg_creation_time=1, event_time=1, atlas_entity_audit={}, supertypes=[]
    )
    res = next(psp_func.process_element(kafka_notification.to_json()))

    assert res[0].tag_id == "dead_letter"
    dead_letter = DeadLetterBoxMesage.from_json(res[1])
    assert dead_letter.original_notification == kafka_notification.to_json()
    assert dead_letter.job == "publish_state"
    assert dead_letter.exception_class == "ElasticPreviouseStateRetrieveException"

def test_empty_prev_atlas_entity():
    """
    Test that GetPreviousEntity return empty previous_version if elastic_client fail on get_previous_atlas_entity
    """
    elastic_client = TestPlugElasticClient()
    elastic_client.set_fail(False)
    psp_func = GetPreviousEntity(elastic_client=elastic_client)

    elastic_client.set_result(
        method="index",
        method_kwargs={"entity_guid": "some_guid", "msg_creation_time": 12, "event_time": 1, "atlas_entity": {"some": 1}}, 
        method_result=True)
    kafka_notification = KafkaNotification(
        kafka_notification={"message": {"entity": {"guid": "some_guid"}}}, 
        atlas_entity={"some": 1}, msg_creation_time=12, event_time=1, atlas_entity_audit={}, supertypes=[]
    )

    correct_res = KafkaNotification(
        kafka_notification={"message": {"entity": {"guid": "some_guid"}}}, 
        atlas_entity={"some": 1}, msg_creation_time=12, event_time=1, atlas_entity_audit={}, supertypes=[],
        previous_version=None
    )
    res = next(psp_func.process_element(kafka_notification.to_json()))
    assert res[0].tag_id == "dead_letter"
    dead_letter = DeadLetterBoxMesage.from_json(res[1])
    assert dead_letter.original_notification == kafka_notification.to_json()
    assert dead_letter.job == "publish_state"
    assert dead_letter.exception_class == "ElasticSearchResultSchemeError"


def test_correct_result():
    """
    Test that GetPreviousEntity return correct result 
    """
    elastic_client = TestPlugElasticClient()
    elastic_client.set_fail(False)
    psp_func = GetPreviousEntity(elastic_client=elastic_client)

    elastic_client.set_result(
        method="get_previous_atlas_entity", 
        method_kwargs={"entity_guid": "some_guid", "msg_creation_time": 12},
        method_result={"some": "result"}
    )
    
    kafka_notification = KafkaNotification(
        kafka_notification={"message": {"entity": {"guid": "some_guid"}}}, 
        atlas_entity={"some": 1}, msg_creation_time=12, event_time=1, atlas_entity_audit={}, supertypes=[]
    )

    correct_res = KafkaNotification(
        kafka_notification={"message": {"entity": {"guid": "some_guid"}}}, 
        atlas_entity={"some": 1}, msg_creation_time=12, event_time=1, atlas_entity_audit={}, supertypes=[],
        previous_version={"some": "result"}
    )

    res = next(psp_func.process_element(kafka_notification.to_json()))
    assert res == correct_res.to_json()