from flink_jobs.flink_dataclasses import DeadLetterBoxMesage, KafkaNotification
from flink_jobs.job_runners.publish_state import ValidateKafkaNotifications


def test_no_kafka_niotificaiton():
    """Test that VerifyKafkaNotification fail if provide None kafka_notification field for KafkaNotification."""
    psp_func = ValidateKafkaNotifications()

    kafka_notification = KafkaNotification(
        kafka_notification=None, atlas_entity=None, msg_creation_time=1, event_time=1, atlas_entity_audit={}, supertypes=[],
    )
    res = next(psp_func.process_element(kafka_notification.to_json(), None))

    assert res[0].tag_id == "dead_letter"
    dead_letter = DeadLetterBoxMesage.from_json(res[1])
    assert dead_letter.original_notification == kafka_notification.to_json()
    assert dead_letter.job == "publish_state"
    assert dead_letter.exception_class == "EventParsingException"

def test_no_guid_error():
    """Test that VerifyKafkaNotification fail if provide None kafka_notification field for KafkaNotification."""
    psp_func = ValidateKafkaNotifications()

    kafka_notification = KafkaNotification(
        kafka_notification={"some": 1}, atlas_entity=None, msg_creation_time=1, event_time=1, atlas_entity_audit={}, supertypes=[],
    )
    res = next(psp_func.process_element(kafka_notification.to_json(), None))

    assert res[0].tag_id == "dead_letter"
    dead_letter = DeadLetterBoxMesage.from_json(res[1])
    assert dead_letter.original_notification == kafka_notification.to_json()
    assert dead_letter.job == "publish_state"
    assert dead_letter.exception_class == "NoGUIDInKafkaNotification"

def test_not_atlas_entity_case():
    """Test that VerifyKafkaNotification return correct result if atlas entity is empty."""
    psp_func = ValidateKafkaNotifications()

    kafka_notification = KafkaNotification(
        kafka_notification={"message": {"entity": {"guid": "correct_guid"}}}, atlas_entity=None, msg_creation_time=1, event_time=1, atlas_entity_audit={}, supertypes=[],
    )
    res = next(psp_func.process_element(kafka_notification.to_json(), None))

    assert isinstance(res, tuple)
    assert res[0].tag_id == "no_atlas_entity"
    returned_kafka_notification = KafkaNotification.from_json(res[1])
    assert returned_kafka_notification == kafka_notification

def test_correct_resutl():
    """Test that VerifyKafkaNotification return correct result if atlas entity is not empty."""
    psp_func = ValidateKafkaNotifications()

    kafka_notification = KafkaNotification(
        kafka_notification={"message": {"entity": {"guid": "correct_guid"}}}, atlas_entity={"atlas": "entity"}, msg_creation_time=1, event_time=1, atlas_entity_audit={}, supertypes=[],
    )
    res = next(psp_func.process_element(kafka_notification.to_json(), None))

    returned_kafka_notification = KafkaNotification.from_json(res)
    assert returned_kafka_notification == kafka_notification
