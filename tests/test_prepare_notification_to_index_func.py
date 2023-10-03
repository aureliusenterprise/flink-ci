from flink_jobs.job_runners.publish_state import PrepareNotificationToIndex
from flink_jobs.flink_dataclasses import DeadLetterBoxMesage, KafkaNotification


def test_correct_return():
    """
    Test that PrepareNotificationToIndex return correct result
    """
    psp_func = PrepareNotificationToIndex()

    kafka_notification = KafkaNotification(
        kafka_notification={"message": {"entity": {"guid": "correct_guid"}}}, atlas_entity={"atlas": "entity"}, msg_creation_time=1, event_time=2, atlas_entity_audit={}, supertypes=[]
    )
    res = next(psp_func.process_element(kafka_notification.to_json(), None))

    assert res == {
            "doc_id": "correct_guid_1",
            "msgCreationTime": 1,
            "eventTime": 2, 
            "body": {"atlas": "entity"}
        }
    