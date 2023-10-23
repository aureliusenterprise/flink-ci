import debugpy
import publish_state
from publish_state import PublishStateJobClassLookup
from pyflink.datastream import DataStream

from flink_jobs.publish_state import DebugPublishState, ElasticClient, PublishState


class DebugPublishStateJobClassLookup(PublishStateJobClassLookup):
    """A class that allows to switch off debugging."""

    @staticmethod
    def get_job_flow(input_stream: DataStream,
        elastic_client: ElasticClient)-> PublishState:
        """
        Returns the corresponding Job object.

        Parameters
        ----------
        input_stream : DataStream
            The input stream of Kafka notifications.
        """  # noqa: D401
        # responsible for debuging on the jobmanager
        try:
            debugpy.listen(("localhost", 5678))
            debugpy.wait_for_client()  # blocks execution until client is attached
            debugpy.debug_this_thread()
            debugpy.trace_this_thread(True)
        except RuntimeError:
            pass
        return DebugPublishState(input_stream, elastic_client)

if __name__ == "__main__":
    publish_state.setup(DebugPublishStateJobClassLookup)
