from pyflink.datastream import DataStream, ProcessFunction


class AtlasProcessFunction(ProcessFunction):
    """
    A base ProcessFunction for processing elements that provides error handling functions.

    This class handles the creation of error and exception outputs in a standard format.
    It should be subclassed by other ProcessFunctions which need these capabilities.

    Attributes
    ----------
    error_tag : OutputTag
        The tag used for capturing error messages.
    job_name : str
        The name of the job using this ProcessFunction.
    """

    def __init__(self, input_stream: DataStream, job_name: str) -> None:
        """
        Initialize the AtlasProcessFunction with error_tag and job_name.

        Parameters
        ----------
        error_tag : OutputTag
            The tag used for capturing error messages.
        job_name : str
            The name of the job using this ProcessFunction.
        """
        self.input_stream = input_stream
        self.job_name = job_name
