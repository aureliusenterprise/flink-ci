import json

from pyflink.datastream import StreamExecutionEnvironment

if __name__ == "__main__":

    env = StreamExecutionEnvironment.get_execution_environment()

    # write all the data to one file
    env.set_parallelism(1)

    data_source = env.from_collection([{"name": "1", "value": 1}, {"name": "2", "value": 2},
                                       {"name": "3", "value": 3}])

    #     data_source
    #     .map(lambda x: x, Types.STRING())

    data_source.map(json.dumps).map(json.loads).print()

    env.execute()
