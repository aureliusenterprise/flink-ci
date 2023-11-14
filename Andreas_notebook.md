# Available User Interfaces
- Kafka UI: http://localhost:8082
- Flink Job Manager: http://localhost:8081
- kibana: http://localhost:5601

# pre-commit checks
in a terminal you can get more information about a pre-commit failure by running
source .venv/bin/activate
pre-commit run

# How to deploy flink jobs
```bash
source .venv/bin/activate
flink run -d -py jobs/publish_state.py -pyexec /workspace/.venv/bin/python
```

```bash
source .venv/bin/activate
flink run -d -py examples/elastic_test.py -pyexec /workspace/.venv/bin/python
```

```bash
source .venv/bin/activate
flink run -d -py examples/kafka_source_flink_sink_demo.py -pyexec /workspace/.venv/bin/python
```
example message {"name":"anwo", "id":"15"}

# How to debug a flink job
To start debugging the flink job, run the command
flink-debug

There is plenty of output from this command. In the last row of the output looks something like
'connect in the vs code debugger to process id 1234'

This is the relevant process id the vs code debugger has to debug to.
To actually start the debugging open in vs code the debugger tab and
press the green triangel in the top to start the debugger.
The debugger will present a list of processes. Select the process
with the process id mentined before, like e.g. 1234.

If the connection was successful, you will see an orrange bar on the bottom of vs code.
Now you can define breakpoints in vs code and step through the code.

# How to create type definitions in Apache Atlas

in the terminal in the dev container
```bash
source .venv/bin/activate
cd /workspace/docker/docker-compose-atlas/scripts
python init-atlas-m4i-types.py
```



# backup - to be removed later on
git commit -a --no-verify -m "message"


{"msgCreationTime": ['Missing data for required field.'],
"eventTime": ['Missing data for required field.'],
"atlasEntityAudit": ['Missing data for required field.'],
"kafkaNotification": ['Missing data for required field.']}


sudo sysctl -w vm.max_map_count=262144
