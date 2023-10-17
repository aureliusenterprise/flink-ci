start the python virtuqal environment
source .venv/bin/activate

in the terminal in the dev container got to directory jobs
copy the sample env file
cp .env_sample .env

you have potentially to adjust the environment variables:
adjust the file if necessary.

then execute the file and register the variables
set -a
source .env
set +a

to submit a job like e.g. Publish_state you can call from the project root directory
flink run -d -py jobs/publish_state.py

the job is then accessible at http://localhost:8081/
