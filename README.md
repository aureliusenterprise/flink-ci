# Aurelius Atlas - Flink

This project houses the Flink jobs and tasks for Aurelius Atlas. This repository provides a integrated development platform to help streamline Flink development and deployment.

## Table of Contents
- [Table of Contents](#table-of-contents)
- [Setup and Installation](#setup-and-installation)
- [Services](#services)
- [Jobs](#jobs)
- [Components](#components)
- [Build](#build)
- [Development](#development)

## Setup and Installation

Leverage the provided [development container](https://containers.dev/) to automatically set up the development environment.

📝 [**Devcontainer Guide**](.devcontainer/README.md)

## Services

This project includes several services for end-to-end testing of Flink jobs. Each service's purpose and UI link (if available) are listed below:

| Service            | Description                                                                             | UI                              |
| ------------------ | --------------------------------------------------------------------------------------- | ------------------------------- |
| Apache Atlas       | Atlas backend for data storage and event triggers                                       | [Link](https://localhost:21000) |
| Keycloak           | Authentication & Authorization                                                          | [Link](https://localhost:8180)  |
| Flink Job Manager  | Coordinates Flink jobs with a management dashboard.                                     | [Link](http://localhost:8081)   |
| Flink Task Manager | Executes Flink tasks; integrated with the dev environment for easier debugging.         |                                 |
| Apache Kafka       | Simulates the Aurelius Atlas Kafka instance for Apache Atlas data flow.                 | [Link](http://localhost:8082)   |
| Kafka Connect      | Manages Kafka connectors for data ingestion into Elasticsearch.                         |                                 |
| Elasticsearch      | Mirrors Aurelius Atlas's Elasticsearch; used for entity lookups and job output storage. | [Link](http://localhost:9200)   |
| Kibana             | Visualizes Elasticsearch data and offers interactive tools.                             | [Link](http://localhost:5601)   |
| Zookeeper          | Manages configuration, naming, synchronization, and group services.                     |                                 |

## Jobs

The following Flink jobs are available for development and testing:

| Job Name                 | Description                                                                                |
|--------------------------|--------------------------------------------------------------------------------------------|
| `synchronize_app_search` | Synchronizes data from Apache Atlas to Elasticsearch for the App Search service.           |
| `publish_state`          | Publishes versioned data from Apache Atlas to Elasticsearch for the Publish State service. |

> **Note**: Each job is a Python script located in the `flink_jobs` directory.

### Configuration

The jobs are configured through environment variables. The following environment variables are available:

| Variable Name                         | Description                                                                            | Required |
| ------------------------------------- | -------------------------------------------------------------------------------------- | -------- |
| `ATLAS_SERVER_URL`                    | The URL of the Apache Atlas server.                                                    | Yes      |
| `ELASTICSEARCH_APP_SEARCH_INDEX_NAME` | The name of the index in Elasticsearch to which app search documents are synchronized. | Yes      |
| `ELASTICSEARCH_STATE_INDEX_NAME`      | The name of the index in Elasticsearch to which entity state is synchronized.          | Yes      |
| `ELASTICSEARCH_ENDPOINT`              | The endpoint URL for the Elasticsearch instance.                                       | Yes      |
| `ELASTICSEARCH_USERNAME`              | The username for Elasticsearch authentication.                                         | Yes      |
| `ELASTICSEARCH_PASSWORD`              | The password for Elasticsearch authentication.                                         | Yes      |
| `KAFKA_APP_SEARCH_TOPIC_NAME`         | The Kafka topic name to which updated App Search documents will be produced.           | Yes      |
| `KAFKA_PUBLISH_STATE_TOPIC_NAME`      | The Kafka topic name to which entity state updates will be produced.                   | Yes      |
| `KAFKA_BOOTSTRAP_SERVER_HOSTNAME`     | The hostname of the Kafka bootstrap server.                                            | Yes      |
| `KAFKA_BOOTSTRAP_SERVER_PORT`         | The port number of the Kafka bootstrap server.                                         | Yes      |
| `KAFKA_CONSUMER_GROUP_ID`             | The consumer group ID for Kafka.                                                       | Yes      |
| `KAFKA_ERROR_TOPIC_NAME`              | The Kafka topic name where errors will be published.                                   | Yes      |
| `KAFKA_PRODUCER_GROUP_ID`             | The producer group ID for Kafka.                                                       | Yes      |
| `KAFKA_SOURCE_TOPIC_NAME`             | The Kafka topic name from which data will be consumed.                                 | Yes      |
| `KEYCLOAK_CLIENT_ID`                  | The client ID for Keycloak authentication.                                             | Yes      |
| `KEYCLOAK_CLIENT_SECRET_KEY`          | The client secret key for Keycloak authentication.                                     | No       |
| `KEYCLOAK_PASSWORD`                   | The password for Keycloak authentication.                                              | Yes      |
| `KEYCLOAK_REALM_NAME`                 | The name of the Keycloak realm.                                                        | Yes      |
| `KEYCLOAK_SERVER_URL`                 | The URL of the Keycloak server.                                                        | Yes      |
| `KEYCLOAK_USERNAME`                   | The username for Keycloak authentication.                                              | Yes      |

> **Note**: For the development environment, these environment variables are set to sensible defaults in the devcontainer.

## Components

The following components are maintained in this repository:

### Apache Atlas

Apache Atlas is the backend for data storage and event triggers. The Apache Atlas configuration is defined in the `atlas` directory. In addition, this repository supplies a minimal dataset for testing purposes. The following commands are available for Apache Atlas in the development environment:

| Command                 | Description                                                                                                |
| ----------------------- | ---------------------------------------------------------------------------------------------------------- |
| `atlas-config-deploy`   | Deploys the Apache Atlas configuration from this repository to the shared volume used by the Atlas Server. |
| `atlas-export-entities` | Exports entities from Apache Atlas to `atlas/export.zip`.                                                  |
| `atlas-import-entities` | Imports the `atlas/export.zip` file to Apache Atlas.                                                       |
| `atlas-import-types`    | Imports the `m4i` types to Apache Atlas.                                                                   |
| `atlas-init`            | Initializes Apache Atlas with the `m4i` types and the entities from the `atlas/export.zip` file.           |

### Kafka Connect

The Kafka Connect sinks responsible for writing data to Elasticsearch are defined in the `elasticsearch_sink` directory. Each sink is defined as a `.j2` template file, which is used to generate the actual connector configuration file based on a given set of variables. The files containing the variables are also located in the same directory.

The following Kafka Connect sinks are available:

| Sink Name              | Description                                   | Deployment Command                 |
| ---------------------- | --------------------------------------------- | ---------------------------------- |
| `app-search-documents` | Writes App Search documents to Elasticsearch. | `app-search-documents-sink-deploy` |
| `publish-state`        | Writes entity state to Elasticsearch.         | `publish-state-sink-deploy`        |

### Keycloak

Keycloak acts as the authentication and authorization provider for Apache Atlas. The Keycloak configuration is defined in the `keycloak` directory. The configuration is split into two parts:

1. The `config/atlas-dev.json` file, which contains the realm settings.
2. The `settings/standalone.xml` file, which contains the server settings.

The following commands are available for Keycloak in the development environment:

| Command                    | Description                                                                                                      |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `get-keycloak-token`       | Retrieves a Keycloak token and prints it to the console.                                                         |
| `keycloak-config-deploy`   | Deploys the Keycloak server configuration from this repository to the shared volume used by the Keycloak Server. |
| `keycloak-settings-deploy` | Deploys the Keycloak realm settings from this repository to the shared volume used by the Keycloak Server.       |

## Build

This project is distributed as a docker image. To build the image, run the following commands from the root of the project directory:

```bash
# Build the Python distributable. The output is placed in the `dist` directory.
poetry build

# Build the docker image.
docker build .
```

> **Note**: The docker image is built using the `Dockerfile` in the root of the project directory.

## Development

Below are instructions on how to execute common development tasks.

### Deploying a Flink Job

First, make sure your Python virtual environment is active in the terminal session:

```bash
source .venv/bin/activate
```

> **Note**: The project is set up to automatically activate the virtual environment for every new terminal session.

Next, submit a job to the jobmanager. For example, to run the `synchronize_app_search` job:

```bash
flink-run flink_jobs/synchronize_app_search.py
```

### Debugging a Flink Job

Run the `flink-debug` command to allow the debugger to attach to the currently running Flink task:

```bash
flink-debug <optional_task_id>
```

> **Note**: By default, `flink-debug` targets the Flink task with `--id=1-1`. If you wish to debug a different task, include its `--id` as an argument.

From the command output, find the process ID. This is typically a four-digit code, for example `1234`.

To attach the debugger in VS Code, first open the debugger tab. Once there, click on the green play button which will display a list of currently running processes. From this list, select the process corresponding to the process ID you previously noted.

Once connected successfully, VS Code's status bar will turn orange, indicating the debugger is active. You can then set breakpoints and inspect the running task in real-time.
