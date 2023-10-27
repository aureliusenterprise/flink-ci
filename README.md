# Aurelius Atlas - Flink

This project houses the Flink jobs and tasks for Aurelius Atlas. This repository provides a integrated development platform to help streamline Flink development and deployment.

## Table of Contents
- [Table of Contents](#table-of-contents)
- [Setup and Installation](#setup-and-installation)
- [Services](#services)
- [How to Use](#how-to-use)

## Setup and Installation

Leverage the provided [development container](https://containers.dev/) to automatically set up the development environment.

📝 [**Devcontainer Guide**](.devcontainer/README.md)

## Services

This project includes several services for end-to-end testing of Flink jobs. Each service's purpose and UI link (if available) are listed below:

| Service            | Description                                                                             | UI                       |
| ------------------ | --------------------------------------------------------------------------------------- | ----------------------------- |
| Flink Job Manager  | Coordinates Flink jobs with a management dashboard.                                     | [Link](http://localhost:8081) |
| Flink Task Manager | Executes Flink tasks; integrated with the dev environment for easier debugging.         |                               |
| Apache Kafka       | Simulates the Aurelius Atlas Kafka instance for Apache Atlas data flow.                 | [Link](http://localhost:8082) |
| Elasticsearch      | Mirrors Aurelius Atlas's Elasticsearch; used for entity lookups and job output storage. | [Link](http://localhost:9200) |
| Kibana             | Visualizes Elasticsearch data and offers interactive tools.                             | [Link](http://localhost:5601) |
| Zookeeper          | Manages configuration, naming, synchronization, and group services.                     |                               |


## How to Use

Below are instructions on how to execute common development tasks.

### Deploying a Flink Job

First, make sure your Python virtual environment is active in the terminal session:

```bash
source .venv/bin/activate
```

> **Note**: The project is set up to automatically activate the virtual environment for every new terminal session.

Next, submit a job to the jobmanager. For example, to run the `publish_state` job:

```bash
flink-run jobs/publish_state.py
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
