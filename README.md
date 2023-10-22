# Python Template

This is a Python template that provides a starting point for developing great Python applications. The template has the following key features:

- Dependency management using [Poetry](https://python-poetry.org/)
- Code linting and formatting using [Ruff](https://github.com/astral-sh/ruff)
- Static type checking using [Pyright](https://github.com/microsoft/pyright)
- Unit testing using [pytest](https://docs.pytest.org)
- Pre-commit hooks using [pre-commit](https://pre-commit.com/)
- A CI environment using [GitHub Actions](https://docs.github.com/en/actions)
- A consistent development environment using a [development container](https://containers.dev)

If you're a developer and want to contribute to this template, please refer to the [contribution guide](./CONTRIBUTING.md).

Happy coding!

## Installation

This project includes a [development container](https://containers.dev/) to simplify the setup process and provide a consistent development environment.

You can use the dev container locally with either [Visual Studio Code](#visual-studio-code) or [PyCharm](#pycharm), or remotely with [GitHub Codespaces](#github-codespaces).

#### Visual Studio Code

This section describes how to install the development container using Visual Studio Code. There are some differences in the setup process depending on your operating system.

> **Note**: The following instructions assume that you have already installed [Docker](https://www.docker.com/) and [Visual Studio Code](https://code.visualstudio.com/).

##### Prerequisites

Regardless of your operating system, please install the [Remote Development extension pack](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.vscode-remote-extensionpack) in Visual Studio Code.

Further, the WSL must use a Ubuntu 20.04 version, which has docker and docker compose installed.
- installation of docker: https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-20-04
- installation of docker compose: https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-20-04
The provided config file for the dev container is NOT working on Ubuntu 22.04.

For the docker compose file to work you have to build a docker image by running the followign commands
docker build -t pyflink:0.1 -f ./docker/images/pyflink/Dockerfile .

this command will create a docker image named pyflink tag 0.1

##### For Windows:

1. Make sure the Docker Desktop is running, and open Visual Studio Code.

1.a) If you are using WSL 2 on Windows, to ensure the WSL 2 back-end is enabled: Right-click on the Docker taskbar item and select **Settings**. Check **Use the WSL 2 based engine** and verify your distribution is enabled under **Resources > WSL Integration**.

1.b) startup WSL and press `F1` to open command pallet, and type `Connect to WSL`. Select the right distribution and a new VS Code instance running in the WSL is opened.

1.c) In the menu on the left there is a button: Clone repository. Press the button and add the followign URL https://github.com/aureliusenterprise/flink-ci.git and press enter. Then open the repository. Now you have access to the code in the WSL with your VS Code.


2. Press `F1` to open the command palette, and then type "Dev Containers: Clone Repository in Container Volume" and select it from the list. Alternatively, you can click on the green icon in the bottom-left corner of the VS Code window and select "Clone Repository in Container Volume" from the popup menu.

2.a) enter the followign URL https://github.com/aureliusenterprise/flink-ci.git and press enter

3. Next, the command palette will ask you for the repository URL. Copy the URL of the GitHub repository, paste it into the command palette and confirm by pressing Enter.

4. VS Code will automatically build the container and connect to it. This might take some time for the first run as it downloads the required Docker images and installs extensions.

5. Once connected, you'll see "Dev Container: Python 3" in the bottom-left corner of the VS Code window, indicating that you are now working inside the container.

6. You're all set! You can now run, develop, build, and test the project using the provided development environment.

##### For Linux (and WSL):

1. Make sure the Docker agent is running on your system.

2. Clone the GitHub repository to your local machine.

    ```bash
    git clone <REPOSITORY_URL>
    ```

3. Navigate to the cloned project root and open the project in VS Code by running:

    ```bash
    cd <PROJECT_NAME>
    code .
    ```

4. Upon opening the project in VS Code, a popup will appear prompting you to "Reopen in Container". Accept this prompt. If you do not see the popup, press `F1` to open the command palette and type "Dev Containers: Reopen in Container", then select it from the list.

5. VS Code will automatically build the container and connect to it. This might take some time for the first run as it downloads the required Docker images and installs extensions.

6. Once connected, you'll see "Dev Container: Python 3" in the bottom-left corner of the VS Code window, indicating that you are now working inside the container.

7. You're all set! You can now run, develop, build, and test the project using the provided development environment.

#### PyCharm

To connect PyCharm to the Development Container, please [follow these instructions](https://www.jetbrains.com/help/pycharm/connect-to-devcontainer.html) provided in the official JetBrains documentation.

#### GitHub Codespaces

> **Note**: GitHub Codespaces is a paid service. At the time of writing, it offers 60 hours of development time for free every month. Use with care.

1. Ensure that you have access to [GitHub Codespaces](https://github.com/features/codespaces).

2. Navigate to the GitHub repository for the project.

3. Click the "Code" button and then select "Open with Codespaces" from the dropdown menu.

4. Click on the "+ New codespace" button to create a new Codespace for the project.

5. GitHub Codespaces will automatically build the container and connect to it. This might take some time for the first run as it downloads the required Docker images and installs extensions.

6. Once connected, you'll see "Dev Container: Python 3" in the bottom-left corner of the VS Code window, indicating that you are now working inside the container.

7. You're all set! You can now run, develop, build, and test the project using the provided development environment.

## Available User Interfaces
- Kafka UI: http://localhost:8082
- Flink Job Manager: http://localhost:8081
- kibana: http://localhost:5601

## How to deploy a flink job
see the readme file in the jobs folder

## How to make commits work
git config --global user.email "andreas.wombacher@gmail.com"
git config --global user.name "wombach"


## pre-commit checks

in a terminal you can get more information about a pre-commit failure by running

pre-commit run

## How to deploy flink jobs
Flink jobs can be deployed from the dev container, which is the task manager. To do so open
a terminal on the dev container and start the python virtual environment
source .venv/bin/activate

if you are executing for the first time, make a copy of the sample env file
cp .env_sample .env

and potentially adkust the values.
Next these environment variables have to be added to the terminal using the following commands
set -a
source jobs/.env
set +a

to submit a job like e.g. Publish_state you can call from the project root directory
flink run -d -py jobs/publish_state.py -pyexec /workspace/.venv/bin/python

You will see a message that the job has been submitted.

You can also see the job is then UI of flink at
http://localhost:8081/

source .venv/bin/activate
set -a
source jobs/.env
set +a
flink run -d -py jobs/publish_state.py -pyexec /workspace/.venv/bin/python


Create an index called “nyc-idx”:
curl -XPUT "http://localhost:9200/nyc-idx"
Create an index mapping called “popular-locations”:  curl -XPUT "http://localhost:9200/nyc-idx/_mapping/popular-locations" -d'
{
 "popular-locations" : {
   "properties" : {
      "cnt": {"type": "integer"},
      "location": {"type": "geo_point"},
      "time": {"type": "date"}
    }
 } 
}'

