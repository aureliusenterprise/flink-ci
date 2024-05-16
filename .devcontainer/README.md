# Dev Container

This project includes a [development container](https://containers.dev/) to simplify the setup process and provide a consistent development environment.

You can use the dev container locally with either [Visual Studio Code](#visual-studio-code) or [PyCharm](#pycharm), remotely with [GitHub Codespaces](#github-codespaces), or with CLI.

> **Note**: You need at least ~18GB of RAM allocated to run the containers on your local machine.

## Visual Studio Code

This section describes how to install the development container using Visual Studio Code. There are some differences in the setup process depending on your operating system.

> **Note**: The following instructions assume that you have already installed [Docker](https://www.docker.com/) and [Visual Studio Code](https://code.visualstudio.com/).

### Prerequisites

Regardless of your operating system, please install the [Remote Development extension pack](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.vscode-remote-extensionpack) in Visual Studio Code.

### For Windows:

1. Make sure the Docker Desktop is running, and open Visual Studio Code.

2. Press `F1` to open the command palette, and then type "Dev Containers: Clone Repository in Container Volume" and select it from the list. Alternatively, you can click on the green icon in the bottom-left corner of the VS Code window and select "Clone Repository in Container Volume" from the popup menu.

3. Next, the command palette will ask you for the repository URL. Paste `https://github.com/aureliusenterprise/flink-ci.git` into the command palette and confirm by pressing Enter.

4. VS Code will automatically build the container and connect to it. This might take some time for the first run as it downloads the required Docker images and installs extensions.

5. Once connected, you'll see "Dev Container: Aurelius Atlas - Flink" in the bottom-left corner of the VS Code window, indicating that you are now working inside the container.

6. You're all set! You can now run, develop, build, and test the project using the provided development environment.

### For Linux (and WSL):

1. Make sure the Docker agent is running on your system.

2. Clone the GitHub repository to your local machine.

    ```bash
    git clone https://github.com/aureliusenterprise/flink-ci.git
    ```

3. Navigate to the cloned project root and open the project in VS Code by running:

    ```bash
    cd flink-ci
    code .
    ```

4. Upon opening the project in VS Code, a popup will appear prompting you to "Reopen in Container". Accept this prompt. If you do not see the popup, press `F1` to open the command palette and type "Dev Containers: Reopen in Container", then select it from the list.

5. VS Code will automatically build the container and connect to it. This might take some time for the first run as it downloads the required Docker images and installs extensions.

6. Once connected, you'll see "Dev Container: Aurelius Atlas - Flink" in the bottom-left corner of the VS Code window, indicating that you are now working inside the container.

7. You're all set! You can now run, develop, build, and test the project using the provided development environment.

## PyCharm

To connect PyCharm to the Development Container, please [follow these instructions](https://www.jetbrains.com/help/pycharm/connect-to-devcontainer.html) provided in the official JetBrains documentation.

## GitHub Codespaces

> **Note**: GitHub Codespaces is a paid service. At the time of writing, it offers 60 hours of development time for free every month. Use with care.

1. Ensure that you have access to [GitHub Codespaces](https://github.com/features/codespaces).

2. Navigate to the GitHub repository for the project.

3. Click the "Code" button and then select "Open with Codespaces" from the dropdown menu.

4. Click on the "+ New codespace" button to create a new Codespace for the project.

5. GitHub Codespaces will automatically build the container and connect to it. This might take some time for the first run as it downloads the required Docker images and installs extensions.

6. Once connected, you'll see "Dev Container: Aurelius Atlas - Flink" in the bottom-left corner of the VS Code window, indicating that you are now working inside the container.

7. You're all set! You can now run, develop, build, and test the project using the provided development environment.

## CLI (Linux/WSL)

If you prefer to use any other code editor of your choice you can opt to start up the containers using devcontainers CLI. Afterwards simply login to the main dev container and run the commands from there.

> **Note**: The following instructions assume that you have already installed [Docker](https://www.docker.com/).
1. Make sure the Docker agent is running on your system.

2. Clone the GitHub repository to your local machine.

    ```bash
    git clone https://github.com/aureliusenterprise/flink-ci.git
    ```

3. Navigate to the cloned project root and open the project in VS Code by running:

    ```bash
    cd flink-ci
    devcontainer up --workspace-folder .
    ```
4. To login to the dev container run:
    ```bash
    docker exec -it flink-ci_devcontainer-dev-1 bash
    ```
    You're ready to go!
