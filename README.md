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

Below are instructions on various ways to install this project. You can choose to either:

1. [Set up a local development environment](#local-installation), or
2. [Use the provided development container](#dev-container-installation) (requires Docker)

### Local Installation

To develop this project on your local machine, follow the steps outlined below.

> **Note**: Ensure you have Python version 3.11 installed. If not, download it from [here](https://www.python.org/downloads/).

1. This project uses [Poetry](https://python-poetry.org/) as a dependency manager. Run the following command to install Poetry:

```bash
python -m pip install poetry
```

2. Next, navigate to the folder where you want the repository to be stored and run the following command to clone the git repository:

```bash
git clone https://github.com/thijs-franck/python-template
```

3. Navigate to the root of the repository and run the following command. Poetry will create a virtual environment and install all the necessary dependencies in it.

```bash
poetry install
```

4. Finally, install the pre-commit hook for your local repository by running the following command:

```bash
poetry run pre-commit install
```

6. You're all set! You can now run, develop, build, and test the project in your local development environment.

### Dev Container Installation

This project includes a [development container](https://containers.dev/) to simplify the setup process and provide a consistent development environment.

You can use the dev container locally with either [Visual Studio Code](#visual-studio-code) or [PyCharm](#pycharm), or remotely with [GitHub Codespaces](#github-codespaces).

#### Visual Studio Code

> **Note**: The following instructions assume that you have already installed [Docker](https://www.docker.com/) and [Visual Studio Code](https://code.visualstudio.com/).

1. Install the [Remote Development extension pack](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.vscode-remote-extensionpack) in Visual Studio Code.

2. Make sure the Docker agent is running, and open Visual Studio Code.

3. Press `F1` to open the command palette, and then type "Dev-Containers: Clone Repository in Container Volume" and select it from the list. Alternatively, you can click on the green icon in the bottom-left corner of the VS Code window and select "Dev-Containers: Clone Repository in Container Volume" from the popup menu.

4. Next, the command palette will ask you for the repository URL. Copy the URL of the GitHub repository, paste it into the command palette and confirm by pressing `Enter`.

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
