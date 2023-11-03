# NeoBridge
_A bridge between various NLP pipelines and sources and Neo4J_

Note: Many (all?) of these tools require that there is a running Neo4J database on the local system. To launch this server with Docker, see the main [README.md](../README.md).

## Installation

It is _strongly_ recommended to configure a virtual environment (unless you're running in a container, such as Docker or Kubernetes). To this end we include `./setup-venv.sh` which will do this for you. Note that you will manually have to execute the command returned by this script to enter the virtual environment, and for whenever you start a new terminal session.

We also leverage [Poetry](https://python-poetry.org/) rather than `pip` for dependency management. The way this is installed varies by operating system and environment (for example, the MacOS `brew` package `poetry` works just fine, but so does `pipx install poetry` via `pipx`) so please consult the [Poetry Documentation](https://python-poetry.org/docs/) for the details for your system.

Once installed, running `./setup-dependencies.sh` should get all required packages and dependency data.

### TL;dr

```
$> ./setup-venv.sh
$> ./source ./venv/bin/activate
$> ./setup-dependencies.sh
```

## Tools

All tools implement command-line options, so running any with `--help` will print the arguments and their descriptions for the tool.

- `ipn2neo4j.py` - Load "Index of Place Names" data into Neo4j
- `linker.py` - Attempt to create or update various cross-tool relationships.
- `NeoBridge.py` - The base Neo4J driver class for subsequent tools.
- `spacy2neo4j.py` - Runs various corpus inputs through the `spacy` pipeline (optionally using `pymusas` tags and models) and loads them into Neo4j