[![codecov](https://codecov.io/gh/aerosense-ai/data-gateway/branch/main/graph/badge.svg?token=GEQFQVL2TK)](https://codecov.io/gh/aerosense-ai/data-gateway)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![python-template](https://img.shields.io/badge/template-python--library-blue)](https://github.com/thclark/python-library-template)


# data-gateway

Read the docs [here.](https://aerosense-data-gateway.readthedocs.io/en/latest/)

*Note that the test coverage figure is more like 90% - the recent addition of multiprocessing has made it difficult to
measure the true coverage across the codebase.*

## Installation and usage
To install, run one of
```shell
pip install data-gateway
```
```shell
poetry add data-gateway
```

The command line interface (CLI) can then be accessed via:
```shell
gateway --help
```

```
Usage: gateway [OPTIONS] COMMAND [ARGS]...

  Enter the Aerosense Gateway CLI. Run the on-tower gateway service to read
  data from the bluetooth receivers and send it to Aerosense Cloud.

Options:
  --logger-uri TEXT               Stream logs to a websocket at the given URI
                                  (useful for monitoring what's happening
                                  remotely).

  --log-level [debug|info|warning|error]
                                  Set the log level.  [default: info]
  --version                       Show the version and exit.
  -h, --help                      Show this message and exit.

Commands:
  add-sensor-type      Add a sensor type to the BigQuery dataset.
  create-installation  Create an installation representing a collection of...
  start                Begin reading and persisting data from the serial...
  supervisord-conf     Print conf entry for use with supervisord.
```

## Developer notes

### Installation
We're using `poetry` instead of `pip` to manage the package. In terms of developer experience, this just means there are
some slightly different commands to run than usual. `data-gateway` can still be `pip`-installed by anyone anywhere, but
dependency resolution and dependency specification for `data-gateway` developers is improved by using `poetry` locally.

#### Clone the repository

First, clone the repository:
```shell
export GATEWAY_VERSION="0.11.7" # Or whatever release number you aim to use, check the latest available on GitHub;
git clone https://github.com/aerosense-ai/data-gateway.git@${GATEWAY_VERSION}
```

Then, change directory into the repository:
```shell
cd data-gateway
```

#### Install on Linux or MacOS

Run the following from the repository root.
```bash
# Install poetry.
pip install poetry

# Editably install data-gateway, including its development dependencies.
poetry install
```

This will editably install `data-gateway` in a `poetry`-managed virtual environment, meaning:
- Any local changes you make to it will be automatically used when running it locally
- It won't be affected by changes to other python packages you have installed on your system, making development much
  easier and more deterministic

Don't forget to re-activate the virtual environment each time you use a new terminal window to work in the repository.

#### Install on Windows
This workflow works for Windows using Powershell.

Prerequisites:
1. Make sure to have python not installed from the [python.org](https://www.python.org/)
2. Install [pyenv-win](https://github.com/pyenv-win/pyenv-win) via pip method
3. Execute ```pip install virtualenv```

Installation:
1. Clone this repo as described above.
2. `cd data-gateway`
3. `pyenv install 3.7.0` (or higher)
4. `pyenv local 3.7.0`
5. `pyenv rehash`
6. `virtualenv venv`
7. `./venv/Scripts/activate`
8. `pip install poetry`
9. `poetry install`

Every time you enter the repo over powershell again, make sure to activate the venv using
```
./venv/Scripts/activate
```

### Testing
These environment variables need to be set to run the tests:
* `GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/service/account/file.json`

Then, from the repository root, run
```bash
tox
```

## Contributing
Take a look at our [contributing](/docs/contributing.md) page.
