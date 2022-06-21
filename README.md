[![codecov](https://codecov.io/gh/aerosense-ai/data-gateway/branch/main/graph/badge.svg?token=GEQFQVL2TK)](https://codecov.io/gh/aerosense-ai/data-gateway)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![python-template](https://img.shields.io/badge/template-python--library-blue)](https://github.com/thclark/python-library-template)


# data-gateway

Read the docs [here.](https://aerosense-data-gateway.readthedocs.io/en/latest/)

*Note that the test coverage figure is more like 90% - the recent addition of multiprocessing has made it difficult to
measure the true coverage across multiple processes.*

## Installation and usage
To install, run:
```shell
pip install git+https://github.com/aerosense-ai/data-gateway.git
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

#### Poetry
We're using `poetry` instead of `pip` to manage the package to take advantage of the `poetry.lock` file [among other
useful features](https://python-poetry.org/). In terms of developer experience, this just means there are some slightly
different commands to run than usual. `data-gateway` can still be `pip`-installed by anyone anywhere, but dependency
resolution and dependency specification for `data-gateway` developers is improved by using `poetry` locally.

#### Architecture-specific installations
Due to some (most likely temporary) constraints with `poetry` and the need to run and develop the gateway on Linux,
Windows, M1 Macs, and Raspberry Pis, the need has arisen for some slightly different installation procedures on these
different architectures/platforms. Instructions are detailed below - [click here](https://github.com/aerosense-ai/data-gateway/issues/65)
to read more.

#### Clone the repository
First, clone the repository and `cd` into it:
```shell
git clone https://github.com/aerosense-ai/data-gateway.git
cd data-gateway
```

Then follow the instructions for your platform below.

#### Install on MacOS and Linux (except on Raspberry Pi)
Run the following from the repository root:
```shell
pip install poetry

# Editably install data-gateway, including its development dependencies.
poetry install
```

This will editably install `data-gateway` in a `poetry`-managed virtual environment, meaning:
- Any local changes you make to it will be automatically used when running it locally
- It won't be affected by changes to other python packages you have installed on your system, making development much
  easier and more deterministic

You may also need to run:
```shell
sudo apt-get update
sudo apt-get install libhdf5-dev libhdf5-serial-dev
```

#### Install on Raspberry Pi
Run the following from the repository root:
```shell
sudo apt-get update
sudo apt-get install libhdf5-dev libhdf5-serial-dev
pip install -r requirements-pi-dev.txt
```

#### Install on Windows
This workflow works for Windows using Powershell.

Prerequisites:
1. Make sure to have python not installed from [python.org](https://www.python.org/)
2. Install [pyenv-win](https://github.com/pyenv-win/pyenv-win) via pip method
3. Execute ```pip install virtualenv```

Installation:
```shell
pyenv install 3.7.0  # (or higher)
pyenv local 3.7.0
pyenv rehash
virtualenv venv
./venv/Scripts/activate
pip install poetry
poetry install
```

Every time you enter the repo over powershell again, make sure to activate the venv using
```shell
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
