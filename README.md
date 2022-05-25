[![codecov](https://codecov.io/gh/aerosense-ai/data-gateway/branch/main/graph/badge.svg?token=GEQFQVL2TK)](https://codecov.io/gh/aerosense-ai/data-gateway)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![python-template](https://img.shields.io/badge/template-python--library-blue)](https://github.com/thclark/python-library-template)


# data-gateway

## Documentation
Coming soon (check the `docs` directory for now).

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

### Usage
The `gateway` CLI is the main entry point.
```bash
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

### Testing
These environment variables need to be set to run the tests:
* `GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/service/account/file.json`

Then, from the repository root, run
```bash
tox
```

### Features

This library is written with:

 - `black` style
 - `sphinx` docs including automated doc build
 - `pre-commit` hooks
 - `tox` tests
 - Code coverage

### Pre-Commit

You need to install pre-commit to get the hooks working. Do:
```
pip install pre-commit
pre-commit install
pre-commit install -t commit-msg
```

Once that's done, each time you make a commit, the following checks are made:

- Valid GitHub repo and files
- Code style
- Import order
- PEP8 compliance
- Documentation build
- Branch naming convention
- Conventional Commit message checks

Upon failure, the commit will halt. **Re-running the commit will automatically fix most issues** except:

- The flake8 checks... hopefully over time Black (which fixes most things automatically already) will negate need for it.
- You'll have to fix documentation yourself prior to a successful commit (there's no auto fix for that!!).
- Any issues with the commit message will have to be fixed manually

You can run pre-commit hooks without making a commit, too, like:
```
pre-commit run black --all-files
```
or
```
# -v gives verbose output, useful for figuring out why docs won't build
pre-commit run build-docs -v
```


### Contributing

- Please raise an issue on the board (or add your $0.02 to an existing issue) so the maintainers know
what's happening and can advise / steer you.

- Create a fork of `data-gateway`, undertake your changes on a new branch, (see `.pre-commit-config.yaml` for branch naming conventions). To run tests and make commits,
you'll need to do something like:
```
git clone <your_forked_repo_address>    # Fetch the repo to your local machine
cd data_gateway                         # Move into the repo directory
pyenv virtualenv 3.6.9 myenv            # Make a virtual environment for you to install the dev tools into. Use any python >= 3.7
pyend activate myenv                    # Activate the virtual environment so you don't screw up other installations
poetry install                          # Install the testing and code formatting utilities
pre-commit install                      # Install the pre-commit code formatting hooks in the git repo
tox                                     # Run the tests with coverage. NB you can also just set up pycharm or vscode to run these.
```

- Adopt a Test Driven Development approach to implementing new features or fixing bugs.

- Ask the `data-gateway` maintainers *where* to make your pull request. We'll create a version branch, according to the
roadmap, into which you can make your PR. We'll help review the changes and improve the PR.

- Once checks have passed, test coverage of the new code is >=95%, documentation is updated and the Review is passed, we'll merge into the version branch.

- Once all the roadmapped features for that version are done, we'll release.


### Release process

The process for creating a new release is as follows:

1. Check out a branch with a name describing the main change
2. Create a Pull Request into the `main` branch.
3. Undertake your changes, committing and pushing to branch
4. Ensure that documentation is updated to match changes, and increment the changelog. **Pull requests which do not update documentation will be refused.**
5. Ensure that test coverage is sufficient. **Pull requests that decrease test coverage will be refused.**
6. Ensure code meets style guidelines (pre-commit scripts and flake8 tests will fail otherwise)
7. Address Review Comments on the PR
8. Ensure the version in `pyproject.toml` is correct.
9. Merge into `main`. Successful test, doc build, flake8 and a new version number will automatically create a GitHub release.

## Documents

### Building documents automatically

The documentation will build automatically in a pre-configured environment when you make a commit.

In fact, the way pre-commit works, you won't be allowed to make the commit unless the documentation builds,
this way we avoid getting broken documentation pushed to the main repository on any commit sha, so we can rely on
builds working.


### Building documents manually

**If you did need to build the documentation**

Install `doxgen`. On a mac, that's `brew install doxygen`; other systems may differ.

Install sphinx and other requirements for building the docs:
```
pip install -r docs/requirements.txt
```

Run the build process:
```
sphinx-build -b html docs/source docs/build
```
