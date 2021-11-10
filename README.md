
[![pipeline status](https://gitlab.com/windenergie-hsr/aerosense/digital-twin/data-gateway/badges/main/pipeline.svg)](https://gitlab.com/windenergie-hsr/aerosense/digital-twin/data-gateway/-/commits/main)
[![coverage report](https://gitlab.com/windenergie-hsr/aerosense/digital-twin/data-gateway/badges/main/coverage.svg)](https://gitlab.com/windenergie-hsr/aerosense/digital-twin/data-gateway/-/commits/main)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![python-template](https://img.shields.io/badge/template-python--library-blue)](https://github.com/thclark/python-library-template)


# data-gateway

## Documentation
Documentation for `data-gateway` can be found [here.](https://windenergie-hsr.gitlab.io/aerosense/digital-twin/data-gateway)

## Developer notes

### Installation

#### Clone the repository
First, clone the repository. It's a private repository, so you'll either need to clone it via:
- `ssh`, or
- `https` with:
  - a personal access token (PAT), or
  - your GitLab username and password entered into the command line when asked.

To use a PAT, run the following:
```shell
export GATEWAY_VERSION="0.2.0" # Or whatever release number you aim to use, check the latest available on GitLab;
export PERSONAL_ACCESS_TOKEN=abcdef123456
git clone git+https://${PERSONAL_ACCESS_TOKEN_GITHUB}@gitlab.com/windenergie-hsr/aerosense/digital-twin/data-gateway@${GATEWAY_VERSION}
```

For instructions on how to create a PAT, [click here](https://windenergie-hsr.gitlab.io/aerosense/digital-twin/data-gateway/installation.html).

Finally, change directory into the repository:
```shell
cd data-gateway
```

#### Install on Linux or MacOS
For development, run the following from the repository root.
```bash
# Create a virtual environment.
python3 -m venv venv

# Activate the virtual environment.
source ./venv/bin/activate

# Editably install data-gateway.
pip install -r requirements-dev.txt
```

This will editably install `data-gateway` in a virtual environment, meaning:
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
2. ``` cd data-gateway```
2. ``` pyenv install 3.7.0``` (or higher)
3. ``` pyenv local 3.7.0 ```
4. ``` pyenv rehash ```
5. ``` virtualenv venv ```
6. ``` ./venv/Scripts/activate ```
7. ``` pip install -r requirements-dev.txt ```

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

  AeroSense Gateway CLI. Run the on-tower gateway service to read data from
  the bluetooth receivers and send it to AeroSense Cloud.

Options:
  --logger-uri TEXT               Stream logs to a websocket at the given URI
                                  (useful for monitoring what's happening
                                  remotely).

  --log-level [debug|info|warning|error]
                                  Set the log level.  [default: info]
  --version                       Show the version and exit.
  -h, --help                      Show this message and exit.

Commands:
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

- Valid github repo and files
- Code style
- Import order
- PEP8 compliance
- Documentation build
- Branch naming convention
- Conventional Commit messages

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
git clone <your_forked_repo_address>    # fetches the repo to your local machine
cd data_gateway                     # move into the repo directory
pyenv virtualenv 3.6.9 myenv            # Makes a virtual environment for you to install the dev tools into. Use any python >= 3.6
pyend activate myenv                    # Activates the virtual environment so you don't screw up other installations
pip install -r requirements-dev.txt     # Installs the testing and code formatting utilities
pre-commit install                      # Installs the pre-commit code formatting hooks in the git repo
tox                                     # Runs the tests with coverage. NB you can also just set up pycharm or vscode to run these.
```

- Adopt a Test Driven Development approach to implementing new features or fixing bugs.

- Ask the `data-gateway` maintainers *where* to make your pull request. We'll create a version branch, according to the
roadmap, into which you can make your PR. We'll help review the changes and improve the PR.

- Once checks have passed, test coverage of the new code is >=95%, documentation is updated and the Review is passed, we'll merge into the version branch.

- Once all the roadmapped features for that version are done, we'll release.


### Release process

The process for creating a new release is as follows:

1. Check out a branch for the next version, called `vX.Y.Z`
2. Create a Pull Request into the `main` branch.
3. Undertake your changes, committing and pushing to branch `vX.Y.Z`
4. Ensure that documentation is updated to match changes, and increment the changelog. **Pull requests which do not update documentation will be refused.**
5. Ensure that test coverage is sufficient. **Pull requests that decrease test coverage will be refused.**
6. Ensure code meets style guidelines (pre-commit scripts and flake8 tests will fail otherwise)
7. Address Review Comments on the PR
8. Ensure the version in `setup.py` is correct and matches the branch version.
9. Merge to `main`. Successful test, doc build, flake8 and a new version number will automatically create the release on pypi.
10. Go to code > releases and create a new release on GitHub at the same SHA.


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
