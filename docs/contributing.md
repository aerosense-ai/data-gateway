# Contributing

## Developing Data Gateway
- We adopt a test-driven development (TDD) approach to implementing new features or fixing bugs
- We use [`pre-commit`](https://pre-commit.com/) to apply consistent code quality checks and linting to new code, commit messages, and documentation - see [below](#pre-commit) for how to set this up
- Documentation is automatically built by `pre-commit` but needs to be updated with any changes to public interface of the package


## Release process
We use continuous deployment and semantic versioning for our releases.
- Continuous deployment - each pull request into `main` constitutes a new version
- [Semantic versioning](https://semver.org/) supported by [Conventional Commits](https://github.com/octue/conventional-commits) to automate version numbering
- Using Conventional Commit messages is essential for this to be automatic. We've developed a `pre-commit` check that guides and enforces this

1. Check out a new branch
2. Create a pull request into the `main` branch
3. Undertake your changes, committing and pushing to your branch
4. Ensure that documentation is updated to match changes, and increment the changelog. **Pull requests which do not update documentation will be refused.**
5. Ensure that test coverage is sufficient. **Pull requests that decrease test coverage without good reason will be refused.**
6. Ensure code meets style guidelines (`pre-commit` checks will fail otherwise)
7. Address review comments on the PR
8. Ensure the version in `pyproject.toml` is correct and satisfies the GitHub workflow check
9. Merge into `main`. A release will automatically be created on GitHub and published to PyPi and Docker Hub.


## Opening a pull request as an external developer
- Please raise an issue on the board (or add your $0.02 to an existing issue) so the maintainers know what's happening and can advise / steer you.
- Create a fork of `data-gateway`, undertake your changes on a new branch, (see `.pre-commit-config.yaml` for branch naming conventions). To run tests and make commits, you'll need to do something like:

  ```
  git clone <your_forked_repo_address>                           # Fetch the repo to your local machine
  cd data_gateway                                                # Move into the repo directory
  pyenv virtualenv 3.6.9 myenv                                   # Make a virtual environment for you to install the dev tools into. Use any python >= 3.7
  pyend activate myenv                                           # Activate the virtual environment so you don't screw up other installations
  poetry install                                                 # Install the testing and code formatting utilities
  pre-commit install && pre-commit install -t commit-msg         # Install the pre-commit code formatting hooks in the git repo
  tox                                                            # Run the tests with coverage. NB you can also just set up pycharm or vscode to run these.
  ```

- Open a pull request into the main branch of `aerosense-ai/data-gateway`.
- Once checks have passed, test coverage of the new code is 100%, documentation is updated, and the review is passed, we'll merge and release.


## Code quality
This library is written with:
 - `black` style
 - `sphinx` docs including automated doc build
 - `pre-commit` hooks
 - `tox` tests
 - Code coverage


## Pre-Commit
You need to install pre-commit to get the hooks working. Run:
```
pip install pre-commit
pre-commit install && pre-commit install -t commit-msg
```

Once that's done, each time you make a commit, the [following checks](/.pre-commit-config.yaml) are made:

- Valid GitHub repo and files
- Code style
- Import order
- PEP8 compliance
- Docstring standards
- Documentation build
- Branch naming convention
- Conventional Commit message compliance

Upon failure, the commit will halt. **Re-running the commit will automatically fix most issues** except:

- The `flake8` checks... hopefully over time `black` (which fixes most things automatically already) will remove the need for it
- Docstrings - the error messages should explain how to fix these easily
- You'll have to fix documentation yourself prior to a successful commit (there's no auto fix for that!!)
- Commit messages - the error messages should explain how to fix these too

You can run pre-commit hooks without making a commit, too, like:
```
pre-commit run black --all-files
```
or
```
# -v gives verbose output, useful for figuring out why docs won't build
pre-commit run build-docs -v
```


## Documentation

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
