# A useful base repository based on ms devcontainer but with a bunch of fixes
# and useful installs (geo tools are unnecessary for this project, but all the other tweaks are helpful)
FROM windpioneers/gdal-python:familiar-catshark-gdal-2.4.1-python-3.9-dev

# Tell zsh where you want to store history
#     We leave you to decide, but if you put this into a folder that's been mapped
#     into the container, then history will persist over container rebuilds :)
#
#     !!!IMPORTANT!!!
#     Make sure your .zsh_history file is NOT committed into your repository, as it can contain
#     sensitive information. So in this case, you should add
#         .devcontainer/.zsh_history
#     to your .gitignore file.
#
ENV HISTFILE="/workspaces/data-gateway/.devcontainer/.zsh_history"

# Switch to vscode user
USER vscode
WORKDIR /workspaces/data-gateway

# Install the rust toolchain and give permission for all users to use it
ENV RUST_INSTALL_DIR=/home/vscode/.rust
ENV RUSTUP_HOME="${RUST_INSTALL_DIR}/rustup"
ENV CARGO_HOME="${RUST_INSTALL_DIR}/cargo"
RUN mkdir -p ${RUSTUP_HOME} && \
    mkdir -p ${CARGO_HOME}
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="${CARGO_HOME}/bin:${PATH}"
RUN chmod -R ugo+rwx ${RUST_INSTALL_DIR}

# Install poetry
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python
ENV PATH "/home/vscode/.poetry/bin:$PATH"
RUN poetry config virtualenvs.create false

# Install python dependencies. Note that poetry installs any root packages by default,
# But this is not available at this stage of caching dependencies. So we do a dependency-only install here
# to cache the dependencies, then a full poetry install post-create to install any root packages.
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-ansi --no-interaction --no-root
