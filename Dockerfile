FROM python:3.10-slim

ARG NB_USER=jovyan
ARG NB_UID=1000
ENV USER=${NB_USER} \
    HOME=/home/${NB_USER} \
    PATH="/home/${NB_USER}/.local/bin:/home/${NB_USER}/blender:${PATH}" \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends wget xz-utils && \
    rm -rf /var/lib/apt/lists/* && \
    useradd -m -u ${NB_UID} ${NB_USER}

USER ${NB_USER}
WORKDIR ${HOME}

# COPY d'abord
COPY --chown=${NB_USER}:${NB_USER} . ${HOME}

# Blender installé APRÈS le COPY
RUN wget -q https://download.blender.org/release/Blender3.6/blender-3.6.23-linux-x64.tar.xz && \
    mkdir -p "$HOME/blender" && \
    tar -xf blender-3.6.23-linux-x64.tar.xz -C "$HOME/blender" --strip-components=1 && \
    rm blender-3.6.23-linux-x64.tar.xz

RUN pip install --no-cache-dir notebook

EXPOSE 8888

CMD ["binder/start"]

# CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser"]

