FROM python:3.10-slim

ARG NB_USER=jovyan
ARG NB_UID=1000
ENV USER=${NB_USER} \
    HOME=/home/${NB_USER} \
    PATH="/home/${NB_USER}/.local/bin:${PATH}" \
    PIP_NO_CACHE_DIR=1

# Dépendances système + librairies requises par Blender
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget \
        xz-utils \
        libxi6 \
        libxxf86vm1 \
        libxfixes3 \
        libxrender1 \
        libgl1 \
        libxkbcommon0 \
        libsm6 \
        && \
    rm -rf /var/lib/apt/lists/* && \
    useradd -m -u ${NB_UID} ${NB_USER}

USER ${NB_USER}
WORKDIR ${HOME}

COPY --chown=${NB_USER}:${NB_USER} . ${HOME}

# Installation Blender + lien symbolique + ajout au .bashrc
RUN rm -rf "$HOME/blender" && \
    wget -q https://download.blender.org/release/Blender3.6/blender-3.6.23-linux-x64.tar.xz && \
    mkdir -p "$HOME/blender" && \
    tar -xf blender-3.6.23-linux-x64.tar.xz -C "$HOME/blender" --strip-components=1 && \
    rm blender-3.6.23-linux-x64.tar.xz && \
    mkdir -p "$HOME/.local/bin" && \
    ln -sf "$HOME/blender/blender" "$HOME/.local/bin/blender" && \
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$HOME/.bashrc"

RUN pip install --no-cache-dir notebook

RUN chmod +x ${HOME}/binder/start

EXPOSE 8888

ENTRYPOINT ["binder/start"]
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser"]
