FROM python:3.10-slim

ARG NB_USER=jovyan
ARG NB_UID=1000
ENV USER=${NB_USER} \
    HOME=/home/${NB_USER} \
    PATH="/home/${NB_USER}/.local/bin:/home/${NB_USER}/blender:${PATH}" \
    PIP_NO_CACHE_DIR=1

# Dépendances système (ajout ca-certificates pour HTTPS)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
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

# Installer Blender en ROOT avant le COPY
WORKDIR /opt
RUN wget https://download.blender.org/release/Blender3.6/blender-3.6.23-linux-x64.tar.xz && \
    tar -xf blender-3.6.23-linux-x64.tar.xz && \
    mv blender-3.6.23-linux-x64 /opt/blender && \
    rm blender-3.6.23-linux-x64.tar.xz && \
    ln -s /opt/blender/blender /usr/local/bin/blender

USER ${NB_USER}
WORKDIR ${HOME}

COPY --chown=${NB_USER}:${NB_USER} . ${HOME}

RUN pip install --no-cache-dir notebook

RUN chmod +x ${HOME}/binder/start

EXPOSE 8888

ENTRYPOINT ["binder/start"]
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser"]
