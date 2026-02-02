FROM python:3.10-slim

ARG NB_USER=jovyan
ARG NB_UID=1000
ENV USER=${NB_USER} \
    HOME=/home/${NB_USER} \
    PATH="/home/${NB_USER}/.local/bin:/home/${NB_USER}/blender:${PATH}" \
    PIP_NO_CACHE_DIR=1

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

USER ${NB_USER}
WORKDIR ${HOME}

COPY --chown=${NB_USER}:${NB_USER} . ${HOME}

RUN pip install --no-cache-dir notebook

RUN chmod +x ${HOME}/binder/start

EXPOSE 8888

ENTRYPOINT ["binder/start"]
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser"]
