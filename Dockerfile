FROM python:3.11-slim

ARG NB_USER=jovyan
ARG NB_UID=1000
ENV USER=${NB_USER}
ENV HOME=/home/${NB_USER}

RUN useradd -m -u ${NB_UID} ${NB_USER}

COPY --chown=${NB_USER}:${NB_USER} . ${HOME}

USER ${NB_USER}
WORKDIR ${HOME}

RUN chmod +x ${HOME}/start

CMD ["./start"]
