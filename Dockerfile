FROM python:3.11-slim

ARG NB_USER=jovyan
ARG NB_UID=1000
ENV USER=${NB_USER}
ENV HOME=/home/${NB_USER}
ENV PATH="${HOME}/.local/bin:${PATH}"

RUN useradd -m -u ${NB_UID} ${NB_USER}

# Jupyter = garde le conteneur vivant
RUN pip install notebook

COPY --chown=${NB_USER}:${NB_USER} . ${HOME}

USER ${NB_USER}
WORKDIR ${HOME}

RUN chmod +x ${HOME}/binder/start

CMD ["binder/start"]
