# ==================== STAGE 1: Builder ====================
FROM python:3.11-slim AS builder

# Installation des outils de build
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir notebook

# Copier et exécuter les installations du binder PENDANT le build
COPY binder/ /tmp/binder/
WORKDIR /tmp

# Exécuter le contenu de start qui installe les dépendances
# (adapter selon ce que fait réellement votre script start)
RUN if [ -f /tmp/binder/requirements.txt ]; then \
        pip install --no-cache-dir -r /tmp/binder/requirements.txt; \
    fi
RUN if [ -f /tmp/binder/postBuild ]; then \
        chmod +x /tmp/binder/postBuild && /tmp/binder/postBuild; \
    fi

# ==================== STAGE 2: Final ====================
FROM python:3.11-slim AS final

ARG NB_USER=jovyan
ARG NB_UID=1000
ENV USER=${NB_USER}
ENV HOME=/home/${NB_USER}
ENV PATH="${HOME}/.local/bin:${PATH}"

RUN useradd -m -u ${NB_UID} ${NB_USER}

# Copier les packages Python installés depuis le builder
COPY --from=builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/

# Copier le projet
COPY --chown=${NB_USER}:${NB_USER} . ${HOME}

USER ${NB_USER}
WORKDIR ${HOME}

EXPOSE 8888

# Lancement DIRECT de jupyter (pas de script d'installation)
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser"]
