FROM jupyter/base-notebook
WORKDIR /hamidadesokan/src
COPY ./requirements.txt .
COPY ./install-packages.sh .
USER root
RUN --mount=type=cache,target=/root/.cache \
    python3 -m pip install -r requirements.txt && \
    conda install -c anaconda psycopg2 && \
    conda install -c conda-forge python-duckdb && \
    fugue-jupyter install startup && \
    conda install -c conda-forge dask &&   \
    conda install -c anaconda ipykernel

RUN ./install-packages.sh
