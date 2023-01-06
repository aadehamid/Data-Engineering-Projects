FROM jupyter/base-notebook
WORKDIR /src/notebooks
COPY requirements.txt .
RUN python3 -m pip install -r requirements.txt && \
    conda install -c anaconda psycopg2 && \
    conda install -c conda-forge python-duckdb && \
    conda install -c conda-forge dask && \
    fugue-jupyter install startup
    # python3 -m pip install duckdb
    # python3 -m pip install "dask[complete]"
