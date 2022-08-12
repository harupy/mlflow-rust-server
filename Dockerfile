FROM rust:1.62

WORKDIR /app

RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh
RUN bash /tmp/miniconda.sh -b -p ~/.miniconda
RUN rm -rf /tmp/miniconda.sh
ENV PATH="/root/.miniconda/bin:$PATH"
RUN python --version
RUN pip install mlflow psycopg2

COPY . .
RUN MLFLOW_TRACKING_URI=sqlite:////tmp/mlflowdb.sqlite python generate_data.py
RUN cargo install --path .
