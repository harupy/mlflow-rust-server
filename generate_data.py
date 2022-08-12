import mlflow
import random
import uuid
import sys


for _ in range(10):
    with mlflow.start_run():
        mlflow.log_params(
            {
                "p1": random.random(),
                "p2": random.random(),
            }
        )
        mlflow.log_metrics({"m": random.random()})
        mlflow.set_tags({"t": str(uuid.uuid4())})
