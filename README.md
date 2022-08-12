## Commands

MLflow Tracking server implemented in Rust (for learning purposes).

```bash
# Launch DB and log some data
docker compose up

# Launch server
cargo run

# Enter the postgres container and run SQL
docker compose exec postgres bash
psql -U mlflowuser mlflowdb
SELECT * FROM metrics

# Clean up
docker-compose down --volumes --remove-orphans
docker-compose down --volumes --remove-orphans  --rmi all

cargo run -- --backend-store-uri sqlite://mlflowdb.sqlite --default-artifact-root ./mlruns
```
