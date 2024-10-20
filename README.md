# DuckDB ADBC memory leak test 

Repository to investigate possible memory leak in `DuckDB` with ADBC driver.

The main go file creates a table and in a forever loop alternatively ingests a set of rows and deletes all rows.

## Build and run

Build and start the docker container

```bash
docker build -t duckdb-test .
docker run --rm -it -m 600m --mount type=bind,source="$(pwd)",target=/app,readonly duckdb-test sh
```

In the container run

```bash
go run main.go
```

In a second terminal run

```
docker container stats CONTAINER_ID
```

where `CONTAINER_ID` can be found using `docker ps`.

## Expected output

The container memory is expected to stabilize at some point since the database only contains a limited number of rows in one table. However, the memory seem to grow unbounded until the container gets OOM killed.
