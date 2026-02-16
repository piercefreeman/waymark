#!/bin/bash
set -euox pipefail

CONTAINER=pg
docker rm -f $CONTAINER 2>/dev/null || true
docker run -d --name $CONTAINER --rm -e POSTGRES_PASSWORD=pass -p 5432:5432 postgres:17-alpine >/dev/null
until docker exec $CONTAINER pg_isready >/dev/null 2>&1; do sleep 1; done;

uv run demo.py "postgresql://demo:demo@localhost:5433/demo"

docker stop $CONTAINER >/dev/null
