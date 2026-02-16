#!/bin/bash
set -euox pipefail

# ephermal postgres instance
CONTAINER=pg
docker rm -f $CONTAINER 2>/dev/null || true
docker run -d --name $CONTAINER --rm -e POSTGRES_PASSWORD=pass -p 5432:5432 postgres:17-alpine >/dev/null
until docker exec $CONTAINER pg_isready >/dev/null 2>&1; do sleep 1; done;

# self contained script with uv inline dependencies
uv run demo.py

docker stop $CONTAINER >/dev/null
