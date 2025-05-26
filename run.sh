#!/bin/bash

docker build -t etl-server .


docker run --rm -it \
  --network=host \
  -v "$(pwd)/logs:/app/logs" \
  etl-server "$@"

