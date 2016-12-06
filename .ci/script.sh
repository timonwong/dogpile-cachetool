#!/bin/bash
set -eux

redis-server --version

if [[ "$TOXENV" =~ -rediscluster$ ]]; then
    echo "Starting redis cluster"
    make redis-install
    make start
fi

tox -vv

if [[ "$TOXENV" =~ -rediscluster$ ]]; then
    echo "Stopping redis cluster"
    make stop
fi
