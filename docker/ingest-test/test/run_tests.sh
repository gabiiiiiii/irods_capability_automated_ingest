#! /bin/bash

# Wait until the provider is up and accepting connections.
until nc -z icat.example.org 1247; do
    sleep 1
done

python -m unittest ${TEST_CASE}
