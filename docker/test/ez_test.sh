#! /bin/bash
# Use this if you have a redis container and an iRODS container with these names!
./run_tests.sh --redis-container some-redis --irods-container irods-box ${1}
