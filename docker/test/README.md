# How to run the test suite using docker

This process is very similar to that of running an ingest job in docker (see [docker/README.md](docker/README.md)).

The ingest-test image is based on the ingest docker image found in the docker directory. As such, many of the same requirements hold when running the tests as when a normal ingest job takes place.

Some notes...
An iRODS server is assumed to be running on an accessible network.
A Redis server is assumed to be running on an accessible network.
The test suite starts and stops its own workers, so it is advisable to stop any workers pointed at the Redis server you plan to use, or use a different database.

## Step 1: Build
Build the ingest image:
```
$ docker build -t ingest ..
```

Build the ingest-test image (uses ingest image as a base so as to be identical):
```
$ docker build -t ingest-test -f Dockerfile.test .
```

## Step 2: Run the tests:
Need a file like this from wherever the tests are launched:
```
$ cat icommands.env
IRODS_PORT=1247
IRODS_HOST=icat.example.org
IRODS_USER_NAME=rods
IRODS_ZONE_NAME=tempZone
IRODS_PASSWORD=rods
```
In fact, the default values used by the tests for iRODS interactions are as shown above and should not be changed.

This will serve as the iRODS client authentication (rather than running `iinit` somewhere).
Also need a valid irods_environment.json file.

To run the full test suite, run this:
```
docker run --rm --env-file icommands.env -v /path/to/mountdir:/tmp/testdir ingest-test
```
`/path/to/mountdir` must be accessible to both the ingest-test container and wherever iRODS is running.
