#!/usr/bin/env bash

set -e

_image=$(buildah from docker.io/library/postgres:18)

buildah copy "${_image}" ./images/init-db.sql /docker-entrypoint-initdb.d/init-db.sql
buildah copy "${_image}" ./images/init-user-db.sh /docker-entrypoint-initdb.d/init-user-db.sh

# commit
buildah commit "${_image}" "storage-pg"
