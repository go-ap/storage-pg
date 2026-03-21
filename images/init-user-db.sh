#!/usr/bin/env bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	-- CREATE USER storage;
	-- CREATE DATABASE storage;
	-- GRANT ALL PRIVILEGES ON DATABASE storage TO storage;
EOSQL