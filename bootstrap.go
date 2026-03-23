package pg

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/go-ap/errors"
)

const (
	createImmutableTSFunc = `
CREATE FUNCTION text2ts(text) RETURNS timestamp with time zone
    LANGUAGE sql IMMUTABLE AS
$$
    SELECT CASE WHEN $1 ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[Zz]?$'
       THEN CAST($1 AS timestamp with time zone) at time zone 'utc'
    END$$;
`
	createObjectsQuery = `
CREATE TABLE objects (
  "raw" jsonb,
  "iri" varchar NOT NULL constraint objects_key unique,
  "id" varchar GENERATED ALWAYS AS (raw ->> 'id') STORED,
  "type" varchar GENERATED ALWAYS AS (raw ->> 'type') STORED ,
  "to" varchar GENERATED ALWAYS AS (raw ->> 'to') STORED ,
  "bto" varchar GENERATED ALWAYS AS (raw ->> 'bto') STORED ,
  "cc" varchar GENERATED ALWAYS AS (raw ->> 'cc') STORED ,
  "bcc" varchar GENERATED ALWAYS AS (raw ->> 'bcc') STORED ,
  "published" timestamp GENERATED ALWAYS AS (text2ts(raw ->> 'published')) STORED ,
  "updated" timestamptz GENERATED ALWAYS AS (text2ts(raw ->> 'updated')) STORED ,
  "url" varchar GENERATED ALWAYS AS (raw ->> 'url') STORED ,
  "name" text GENERATED ALWAYS AS (raw ->> 'name') STORED ,
  "preferred_username" text GENERATED ALWAYS AS (raw ->> 'preferredUsername') STORED ,
  "summary" text GENERATED ALWAYS AS (raw ->> 'summary') STORED ,
  "content" text GENERATED ALWAYS AS (raw ->> 'content') STORED ,
  "actor" varchar GENERATED ALWAYS AS (raw ->> 'actor') STORED ,
  "object" varchar GENERATED ALWAYS AS (raw ->> 'object') STORED 
);
CREATE INDEX objects_type ON objects(type);
CREATE INDEX objects_names ON objects USING GIN (tsvector_concat(to_tsvector('english', name), to_tsvector('english', preferred_username)));
CREATE INDEX objects_contents ON objects USING GIN (to_tsvector('english', summary)); 
-- CREATE INDEX objects_contents ON objects USING GIN (to_tsvector('english', content));
CREATE INDEX objects_published ON objects(published);
`

	createMetaDataQuery = `CREATE TABLE "meta" (
  "id" varchar NOT NULL constraint meta_key unique,
  "meta" jsonb
);`

	createCollectionsQuery = `
CREATE TABLE collections (
  "id" varchar references objects(iri),
  "iri" varchar,
  "published" timestamptz default now()
);

-- CREATE TRIGGER collections_updated_published AFTER UPDATE ON collections BEGIN
--   UPDATE collections SET published = strftime('%%Y-%%m-%%dT%%H:%%M:%%fZ') WHERE id = old.id;
-- END;`

	createClientTable = `CREATE TABLE IF NOT EXISTS "client"(
	"code" varchar constraint client_code_pkey PRIMARY KEY,
	"secret" varchar NOT NULL,
	"redirect_uri" varchar NOT NULL,
	"extra" jsonb DEFAULT '{}'
);
`

	createAuthorizeTable = `CREATE TABLE IF NOT EXISTS "authorize" (
	"client" varchar REFERENCES client(code),
	"code" varchar constraint authorize_code_pkey PRIMARY KEY,
	"expires_in" INTEGER,
	"scope" varchar,
	"redirect_uri" varchar NOT NULL,
	"state" varchar,
	"code_challenge" varchar DEFAULT NULL,
	"code_challenge_method" varchar DEFAULT NULL,
	"created_at" timestamptz DEFAULT now(),
	"extra" jsonb DEFAULT '{}'
);
`

	createAccessTable = `CREATE TABLE IF NOT EXISTS "access" (
    "token" varchar constraint access_token_pkey PRIMARY KEY,
	"client" varchar REFERENCES client(code),
	"authorize" varchar REFERENCES authorize(code),
	"previous" varchar,
	"refresh_token" varchar NOT NULL,
	"expires_in" INTEGER,
	"scope" varchar DEFAULT NULL,
	"redirect_uri" varchar NOT NULL,
	"created_at" timestamptz DEFAULT now(),
	"extra" varchar DEFAULT '{}'
);
`

	createRefreshTable = `CREATE TABLE IF NOT EXISTS "refresh" (
	"token" varchar PRIMARY KEY NOT NULL,
	"access_token" varchar NOT NULL REFERENCES access(token)
);
`
)

func stringClean(qSql string) string {
	return strings.ReplaceAll(qSql, "\n", "")
}

func Bootstrap(conf Config) error {
	dsn := conf.DSN()
	if dsn == "" {
		return errInvalidConnection
	}

	r, err := New(conf)
	if err != nil {
		return err
	}

	if err = r.open(dsn); err != nil {
		return err
	}

	exec := func(conn *sql.DB, qRaw string, par ...any) error {
		qSql := fmt.Sprintf(qRaw, par...)
		r.logFn("Executing %s", stringClean(qSql))
		if _, err := conn.Exec(qSql); err != nil {
			r.errFn("Failed: %s", err)
			return errors.Annotatef(err, "unable to execute: %s", stringClean(qSql))
		}
		r.logFn("Success!")
		return nil
	}

	if err := exec(r.conn, createImmutableTSFunc); err != nil {
		return err
	}
	if err := exec(r.conn, createObjectsQuery); err != nil {
		return err
	}
	if err := exec(r.conn, createCollectionsQuery); err != nil {
		return err
	}
	if err := exec(r.conn, createMetaDataQuery); err != nil {
		return err
	}
	if err := exec(r.conn, createClientTable); err != nil {
		return err
	}
	if err := exec(r.conn, createAuthorizeTable); err != nil {
		return err
	}
	if err := exec(r.conn, createAccessTable); err != nil {
		return err
	}
	if err := exec(r.conn, createRefreshTable); err != nil {
		return err
	}
	return nil
}

var errInvalidConnection = os.ErrNotExist

func (r repo) Reset() {
}

func Clean(c Config) error {
	return nil
}
