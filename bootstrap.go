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
CREATE OR REPLACE FUNCTION text2ts(text) RETURNS timestamp with time zone LANGUAGE sql IMMUTABLE AS
$$
    SELECT CASE WHEN $1 ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:.\d+)?' -- extract date-time part of the RFC3339 value
        THEN CAST($1 AS timestamp with time zone) 
            at time zone coalesce((regexp_match($1, '(Z|[+\-]?\d\d:\d\d)$'))[1], 'utc') -- extract tz part of the RFC3339 value
    END
$$;
`
	createObjectsQuery = `
CREATE TABLE object (
  "raw" jsonb,
  "iri" varchar NOT NULL constraint object_key unique,
  "id" varchar GENERATED ALWAYS AS (raw ->> 'id') STORED,
  "type" varchar GENERATED ALWAYS AS (raw ->> 'type') STORED ,
  "to" varchar GENERATED ALWAYS AS (raw ->> 'to') STORED ,
  "bto" varchar GENERATED ALWAYS AS (raw ->> 'bto') STORED ,
  "cc" varchar GENERATED ALWAYS AS (raw ->> 'cc') STORED ,
  "bcc" varchar GENERATED ALWAYS AS (raw ->> 'bcc') STORED ,
  "published" timestamptz GENERATED ALWAYS AS (text2ts(raw ->> 'published')) STORED,
  "updated" timestamptz GENERATED ALWAYS AS (text2ts(raw ->> 'updated')) STORED ,
  "url" varchar GENERATED ALWAYS AS (raw ->> 'url') STORED ,
  "name" varchar GENERATED ALWAYS AS (raw ->> 'name') STORED ,
  "preferred_username" text GENERATED ALWAYS AS (raw ->> 'preferredUsername') STORED ,
  "summary" varchar GENERATED ALWAYS AS (raw ->> 'summary') STORED ,
  "content" varchar GENERATED ALWAYS AS (raw ->> 'content') STORED ,
  "actor" varchar GENERATED ALWAYS AS (raw ->> 'actor') STORED ,
  "object" varchar GENERATED ALWAYS AS (raw ->> 'object') STORED 
);
CREATE INDEX object_type ON object(type);
CREATE INDEX object_names ON object USING GIN (tsvector_concat(to_tsvector('english', name), to_tsvector('english', preferred_username)));
CREATE INDEX object_contents ON object USING GIN (to_tsvector('english', summary)); 
-- CREATE INDEX object_contents ON object USING GIN (to_tsvector('english', content));
CREATE INDEX object_published ON object(published);
`

	createCollectionsQuery = `
CREATE TABLE collection (
  "id" varchar references object(iri),
  "iri" varchar NOT NULL,
  "added" timestamptz default (now() at time zone 'utc')
);

-- CREATE TRIGGER collections_updated_published AFTER UPDATE ON collection BEGIN
-- UPDATE object SET updated = NOW() WHERE iri = old.id;
-- END;
`

	createMetaDataQuery = `CREATE TABLE "meta" (
  "iri" varchar NOT NULL constraint meta_key unique,
  "raw" jsonb NOT NULL DEFAULT '{}'
);`

	createClientTable = `CREATE TABLE IF NOT EXISTS "client"(
	"code" varchar constraint client_code_pkey PRIMARY KEY,
	"secret" varchar NOT NULL,
	"redirect_uri" varchar NOT NULL,
	"extra" varchar
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
	"created_at" timestamptz DEFAULT (now() at time zone 'utc'),
	"extra" varchar
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
	"created_at" timestamptz DEFAULT (now() at time zone 'utc'),
	"extra" varchar
);
`

	createRefreshTable = `CREATE TABLE IF NOT EXISTS "refresh" (
	"token" varchar PRIMARY KEY NOT NULL,
	"access_token" varchar NOT NULL REFERENCES access(token) ON DELETE CASCADE 
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
	defer r.Close()

	if err = r.conn.Ping(); err != nil {
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

var tables = []string{
	"object", "collection",
	"meta",
	"client", "authorize", "access", "refresh",
}

func (r *repo) Reset() {
	if r.conn == nil {
		r.errFn("connection is not open")
		return
	}

	tx, err := r.conn.Begin()
	if err != nil {
		r.errFn("unable to start transaction: %s", err)
		return
	}

	for _, table := range tables {
		s := `TRUNCATE TABLE "` + table + `" CASCADE;`
		if _, err = tx.Exec(s); err != nil {
			_ = tx.Rollback()
			r.errFn("unable to truncate table %s: %s", table, err)
		}
	}
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		r.errFn("unable to commit transaction item: %+s", err)
	}
}

func Clean(conf Config) error {
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
	if err = r.conn.Ping(); err != nil {
		return err
	}
	defer r.Close()

	for _, table := range tables {
		if _, err = r.conn.Exec(`DROP TABLE "` + table + `"`); err != nil {
			r.errFn("unable to drop table %s: %s", table, err)
		}
	}
	return nil
}
