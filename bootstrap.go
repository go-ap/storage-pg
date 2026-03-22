package pg

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/go-ap/errors"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
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
  "published" timestamptz GENERATED ALWAYS AS (raw ->> 'published') STORED ,
  "updated" timestamptz GENERATED ALWAYS AS (raw ->> 'updated') STORED ,
  "url" varchar GENERATED ALWAYS AS (raw ->> 'url') STORED ,
  "name" varchar GENERATED ALWAYS AS (raw ->> 'name') STORED ,
  "preferred_username" varchar GENERATED ALWAYS AS (raw ->> 'preferredUsername') STORED ,
  "summary" varchar GENERATED ALWAYS AS (raw ->> 'summary') STORED ,
  "content" varchar GENERATED ALWAYS AS (raw ->> 'content') STORED ,
  "actor" varchar GENERATED ALWAYS AS (raw ->> 'actor') STORED ,
  "object" varchar GENERATED ALWAYS AS (raw ->> 'object') STORED 
);
-- CREATE INDEX objects_type ON objects(type);
-- CREATE INDEX objects_name ON objects(name);
-- CREATE INDEX objects_content ON objects(content);
-- CREATE INDEX objects_published ON objects(published);
`

	createMetaQuery = `CREATE TABLE "meta" (
  "id" varchar constraint meta_key unique,
  "meta" varchar
);`

	createCollectionsQuery = `
CREATE TABLE collections (
  "id" varchar,
  "iri" varchar,
  "published" timestamptz default CURRENT_TIMESTAMP
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
	"created_at" timestamptz DEFAULT CURRENT_TIMESTAMP,
	"extra" jsonb DEFAULT '{}'
);
`

	createAccessTable = `CREATE TABLE IF NOT EXISTS "access" (
	"client" varchar REFERENCES client(code),
	"authorize" varchar REFERENCES authorize(code),
	"previous" varchar,
	"token" varchar NOT NULL,
	"refresh_token" varchar NOT NULL,
	"expires_in" INTEGER,
	"scope" varchar DEFAULT NULL,
	"redirect_uri" varchar NOT NULL,
	"created_at" timestamptz DEFAULT CURRENT_TIMESTAMP,
	"extra" varchar DEFAULT '{}'
);
`

	createRefreshTable = `CREATE TABLE IF NOT EXISTS "refresh" (
	"access_token" varchar NOT NULL REFERENCES access(token),
	"token" varchar PRIMARY KEY NOT NULL
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
	if err = r.open(); err != nil {
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
