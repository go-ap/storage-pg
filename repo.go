package sqlite

import (
	"database/sql"

	"github.com/go-ap/errors"
)

type Config struct {
	User string
	Pw   string
	Host string
	DB   string
}

func (c Config) DSN() string {
	return "postgres://" + c.User + ":" + c.Pw + "@" + c.Host + "/" + c.DB + "?sslmode=disable"
}

type repo struct {
	conn *sql.DB
	conf Config

	logFn func(string, ...any)
	errFn func(string, ...any)
}

func New(c Config) (*repo, error) {
	return nil, errors.NotImplementedf("implement me!")
}

func (r *repo) open() error {
	var err error
	r.conn, err = sql.Open("postgres", r.conf.DSN())
	return err
}
