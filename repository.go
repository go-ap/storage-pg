package pg

import (
	"database/sql"
	"strconv"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"

	// NOTE(marius): we're using the stdlib compatibility layer for the moment for pgx
	_ "github.com/jackc/pgx/v5/stdlib"
)

type Config struct {
	Host     string // host (e.g. localhost) or absolute path to unix domain socket directory (e.g. /private/tmp)
	Port     uint16
	Database string
	User     string
	Password string

	LogFn func(string, ...any)
	ErrFn func(string, ...any)
}

func (c Config) DSN() string {
	if c.Port <= 0 {
		return "postgres://" + c.User + ":" + c.Password + "@" + c.Host + "/" + c.Database + "?sslmode=disable"
	}
	return "postgres://" + c.User + ":" + c.Password + "@" + c.Host + ":" + strconv.Itoa(int(c.Port)) + "/" + c.Database + "?sslmode=disable"
}

type repo struct {
	conn *sql.DB
	conf Config

	logFn func(string, ...any)
	errFn func(string, ...any)
}

func emptyLogFn(_ string, _ ...any) {

}

func New(c Config) (*repo, error) {
	r := repo{
		conn:  nil,
		conf:  c,
		logFn: emptyLogFn,
		errFn: emptyLogFn,
	}
	if c.LogFn != nil {
		r.logFn = c.LogFn
	}
	if c.ErrFn != nil {
		r.errFn = c.ErrFn
	}
	return &r, nil
}

func (r *repo) Open() error {
	return r.open(r.conf.DSN())
}

func (r *repo) open(dsn string) error {
	var err error
	r.conn, err = sql.Open("pgx", dsn)
	return err
}

func (r *repo) Save(it vocab.Item) (vocab.Item, error) {
	return nil, errors.NotImplementedf("implement me")
}

func (r *repo) Delete(it vocab.Item) error {
	return errors.NotImplementedf("implement me")
}

func (r *repo) Create(col vocab.CollectionInterface) (vocab.CollectionInterface, error) {
	return nil, errors.NotImplementedf("implement me")
}

func (r *repo) AddTo(colIRI vocab.IRI, items ...vocab.Item) error {
	return errors.NotImplementedf("implement me")
}

func (r *repo) RemoveFrom(colIRI vocab.IRI, items ...vocab.Item) error {
	return errors.NotImplementedf("implement me")
}

func (r *repo) Load(iri vocab.IRI, check ...filters.Check) (vocab.Item, error) {
	return nil, errors.NotImplementedf("implement me")
}

func save(r *repo, it vocab.Item) (vocab.Item, error) {
	return nil, errors.NotImplementedf("implement me")
}
