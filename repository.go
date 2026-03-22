package pg

import (
	"database/sql"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
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

func (r *repo) Open() error {
	return r.open()
}

func (r *repo) open() error {
	var err error
	r.conn, err = sql.Open("postgres", r.conf.DSN())
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
