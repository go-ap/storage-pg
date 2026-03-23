package pg

import (
	"database/sql"
	"net/url"
	"path/filepath"
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

	logFn loggerFn
	errFn loggerFn
}

var emptyLogFn loggerFn

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

var encodeItemFn = vocab.MarshalJSON
var decodeItemFn = vocab.UnmarshalJSON

type loggerFn func(string, ...any)

func iriPath(iri vocab.IRI) string {
	u, err := iri.URL()
	if err != nil {
		return ""
	}

	pieces := make([]string, 0)
	if h := u.Host; h != "" {
		pieces = append(pieces, h)
	}
	if p := u.Path; p != "" && p != "/" {
		pieces = append(pieces, p)
	}
	if u.Fragment != "" {
		pieces = append(pieces, url.PathEscape(u.Fragment))
	}
	return filepath.Join(pieces...)
}

var collectionPaths = append(filters.FedBOXCollections, append(vocab.OfActor, vocab.OfObject...)...)

func isCollectionIRI(iri vocab.IRI) bool {
	lst := vocab.CollectionPath(filepath.Base(iriPath(iri)))
	return collectionPaths.Contains(lst)
}

const upsertObjectSQL = `INSERT INTO object (iri, raw) VALUES ($1, $2) 
ON CONFLICT ON CONSTRAINT object_key DO UPDATE SET raw = excluded.raw;`

func save(r *repo, it vocab.Item) (vocab.Item, error) {
	if vocab.IsNil(it) {
		return nil, nil
	}

	iri := it.GetLink()

	raw, err := encodeItemFn(it)
	if err != nil {
		r.errFn("query error: %s", err)
		return it, errors.Annotatef(err, "query error")
	}
	tx, err := r.conn.Begin()
	if err != nil {
		return nil, errors.Annotatef(err, "transaction start error")
	}

	params := []any{iri, raw}

	st, err := tx.Prepare(upsertObjectSQL)
	if err != nil {
		return nil, errors.Annotatef(err, "unable to prepare statement")
	}
	defer st.Close()

	if _, err = st.Exec(params...); err != nil {
		return it, errors.Annotatef(err, "query execution error")
	}
	//col, _ := path.Split(iri.String())
	//if isCollectionIRI(vocab.IRI(col)) {
	//	// Add private items to the collections table
	//	if colIRI, k := vocab.Split(vocab.IRI(col)); k == "" {
	//		if err = r.addTo(tx, colIRI, it); err != nil {
	//			r.logFn("warning adding item: %s: %s", colIRI, err)
	//		}
	//	}
	//}

	if err = tx.Commit(); err != nil {
		err = errors.Annotatef(err, "failed to commit transaction")
	}
	return it, err
}
