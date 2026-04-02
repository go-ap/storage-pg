package pg

import (
	"database/sql"
	"net/url"
	"path/filepath"
	"strconv"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
	"github.com/leporo/sqlf"

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

func emptyLogFn(_ string, _ ...any) {}

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
	if r == nil || r.conn == nil {
		return nil, errInvalidConnection
	}
	if vocab.IsNil(it) {
		return nil, errNilItem
	}

	tx, err := r.conn.Begin()
	if err != nil {
		return nil, errors.Annotatef(err, "failed to start transaction")
	}

	if it, err = r.save(tx, it); err != nil {
		_ = tx.Rollback()
		return it, errors.Annotatef(err, "failed to save item")
	}

	if err = tx.Commit(); err != nil {
		err = errors.Annotatef(err, "failed to commit transaction")
	}
	return it, err
}

func (r *repo) Delete(it vocab.Item) error {
	if r == nil || r.conn == nil {
		return errInvalidConnection
	}
	if vocab.IsNil(it) {
		return errNilItem
	}
	tx, err := r.conn.Begin()
	if err != nil {
		return errors.Annotatef(err, "failed to start transaction")
	}

	if vocab.IsItemCollection(it) {
		err = vocab.OnCollectionIntf(it, func(c vocab.CollectionInterface) error {
			return r.delete(tx, c.Collection()...)
		})
	} else {
		err = r.delete(tx, it)
	}

	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		return errors.Annotatef(err, "failed to commit transaction")
	}
	return nil
}

func (r *repo) delete(tx *sql.Tx, items ...vocab.Item) error {
	if len(items) == 0 {
		return nil
	}

	params := make([]any, 0, len(items))
	for _, it := range items {
		if vocab.IsNil(it) {
			continue
		}
		params = append(params, it.GetLink())
	}

	q := pgs.DeleteFrom("pub.object")
	q.Where("iri").In(params...)
	st, err := tx.Prepare(q.String())
	if err != nil {
		return errors.Annotatef(err, "unable to prepare statement")
	}
	defer st.Close()

	if _, err := st.Exec(params...); err != nil {
		return errors.Annotatef(err, "unable to delete items")
	}

	return nil
}

func (r *repo) Create(col vocab.CollectionInterface) (vocab.CollectionInterface, error) {
	if r == nil || r.conn == nil {
		return nil, errInvalidConnection
	}

	if vocab.IsNil(col) {
		return nil, errNilItem
	}
	if col.GetLink() == "" {
		return col, errNilItem
	}
	tx, err := r.conn.Begin()
	if err != nil {
		return nil, errors.Annotatef(err, "failed to start transaction")
	}
	if _, err = r.save(tx, col); err != nil {
		_ = tx.Rollback()
		return nil, errors.Annotatef(err, "failed to create collection")
	}
	if err = tx.Commit(); err != nil {
		return nil, errors.Annotatef(err, "failed to commit transaction")
	}
	return col, nil
}

var errInvalidCollection = errors.Errorf("invalid collection IRI")

func (r *repo) AddTo(col vocab.IRI, items ...vocab.Item) error {
	if r == nil || r.conn == nil {
		return errInvalidConnection
	}

	if col == "" {
		return errInvalidCollection
	}

	tx, err := r.conn.Begin()
	if err != nil {
		return errors.Annotatef(err, "failed to start transaction")
	}

	if err = r.addTo(tx, col, items...); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		return errors.Annotatef(err, "failed to commit transaction")
	}
	return nil
}

func (r *repo) RemoveFrom(col vocab.IRI, items ...vocab.Item) error {
	if r == nil || r.conn == nil {
		return errInvalidConnection
	}

	tx, err := r.conn.Begin()
	if err != nil {
		return errors.Annotatef(err, "failed to start transaction")
	}

	if err = r.removeFrom(tx, col, items...); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		return errors.Annotatef(err, "failed to commit transaction")
	}
	return nil
}

func (r *repo) removeFrom(tx *sql.Tx, col vocab.IRI, items ...vocab.Item) error {
	delSt := pgs.DeleteFrom("pub.collection")
	delSt.Where("id = ?", col)
	iris := make([]any, 0, len(items))
	for _, iri := range vocab.ItemCollection(items).IRIs() {
		iris = append(iris, iri)
	}
	delSt.Where("iri").In(iris...)

	delSQL := delSt.String()
	args := delSt.Args()
	st, err := tx.Prepare(delSQL)
	if err != nil {
		return err
	}
	defer st.Close()

	rows, err := st.Query(args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	return nil
}

type preparer interface {
	Prepare(query string) (*sql.Stmt, error)
}

func (r *repo) Load(iri vocab.IRI, ff ...filters.Check) (vocab.Item, error) {
	if r == nil || r.conn == nil {
		return nil, errInvalidConnection
	}

	it, err := loadFromDb(r.conn, iri, ff...)
	if err != nil {
		return nil, err
	}

	it = filters.Checks(ff).Run(it)
	if col, ok := it.(vocab.ItemCollection); ok && col.Count() == 1 {
		return col.First(), nil
	}
	return it, nil
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

var collectionPaths = append(vocab.ActivityPubCollections, filters.BlockedType, filters.IgnoredType)

func isCollectionIRI(iri vocab.IRI) bool {
	lst := vocab.CollectionPath(filepath.Base(iriPath(iri)))
	return collectionPaths.Contains(lst)
}

func (r *repo) save(tx *sql.Tx, it vocab.Item) (vocab.Item, error) {
	if vocab.IsNil(it) {
		return nil, nil
	}

	iri := it.GetLink()

	raw, err := encodeItemFn(it)
	if err != nil {
		return it, errors.Annotatef(err, "query error")
	}

	q := pgs.InsertInto("pub.object").
		Set("iri", iri).
		Set("raw", raw).
		Clause("ON CONFLICT ON CONSTRAINT object_key DO UPDATE SET raw = excluded.raw")
	st, err := tx.Prepare(q.String())
	if err != nil {
		return nil, errors.Annotatef(err, "unable to prepare statement")
	}
	defer st.Close()

	if _, err = st.Exec(q.Args()...); err != nil {
		return it, errors.Annotatef(err, "query execution error")
	}

	// NOTE(marius): we don't use vocab.Split because we want the path without last element,
	// which might be a collection
	if col, _ := filepath.Split(iri.String()); isCollectionIRI(vocab.IRI(col)) {
		// Add private items to the collections table
		if colIRI, k := vocab.Split(vocab.IRI(col)); k == "" {
			if err = r.addTo(tx, colIRI, it); err != nil {
				r.logFn("failed adding item to collection: %s: %s", colIRI, err)
			}
		}
	}

	return it, err
}

func cleanIRI(i vocab.IRI) vocab.IRI {
	u, err := i.URL()
	if err != nil {
		return i
	}
	u = &url.URL{
		Scheme:     u.Scheme,
		Opaque:     u.Opaque,
		User:       u.User,
		Host:       u.Host,
		Path:       u.Path,
		RawPath:    u.RawPath,
		OmitHost:   u.OmitHost,
		ForceQuery: u.ForceQuery,
	}

	return vocab.IRI(u.String())
}

const selOneQ = "SELECT id, raw FROM pub.object WHERE id = $1;"

func loadSingleObject(tx preparer, iri vocab.IRI) (vocab.Item, error) {
	st, err := tx.Prepare(selOneQ)
	if err != nil {
		return nil, errors.Annotatef(err, "unable to load object %s", iri)
	}
	defer st.Close()

	rows, err := st.Query(cleanIRI(iri))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NotFoundf("Unable to load %s", iri)
		}
		return nil, errors.Annotatef(err, "unable to load object %s", iri)
	}
	defer rows.Close()

	var it vocab.Item
	// Iterate through the result set
	for rows.Next() {
		var iri vocab.IRI
		var raw sql.NullString

		if err = rows.Scan(&iri, &raw); err != nil {
			return nil, errors.Annotatef(err, "scan values error")
		}

		if raw.Valid {
			it, err = decodeItemFn([]byte(raw.String))
			if err != nil {
				return nil, errors.Annotatef(err, "unable to unmarshal raw item")
			}
		}
		if vocab.IsNil(it) {
			return nil, errors.Annotatef(errNilItem, "IRI %s", iri)
		}
		it = loadFirstLevelProperties(tx, it)
	}
	if vocab.IsNil(it) {
		return nil, errNotFound
	}

	return it, nil
}

func loadFirstLevelProperties(tx preparer, it vocab.Item, ff ...filters.Check) vocab.Item {
	typ := it.GetType()
	if vocab.ActorTypes.Match(typ) {
		return loadActorFirstLevelProperties(tx, it, filters.ActorChecks(ff...)...)
	} else if append(vocab.ActivityTypes, vocab.IntransitiveActivityTypes...).Match(typ) {
		return runActivityFirstLevelProperties(tx, it, filters.ActivityChecks(ff...)...)
	}
	return loadObjectFirstLevelProperties(tx, it, ff...)
}

func loadActorFirstLevelProperties(tx preparer, it vocab.Item, ff ...filters.Check) vocab.Item {
	return loadObjectFirstLevelProperties(tx, it, ff...)
}

func loadObjectFirstLevelProperties(tx preparer, it vocab.Item, ff ...filters.Check) vocab.Item {
	_ = vocab.OnObject(it, loadTagsForObject(tx, ff...))
	return it
}

func loadTagsForObject(tx preparer, ff ...filters.Check) func(o *vocab.Object) error {
	tf := filters.TagChecks(ff...)
	return func(o *vocab.Object) error {
		if len(o.Tag) == 0 {
			return nil
		}
		return vocab.OnItemCollection(o.Tag, func(col *vocab.ItemCollection) error {
			for i, t := range *col {
				if vocab.IsNil(t) || !vocab.IsIRI(t) {
					return nil
				}
				if ob, err := loadFromDb(tx, t.GetLink(), tf...); err == nil && !vocab.IsNil(ob) {
					(*col)[i] = ob
				}
			}
			return nil
		})
	}
}

func runActivityFirstLevelProperties(tx preparer, it vocab.Item, ff ...filters.Check) vocab.Item {
	return it
}

var (
	orderedCollectionTypes = vocab.ActivityVocabularyTypes{vocab.OrderedCollectionPageType, vocab.OrderedCollectionType}
	collectionTypes        = vocab.ActivityVocabularyTypes{vocab.CollectionPageType, vocab.CollectionType}
)

var pgs = sqlf.PostgreSQL

func loadCollectionItems(tx preparer, iri vocab.IRI, ff ...filters.Check) (vocab.ItemCollection, error) {
	s := pgs.From("pub.object o")
	s.Select("o.id")
	s.Select("o.raw")
	s.Join("pub.collection c", "c.iri = o.iri")
	_ = filters.SQLWhere(s, ff...)
	s.Where("c.id = ?", iri)
	//s.OrderBy("COALESCE(o.published, c.added) DESC")

	query := s.String()
	args := s.Args()
	st, err := tx.Prepare(query)
	if err != nil {
		return nil, errors.Annotatef(err, "unable to prepare query")
	}
	defer st.Close()

	rows, err := st.Query(args...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NotFoundf("Unable to load %s", iri)
		}
		return nil, errors.Annotatef(err, "unable to run select")
	}
	defer rows.Close()

	var errs []error
	ret := make(vocab.ItemCollection, 0)
	for rows.Next() {
		var iri string
		var raw sql.NullString
		var it vocab.Item

		if err = rows.Scan(&iri, &raw); err != nil {
			errs = append(errs, errors.Annotatef(err, "scan values error"))
			continue
		}

		if raw.Valid {
			it, err = decodeItemFn([]byte(raw.String))
			if err != nil {
				errs = append(errs, errors.Annotatef(err, "unable to unmarshal raw item %s", iri))
				continue
			}
		}
		if vocab.IsNil(it) {
			errs = append(errs, errors.Annotatef(errNilItem, "IRI %s", iri))
			continue
		}
		ret = append(ret, it)
	}
	if len(errs) > 0 {
		return ret, errors.Join(errs...)
	}
	if len(ret) == 0 {
		return ret, errNotFound
	}

	err = vocab.OnItem(ret, func(item vocab.Item) error {
		item = loadActorFirstLevelProperties(tx, item, ff...)
		item = loadObjectFirstLevelProperties(tx, item, ff...)
		item = runActivityFirstLevelProperties(tx, item, ff...)
		return nil
	})
	return ret, err
}

func loadFromDb(tx preparer, iri vocab.IRI, checks ...filters.Check) (vocab.Item, error) {
	it, err := loadSingleObject(tx, iri)
	if it == nil || vocab.IsNil(it) {
		return nil, errors.NewNotFound(errNilItem, "not found")
	}
	if !it.IsCollection() {
		return it, nil
	}

	items, _ := loadCollectionItems(tx, it.GetLink(), checks...)
	typ := it.GetType()
	if orderedCollectionTypes.Match(typ) {
		err = vocab.OnOrderedCollection(it, func(col *vocab.OrderedCollection) error {
			col.ID = iri
			col.OrderedItems = items
			return nil
		})
	} else if collectionTypes.Match(typ) {
		err = vocab.OnCollection(it, func(col *vocab.Collection) error {
			col.ID = iri
			if col.TotalItems > 0 {
				col.Items = items
			}
			return nil
		})
	}

	if err != nil {
		return nil, err
	}
	return it, nil
}

var errNilItem = errors.Errorf("nil item")
var errNotFound = errors.NotFoundf("not found")

func (r *repo) addTo(tx *sql.Tx, col vocab.IRI, items ...vocab.Item) error {
	if len(items) == 0 {
		return nil
	}

	q := pgs.InsertInto("pub.collection")
	for _, it := range items {
		if vocab.IsNil(it) {
			return errNilItem
		}

		ob, err := loadSingleObject(tx, it.GetLink())
		if err != nil {
			if it, err = r.save(tx, it); err != nil {
				return errors.Annotatef(err, "unable to save item")
			}
			if vocab.IsIRI(it) {
				it = ob
			}
		}
		if vocab.IsNil(it) {
			return errNotFound
		}

		q.NewRow().
			Set("id", col).
			Set("iri", it.GetLink())
	}

	st, err := tx.Prepare(q.String())
	if err != nil {
		return errors.Annotatef(err, "unable to prepare statement")
	}
	defer st.Close()

	args := q.Args()
	if _, err := st.Exec(args...); err != nil {
		return errors.Annotatef(err, "unable to append item to collection")
	}

	// NOTE(marius): update collection object with the correct number of items
	colOb, err := loadSingleObject(tx, col)
	if err != nil {
		return err
	}
	_ = vocab.OnCollection(colOb, func(col *vocab.Collection) error {
		col.TotalItems = uint(len(args) / 2)
		_, err = r.save(tx, col)
		return err
	})

	return nil
}
