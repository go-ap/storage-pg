package pg

import (
	"context"
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

	col, err := r.save(tx, it)
	if err != nil {
		_ = tx.Rollback()
		return it, errors.Annotatef(err, "failed to save item")
	}
	if err = tx.Commit(); err != nil {
		err = errors.Annotatef(err, "failed to commit transaction")
	}
	it = col.Normalize()
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

// Create saves a collection
// Deprecated
func (r *repo) Create(col vocab.CollectionInterface) (vocab.CollectionInterface, error) {
	it, err := r.Save(col)
	return it.(vocab.CollectionInterface), err
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

	sres, err := pgs.Select("iri").From("pub.object").Where("iri = ?", col).
		ExecAndClose(context.Background(), tx)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if cnt, err := sres.RowsAffected(); cnt == 0 || err != nil {
		_ = tx.Rollback()
		return errors.NotFoundf("unable load collection %s", col)
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

	sres, err := pgs.Select("iri").From("pub.object").Where("iri = ?", col).
		ExecAndClose(context.Background(), tx)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if cnt, err := sres.RowsAffected(); cnt == 0 || err != nil {
		_ = tx.Rollback()
		return errors.NotFoundf("unable load collection %s", col)
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

func (r *repo) Load(iri vocab.IRI, checks ...filters.Check) (vocab.Item, error) {
	if r == nil || r.conn == nil {
		return nil, errInvalidConnection
	}

	it, err := loadFromDb(r.conn, iri, checks...)
	if err != nil {
		return nil, err
	}

	it = filters.Checks(checks).Run(it)
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

func (r *repo) save(tx preparer, items ...vocab.Item) (vocab.ItemCollection, error) {
	if len(items) == 0 {
		return nil, nil
	}

	q := pgs.InsertInto("pub.object").
		Clause("ON CONFLICT ON CONSTRAINT object_key DO UPDATE SET raw = excluded.raw")

	for _, it := range items {
		if vocab.IsNil(it) {
			return nil, nil
		}

		iri := it.GetLink()

		raw, err := encodeItemFn(it)
		if err != nil {
			return nil, errors.Annotatef(err, "unable to encode item")
		}

		q.Set("iri", iri).Set("raw", raw)
	}

	st, err := tx.Prepare(q.String())
	if err != nil {
		return nil, errors.Annotatef(err, "unable to prepare statement")
	}
	defer st.Close()

	if _, err = st.Exec(q.Args()...); err != nil {
		return items, errors.Annotatef(err, "query execution error")
	}

	colMap := make(map[vocab.IRI]vocab.ItemCollection)
	for _, it := range items {
		// NOTE(marius): we don't use vocab.Split because we want the path without last element,
		// which might be a collection
		if col, _ := filepath.Split(it.GetLink().String()); isCollectionIRI(vocab.IRI(col)) {
			// Add private items to the collection table they part of (eg. ~jdoe/outbox/1 -> ~jdoe/outbox)
			if colIRI, k := vocab.Split(vocab.IRI(col)); k == "" {
				col, ok := colMap[colIRI]
				if !ok {
					col = make(vocab.ItemCollection, 0)
				}
				_ = col.Append(it)
				colMap[colIRI] = col
			}
		}
	}

	for colIRI, colItems := range colMap {
		if err = r.addTo(tx, colIRI, colItems...); err != nil {
			r.logFn("failed adding items to collection: %s: %s", colIRI, err)
		}
	}

	return items, err
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

func loadMultipleObjects(tx preparer, iris ...vocab.IRI) (vocab.ItemCollection, error) {
	params := make([]any, 0, len(iris))
	for _, iri := range iris {
		params = append(params, iri)
	}
	q := pgs.From("pub.object").
		Select("iri").
		Select("raw").
		Where("iri").In(params...)

	st, err := tx.Prepare(q.String())
	if err != nil {
		return nil, errors.Annotatef(err, "unable to prepare statement")
	}
	defer st.Close()

	rows, err := st.Query(q.Args()...)
	if err != nil {
		return nil, errors.Annotatef(err, "query execution error")
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NotFoundf("Unable to load %s", iris)
		}
		return nil, errors.Annotatef(err, "unable to load object %s", iris)
	}
	defer rows.Close()

	col := make(vocab.ItemCollection, 0, len(iris))
	// Iterate through the result set
	for rows.Next() {
		var iri vocab.IRI
		var raw sql.NullString

		if err = rows.Scan(&iri, &raw); err != nil {
			return nil, errors.Annotatef(err, "scan values error")
		}

		var it vocab.Item
		if raw.Valid {
			if it, _ = decodeItemFn([]byte(raw.String)); !vocab.IsNil(it) {
				_ = col.Append(it)
			}
		}
	}

	return col, nil
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
	}
	if vocab.IsNil(it) {
		return nil, errNotFound
	}

	return it, nil
}

func loadFirstLevelProperties(tx preparer, checks ...filters.Check) func(vocab.Item) error {
	return func(it vocab.Item) error {
		var err error
		typ := it.GetType()
		switch {
		case vocab.IntransitiveActivityTypes.Match(typ):
			err = vocab.OnIntransitiveActivity(it, loadFilteredPropsForIntransitiveActivity(tx, checks...))
		case vocab.ActivityTypes.Match(typ):
			err = vocab.OnActivity(it, loadFilteredPropsForActivity(tx, checks...))
		case vocab.ActorTypes.Match(typ):
			err = vocab.OnActor(it, loadFilteredPropsForActor(tx, checks...))
		case vocab.ObjectTypes.Match(typ):
			err = vocab.OnObject(it, loadFilteredPropsForObject(tx, checks...))
		case orderedCollectionTypes.Match(typ):
			err = vocab.OnOrderedCollection(it, loadFilteredItemsForOrderedCollection(tx, checks...))
		case collectionTypes.Match(typ):
			err = vocab.OnCollection(it, loadFilteredItemsForCollection(tx, checks...))
		}
		return err
	}
}

func loadFilteredItemsForOrderedCollection(tx preparer, checks ...filters.Check) vocab.WithOrderedCollectionFn {
	return func(col *vocab.OrderedCollection) error {
		var err error
		col.OrderedItems, err = loadCollectionItems(tx, col.ID, checks...)
		return err
	}
}
func loadFilteredItemsForCollection(tx preparer, checks ...filters.Check) vocab.WithCollectionFn {
	return func(col *vocab.Collection) error {
		var err error
		col.Items, err = loadCollectionItems(tx, col.ID, checks...)
		return err
	}
}

func loadFilteredPropsForObject(tx preparer, checks ...filters.Check) vocab.WithObjectFn {
	tagChecks := filters.TagChecks(checks...)
	if len(tagChecks) == 0 {
		tagChecks = filters.Checks{filters.NoType}
	}
	return func(o *vocab.Object) error {
		var err error
		if !vocab.IsNil(o.Tag) && len(tagChecks) > 0 {
			o.Tag, err = loadItemFromDB(tx, o.Tag, tagChecks...)
		}
		return err
	}
}

func loadFilteredPropsForActor(tx preparer, checks ...filters.Check) vocab.WithActorFn {
	return func(a *vocab.Actor) error {
		return vocab.OnObject(a, loadFilteredPropsForObject(tx, checks...))
	}
}

func loadFilteredPropsForIntransitiveActivity(tx preparer, checks ...filters.Check) vocab.WithIntransitiveActivityFn {
	return func(a *vocab.IntransitiveActivity) error {
		return vocab.OnObject(a, loadFilteredPropsForObject(tx, checks...))
	}

}

func loadFilteredPropsForActivity(tx preparer, checks ...filters.Check) vocab.WithActivityFn {
	return func(a *vocab.Activity) error {
		return vocab.OnIntransitiveActivity(a, loadFilteredPropsForIntransitiveActivity(tx, checks...))
	}
}

var (
	orderedCollectionTypes = vocab.ActivityVocabularyTypes{vocab.OrderedCollectionPageType, vocab.OrderedCollectionType}
	collectionTypes        = vocab.ActivityVocabularyTypes{vocab.CollectionPageType, vocab.CollectionType}
)

var pgs = sqlf.PostgreSQL

func loadCollectionItems(tx preparer, iri vocab.IRI, checks ...filters.Check) (vocab.ItemCollection, error) {
	s := pgs.From("pub.object o")
	s.Select("o.id")
	s.Select("o.raw")
	s.Join("pub.collection c", "c.iri = o.iri")
	_ = filters.SQLWhere(s, checks...)
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

	if len(ret) > 0 {
		err = vocab.OnItem(ret, loadFirstLevelProperties(tx, checks...))
	}

	return ret, err
}

func loadItemFromDB(tx preparer, it vocab.Item, checks ...filters.Check) (vocab.Item, error) {
	res := make(vocab.ItemCollection, 0)
	err := vocab.OnItem(it, func(iit vocab.Item) error {
		if vocab.IsNil(iit) {
			return nil
		}

		if vocab.IsIRI(iit) {
			ob, err := loadSingleObject(tx, iit.GetLink())
			if err != nil {
				return nil
			}
			if ob = filters.Checks(checks).Run(ob); ob != nil {
				iit = ob
			}
		}
		return res.Append(iit)
	})
	return res.Normalize(), err
}

func loadFromDb(tx preparer, iri vocab.IRI, checks ...filters.Check) (vocab.Item, error) {
	it, err := loadSingleObject(tx, iri)
	if err != nil {
		return nil, errors.NewNotFound(err, "not found")
	}
	if it == nil || vocab.IsNil(it) {
		return nil, errors.NewNotFound(errNilItem, "not found")
	}

	if err = vocab.OnItem(it, loadFirstLevelProperties(tx, checks...)); err != nil {
		return nil, err
	}

	if !vocab.IsCollection(it) {
		return it, nil
	}
	typ := it.GetType()
	switch {
	case orderedCollectionTypes.Match(typ):
		_ = vocab.OnOrderedCollection(it, func(col *vocab.OrderedCollection) error {
			col.ID = iri
			return nil
		})
	case collectionTypes.Match(typ):
		_ = vocab.OnCollection(it, func(col *vocab.Collection) error {
			col.ID = iri
			return nil
		})
	}

	return it, nil
}

var errNilItem = errors.Errorf("nil item")
var errNotFound = errors.NotFoundf("not found")

func (r *repo) addTo(tx preparer, col vocab.IRI, items ...vocab.Item) error {
	if len(items) == 0 {
		return nil
	}

	colIt, err := loadSingleObject(tx, col)
	if err != nil {
		return err
	}

	iris := vocab.ItemCollection(items).IRIs()
	local, _ := loadMultipleObjects(tx, iris...)
	localIRIs := local.IRIs()
	toSaveLocally := make(vocab.ItemCollection, 0, len(items)-len(local))

	cIns := pgs.InsertInto("pub.collection")
	for _, it := range items {
		if vocab.IsNil(it) {
			continue
		}
		cIns.NewRow().Set("id", col).Set("iri", it.GetLink())
		if localIRIs.Contains(it.GetLink()) {
			continue
		}
		_ = toSaveLocally.Append(it)
	}

	if len(toSaveLocally) > 0 {
		// NOTE(marius): save items that don't exist locally
		var err error
		if toSaveLocally, err = r.save(tx, toSaveLocally...); err != nil {
			return errors.Annotatef(err, "unable to save non local items")
		}
	}

	st, err := tx.Prepare(cIns.String())
	if err != nil {
		return errors.Annotatef(err, "unable to prepare collection insert statement")
	}
	defer st.Close()

	args := cIns.Args()
	if _, err = st.Exec(args...); err != nil {
		return errors.Annotatef(err, "unable to append item to collection")
	}

	// NOTE(marius): update collection object with the correct number of items
	return vocab.OnCollection(colIt, func(col *vocab.Collection) error {
		col.TotalItems = uint(len(items))
		_, err = r.save(tx, col)
		return err
	})
}
