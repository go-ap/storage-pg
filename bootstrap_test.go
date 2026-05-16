package pg

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/google/go-cmp/cmp"
	"github.com/openshift/osin"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/crypto/bcrypt"
)

func areErrors(a, b any) bool {
	_, ok1 := a.(error)
	_, ok2 := b.(error)
	return ok1 && ok2
}

func compareErrors(x, y any) bool {
	xe, _ := x.(error)
	ye, _ := y.(error)
	if errors.Is(xe, ye) || errors.Is(ye, xe) {
		return true
	}
	return xe.Error() == ye.Error()
}

var EquateWeakErrors = cmp.FilterValues(areErrors, cmp.Comparer(compareErrors))

func areItemCollections(a, b any) bool {
	_, ok1 := a.(vocab.ItemCollection)
	_, ok3 := a.(*vocab.ItemCollection)
	_, ok2 := b.(vocab.ItemCollection)
	_, ok4 := b.(*vocab.ItemCollection)
	return (ok1 || ok3) && (ok2 || ok4)
}

func compareItemCollections(x, y interface{}) bool {
	var i1 vocab.ItemCollection
	var i2 vocab.ItemCollection
	if ic1, ok := x.(vocab.ItemCollection); ok {
		i1 = ic1
	}
	if ic1, ok := x.(*vocab.ItemCollection); ok {
		i1 = *ic1
	}
	if ic2, ok := y.(vocab.ItemCollection); ok {
		i2 = ic2
	}
	if ic2, ok := y.(*vocab.ItemCollection); ok {
		i2 = *ic2
	}
	return i1.Equal(i2)
}

var EquateItemCollections = cmp.FilterValues(areItemCollections, cmp.Comparer(compareItemCollections))

func checkInsertedValue(t *testing.T, db *sql.DB, it vocab.Item) {
	sel := "SELECT id, raw FROM object WHERE id = ?;"
	res, err := db.Query(sel, it.GetLink())
	if err != nil {
		t.Errorf("unable to execute query %s", err)
	}
	if res == nil {
		t.Fatalf("nil result returned by query")
	}
	defer res.Close()

	for res.Next() {
		var id string
		var raw []byte

		err = res.Scan(&id, &raw)
		if err != nil {
			t.Errorf("unable to scan query values %s", err)
		}
		if !it.GetLink().Equal(vocab.IRI(id)) {
			t.Errorf("it.GetLink() %s different than expected, %s", it.GetLink(), id)
		}

		incraw, err := vocab.MarshalJSON(it)
		if err != nil {
			t.Errorf("unable to unmarshal raw value %s", err)
		}

		if string(incraw) != string(raw) {
			t.Errorf("loaded raw value %s different than expected %s", incraw, raw)
		}
	}
}

func setupContainer(t *testing.T) Config {
	ctx := context.Background()

	if dockerHost := os.Getenv("DOCKER_HOST"); dockerHost == "" {
		t.Skipf("no DOCKER_HOST environment variable set to use for go-containers setup")
		return Config{}
	}
	pgContainer, err := postgres.Run(ctx, "postgres:18-alpine",
		postgres.WithInitScripts(filepath.Join("images", "init-db.sql")),
		postgres.WithDatabase("storage"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate pgContainer: %s", err)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("err getting connection string: %s", err)
	}

	conf, err := ParseConfig(connStr)
	if err != nil {
		t.Fatalf("err getting config: %s", err)
	}
	conf.ErrFn = t.Logf

	return conf
}

type fields struct {
	Config
	d *sql.DB
}

type initFn func(*testing.T, *repo) *repo

func withBootstrap(t *testing.T, r *repo) *repo {
	if err := Bootstrap(r.conf); err != nil {
		t.Errorf("unable to bootstrap %s: %+v", r.conf.DSN(), err)
	}
	return r
}

func withCleanup(t *testing.T, r *repo) *repo {
	r.Reset()
	return nil
}

func withOpenRoot(t *testing.T, r *repo) *repo {
	if err := r.Open(); err != nil {
		t.Logf("Could not open db %s: %s", r.conf.DSN(), err)
	}
	return r
}

var (
	mockItems = vocab.ItemCollection{
		vocab.IRI("https://example.com/plain-iri"),
		&vocab.Object{ID: "https://example.com/1", Type: vocab.NoteType},
		&vocab.Place{ID: "https://example.com/arctic", Type: vocab.PlaceType},
		//&vocab.Profile{ID: "https://example.com/~jdoe/profile", Type: vocab.ProfileType},
		&vocab.Link{ID: "https://example.com/1", Href: "https://example.com/1", Type: vocab.LinkType},
		&vocab.Actor{ID: "https://example.com/~jdoe", Type: vocab.PersonType},
		&vocab.Activity{ID: "https://example.com/~jdoe/1", Type: vocab.UpdateType},
		&vocab.Object{ID: "https://example.com/~jdoe/tag-none", Type: vocab.UpdateType},
		&vocab.Question{ID: "https://example.com/~jdoe/2", Type: vocab.QuestionType},
		&vocab.IntransitiveActivity{ID: "https://example.com/~jdoe/3", Type: vocab.ArriveType},
		&vocab.Tombstone{ID: "https://example.com/objects/1", Type: vocab.TombstoneType},
		&vocab.Tombstone{ID: "https://example.com/actors/f00", Type: vocab.TombstoneType},
	}

	pk, _      = rsa.GenerateKey(rand.Reader, 4096)
	pkcs8Pk, _ = x509.MarshalPKCS8PrivateKey(pk)
	key        = pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: pkcs8Pk,
	})

	pubEnc, _  = x509.MarshalPKIXPublicKey(pk.Public())
	pubEncoded = pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubEnc,
	})

	apPublic = &vocab.PublicKey{
		ID:           "https://example.com/~jdoe#main",
		Owner:        "https://example.com/~jdoe",
		PublicKeyPem: string(pubEncoded),
	}

	defaultPw = []byte("dsa")

	encPw, _ = bcrypt.GenerateFromPassword(defaultPw, bcrypt.DefaultCost)
)

func withMockItems(t *testing.T, r *repo) *repo {
	return withItems(mockItems)(t, r)
}

func withMetadataJDoe(t *testing.T, r *repo) *repo {
	m := Metadata{
		Pw:         encPw,
		PrivateKey: key,
	}

	if err := r.SaveMetadata("https://example.com/~jdoe", m); err != nil {
		t.Errorf("unable to save metadata for jdoe: %s", err)
	}
	return r
}

var (
	defaultClient = &osin.DefaultClient{
		Id:          "test-client",
		Secret:      "asd",
		RedirectUri: "https://example.com",
		UserData:    nil,
	}
)

func mockAuth(code string, cl osin.Client) *osin.AuthorizeData {
	return &osin.AuthorizeData{
		Client:    cl,
		Code:      code,
		ExpiresIn: 10,
		CreatedAt: time.Now().Add(10 * time.Minute).Round(10 * time.Minute),
		UserData:  vocab.IRI("https://example.com/jdoe"),
	}
}

func mockAuthWithCodeChallenge(code string, cl osin.Client) *osin.AuthorizeData {
	return &osin.AuthorizeData{
		Client:              cl,
		Code:                code,
		ExpiresIn:           10,
		CreatedAt:           time.Now().Add(10 * time.Minute).Round(10 * time.Minute),
		UserData:            vocab.IRI("https://example.com/jdoe"),
		CodeChallengeMethod: "S256",
		CodeChallenge:       "0000000000000000000000000000000000000000123",
	}
}

func mockAccess(code string, cl osin.Client) *osin.AccessData {
	ad := &osin.AccessData{
		Client:        cl,
		AuthorizeData: mockAuth("test-code", cl),
		AccessToken:   code,
		ExpiresIn:     10,
		Scope:         "none",
		RedirectUri:   "http://localhost",
		CreatedAt:     time.Now().Add(10 * time.Minute).Round(10 * time.Minute),
		UserData:      vocab.IRI("https://example.com/jdoe"),
	}
	if code != "refresh-666" {
		ad.RefreshToken = "refresh-666"
		ad.AccessData = &osin.AccessData{
			Client:      cl,
			AccessToken: "refresh-666",
			ExpiresIn:   10,
			Scope:       "none",
			RedirectUri: "http://localhost",
			CreatedAt:   time.Now().Add(10 * time.Minute).Round(10 * time.Minute),
			UserData:    vocab.IRI("https://example.com/jdoe"),
		}
	}
	return ad
}

func withClient(t *testing.T, r *repo) *repo {
	if err := r.SaveClient(defaultClient); err != nil {
		t.Errorf("failed to save new client: %s", err)
	}
	return r
}

func withAuthorization(t *testing.T, r *repo) *repo {
	if err := r.SaveAuthorize(mockAuth("test-code", defaultClient)); err != nil {
		t.Errorf("failed to create authorization data: %s", err)
	}
	return r
}

func withAccess(t *testing.T, r *repo) *repo {
	if err := r.SaveAccess(mockAccess("refresh-666", defaultClient)); err != nil {
		t.Errorf("failed to create access data: %s", err)
	}
	if err := r.SaveAccess(mockAccess("access-666", defaultClient)); err != nil {
		t.Errorf("failed to create access data: %s", err)
	}
	return r
}

var (
	rootIRI       = vocab.IRI("https://example.com")
	rootInboxIRI  = rootIRI.AddPath(string(vocab.Inbox))
	rootOutboxIRI = rootIRI.AddPath(string(vocab.Outbox))
	root          = &vocab.Actor{
		ID:        rootIRI,
		Type:      vocab.ServiceType,
		Published: publishedTime,
		Name:      vocab.DefaultNaturalLanguage("example.com"),
		Inbox:     rootInboxIRI,
		Outbox:    rootOutboxIRI,
	}

	publishedTime = time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC)

	createCnt     = atomic.Int32{}
	allActors     = atomic.Pointer[vocab.ItemCollection]{}
	allObjects    = atomic.Pointer[vocab.ItemCollection]{}
	allActivities = atomic.Pointer[vocab.ItemCollection]{}
)

func mockCollection(parent vocab.Item, colType vocab.CollectionPath) vocab.CollectionInterface {
	return &vocab.OrderedCollection{
		ID:           colType.Of(parent).GetLink(),
		Type:         vocab.OrderedCollectionType,
		AttributedTo: parent.GetLink(),
		CC:           vocab.ItemCollection{vocab.PublicNS},
		Published:    publishedTime,
	}
}

func withItems(items vocab.ItemCollection) initFn {
	return func(t *testing.T, r *repo) *repo {
		tx, err := r.conn.Begin()
		if err != nil {
			t.Errorf("unable to start transaction item: %+s", err)
			return r
		}
		for _, it := range items {
			if it, err = r.save(tx, it); err != nil {
				_ = tx.Rollback()
				t.Errorf("unable to save %T[%s]: %s", it, it.GetLink(), err)
			}
		}
		if err = tx.Commit(); err != nil {
			_ = tx.Rollback()
			t.Errorf("unable to commit transaction item: %+s", err)
			return r
		}
		return r
	}
}

func withActivitiesToCollections(activities vocab.ItemCollection) initFn {
	return func(t *testing.T, r *repo) *repo {
		collectionIRI := vocab.Outbox.IRI(root)
		_ = r.AddTo(collectionIRI, activities...)
		return r
	}
}

func mockRepo(t *testing.T, f fields, initFns ...initFn) *repo {
	r := &repo{
		conf:  f.Config,
		logFn: t.Logf,
		errFn: t.Errorf,
	}

	for _, fn := range initFns {
		fn(t, r)
	}
	return r
}

func TestBootstrap(t *testing.T) {
	tests := []struct {
		name    string
		arg     Config
		wantErr error
	}{
		{
			name:    "empty",
			arg:     Config{},
			wantErr: fmt.Errorf("failed to connect to `user=test-pg database=`: /tmp/.s.PGSQL.5432 (/tmp): dial error: dial unix /tmp/.s.PGSQL.5432: connect: no such file or directory"),
		},
		{
			name: "regular setup",
			arg:  setupContainer(t),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// NOTE(marius): we set PGUSER to match the error message for an empty config
			_ = os.Setenv("PGUSER", "test-pg")
			if err := Bootstrap(tt.arg); !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Fatalf("Bootstrap() error = %s", cmp.Diff(tt.wantErr, err, EquateWeakErrors))
			}
		})
	}
}

func TestClean(t *testing.T) {
	tests := []struct {
		name    string
		arg     Config
		wantErr error
	}{
		{
			name:    "empty",
			arg:     Config{},
			wantErr: fmt.Errorf("failed to connect to `user=test-pg database=`: /tmp/.s.PGSQL.5432 (/tmp): dial error: dial unix /tmp/.s.PGSQL.5432: connect: no such file or directory"),
		},
		{
			name:    "config",
			arg:     setupContainer(t),
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// NOTE(marius): we set PGUSER to match the error message for an empty config
			_ = os.Setenv("PGUSER", "test-pg")
			if err := Clean(tt.arg); !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("Clean() error = %s", cmp.Diff(tt.wantErr, err, EquateWeakErrors))
			}
		})
	}
}

func Test_repo_Reset(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
	}{
		{
			name: "bootstrapped",
			fields: fields{
				Config: setupContainer(t),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap},
		},
		{
			name: "not empty",
			fields: fields{
				Config: setupContainer(t),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withClient, withAuthorization, withAccess, withMockItems},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			if r.conn == nil {
				_ = r.Open()
				t.Cleanup(r.Close)
			}

			r.Reset()

			for _, table := range tables {
				var count sql.NullInt32
				query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE true", table)
				if err := r.conn.QueryRow(query).Scan(&count); err != nil {
					t.Fatalf("Reset() left table in invalid state: %s", err)
				}
				if !count.Valid {
					t.Fatalf("Reset() left table in invalid state: %v", count)
				}
				if count.Int32 > 0 {
					t.Errorf("Reset() left table with existing rows: %d", count.Int32)
				}
			}
		})
	}
}
