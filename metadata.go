package pg

import (
	"bytes"
	"crypto"
	"crypto/dsa"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"fmt"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"golang.org/x/crypto/bcrypt"
)

type Metadata struct {
	Pw         []byte
	PrivateKey []byte
}

var encodeFn = func(v any) ([]byte, error) {
	buf := bytes.Buffer{}
	err := json.NewEncoder(&buf).Encode(v)
	return buf.Bytes(), err
}

var decodeFn = func(data []byte, m any) error {
	return json.NewDecoder(bytes.NewReader(data)).Decode(m)
}

func (r *repo) LoadMetadata(iri vocab.IRI, m any) error {
	if r == nil || r.conn == nil {
		return errInvalidConnection
	}
	raw, err := loadMetadataFromTable(r.conn, iri)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.NewNotFound(err, "not found")
		}
		return err
	}
	if len(raw) == 0 {
		return nil
	}

	if err = decodeFn(raw, m); err != nil {
		return errors.Annotatef(err, "Could not unmarshal metadata")
	}
	return nil
}

func (r *repo) SaveMetadata(iri vocab.IRI, m any) error {
	if r == nil || r.conn == nil {
		return errInvalidConnection
	}
	if m == nil {
		return errors.Newf("Could not save nil metadata")
	}
	entryBytes, err := encodeFn(m)
	if err != nil {
		return errors.Annotatef(err, "Could not marshal metadata")
	}
	return saveMetadataToTable(r.conn, iri, entryBytes)
}

func (r *repo) PasswordSet(iri vocab.IRI, pw []byte) error {
	if r == nil || r.conn == nil {
		return errInvalidConnection
	}
	if pw == nil {
		return errors.Newf("could not generate hash for nil pw")
	}
	if len(iri) == 0 {
		return errors.NotFoundf("not found")
	}

	m := new(Metadata)
	if err := r.LoadMetadata(iri, m); err != nil && !errors.IsNotFound(err) {
		return err
	}

	var err error
	m.Pw, err = bcrypt.GenerateFromPassword(pw, -1)
	if err != nil {
		return errors.Annotatef(err, "Could not generate password hash")
	}
	return r.SaveMetadata(iri, m)
}

func (r *repo) PasswordCheck(iri vocab.IRI, pw []byte) error {
	if r == nil || r.conn == nil {
		return errInvalidConnection
	}
	m := new(Metadata)
	if err := r.LoadMetadata(iri, m); err != nil {
		return errors.Annotatef(err, "Could not find load metadata for %s", iri)
	}
	if err := bcrypt.CompareHashAndPassword(m.Pw, pw); err != nil {
		return errors.NewUnauthorized(err, "Invalid pw")
	}
	return nil
}

func (r *repo) LoadKey(iri vocab.IRI) (crypto.PrivateKey, error) {
	if r == nil || r.conn == nil {
		return nil, errInvalidConnection
	}
	m := new(Metadata)
	if err := r.LoadMetadata(iri, m); err != nil {
		return nil, err
	}
	b, _ := pem.Decode(m.PrivateKey)
	if b == nil {
		return nil, errors.Errorf("failed decoding pem")
	}
	prvKey, err := x509.ParsePKCS8PrivateKey(b.Bytes)
	if err != nil {
		return nil, err
	}
	return prvKey, nil
}

func (r *repo) SaveKey(iri vocab.IRI, key crypto.PrivateKey) (*vocab.PublicKey, error) {
	if r == nil || r.conn == nil {
		return nil, errInvalidConnection
	}
	m := new(Metadata)
	if err := r.LoadMetadata(iri, m); err != nil && !errors.IsNotFound(err) {
		return nil, err
	}
	if m.PrivateKey != nil {
		r.logFn("actor %s already has a private key", iri)
	}
	prvEnc, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}

	m.PrivateKey = pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: prvEnc,
	})
	if err = r.SaveMetadata(iri, m); err != nil {
		return nil, err
	}

	var pub crypto.PublicKey
	switch prv := key.(type) {
	case *ecdsa.PrivateKey:
		pub = prv.Public()
	case *rsa.PrivateKey:
		pub = prv.Public()
	case *dsa.PrivateKey:
		pub = &prv.PublicKey
	case ed25519.PrivateKey:
		pub = prv.Public()
	default:
		r.errFn("received key %T does not match any of the known private key types", key)
		return nil, nil
	}
	pubEnc, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		r.errFn("unable to x509.MarshalPKIXPublicKey() the private key %T for %s", pub, iri)
		return nil, err
	}
	pubEncoded := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubEnc,
	})

	return &vocab.PublicKey{
		ID:           vocab.IRI(fmt.Sprintf("%s#main", iri)),
		Owner:        iri,
		PublicKeyPem: string(pubEncoded),
	}, nil
}

func loadMetadataFromTable(conn *sql.DB, iri vocab.IRI) ([]byte, error) {
	var meta []byte
	if err := conn.QueryRow("SELECT raw FROM meta WHERE iri = $1;", iri).Scan(&meta); err != nil {
		return nil, errors.Annotatef(err, "query execution error")
	}
	return meta, nil
}

func saveMetadataToTable(conn *sql.DB, iri vocab.IRI, m []byte) error {
	query := "INSERT INTO meta (iri, raw) VALUES($1, $2) ON CONFLICT ON CONSTRAINT meta_key DO UPDATE SET raw = excluded.raw;;"
	_, err := conn.Exec(query, iri, string(m))
	return err
}
