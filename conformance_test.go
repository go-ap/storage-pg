//go:build conformance

package pg

import (
	"testing"

	conformance "github.com/go-ap/storage-conformance-suite"
)

func initStorage(t *testing.T) conformance.ActivityPubStorage {
	conf := setupContainer(t)

	if err := Bootstrap(conf); err != nil {
		t.Fatalf("unable to bootstrap storage: %s", err)
	}

	storage, err := New(conf)
	if err != nil {
		t.Fatalf("unable to initialize storage: %s", err)
	}

	return storage
}

func Test_Conformance(t *testing.T) {
	conformance.Suite(conformance.TestActivityPub, conformance.TestMetadata,
		conformance.TestOAuth, conformance.TestPassword, conformance.TestKey).Run(t, initStorage(t))
}
