package pg

import (
	conformance "github.com/go-ap/storage-conformance-suite"
)

var _ conformance.ActivityPubStorage = new(repo)
