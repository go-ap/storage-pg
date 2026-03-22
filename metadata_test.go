package pg

import conformance "github.com/go-ap/storage-conformance-suite"

var _ conformance.MetadataStorage = new(repo)
var _ conformance.PasswordStorage = new(repo)
