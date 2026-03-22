package pg

import (
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
)

func (r *repo) LoadMetadata(iri vocab.IRI, m any) error {
	return errors.NotImplementedf("implement me")
}

func (r *repo) SaveMetadata(iri vocab.IRI, m any) error {
	return errors.NotImplementedf("implement me")
}

func (r *repo) PasswordSet(it vocab.IRI, pw []byte) error {
	return errors.NotImplementedf("implement me")
}

func (r *repo) PasswordCheck(it vocab.IRI, pw []byte) error {
	return errors.NotImplementedf("implement me")
}
