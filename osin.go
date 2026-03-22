package pg

import (
	"github.com/go-ap/errors"
	"github.com/openshift/osin"
)

func (r *repo) Clone() osin.Storage {
	return r
}

func (r *repo) Close() {
	//TODO implement me
	panic("implement me")
}

func (r *repo) GetClient(id string) (osin.Client, error) {
	return nil, errors.NotImplementedf("implement me")
}

func (r *repo) SaveAuthorize(data *osin.AuthorizeData) error {
	return errors.NotImplementedf("implement me")
}

func (r *repo) LoadAuthorize(code string) (*osin.AuthorizeData, error) {
	return nil, errors.NotImplementedf("implement me")
}

func (r *repo) RemoveAuthorize(code string) error {
	return errors.NotImplementedf("implement me")
}

func (r *repo) SaveAccess(data *osin.AccessData) error {
	return errors.NotImplementedf("implement me")
}

func (r *repo) LoadAccess(token string) (*osin.AccessData, error) {
	return nil, errors.NotImplementedf("implement me")
}

func (r *repo) RemoveAccess(token string) error {
	return errors.NotImplementedf("implement me")
}

func (r *repo) LoadRefresh(token string) (*osin.AccessData, error) {
	return nil, errors.NotImplementedf("implement me")
}

func (r *repo) RemoveRefresh(token string) error {
	return errors.NotImplementedf("implement me")
}
