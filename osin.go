package pg

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/go-ap/errors"
	"github.com/openshift/osin"
)

func (r *repo) Clone() osin.Storage {
	r.conn = nil
	return r
}

func (r *repo) close() error {
	if r.conn == nil {
		return nil
	}
	if err := r.conn.Close(); err != nil {
		return err
	}
	r.conn = nil
	return nil
}

func (r *repo) Close() {
	if err := r.close(); err != nil {
		r.errFn("unable to close connection %s", err)
	}
}

const upsertClientSQL = `INSERT INTO client (code, secret, redirect_uri, extra) VALUES ($1, $2, $3, $4) 
ON CONFLICT ON CONSTRAINT client_code_pkey DO UPDATE SET (secret, redirect_uri, extra) 
= (excluded.secret, excluded.redirect_uri, excluded.extra);`

var emptyJSONObject = []byte{'{', '}'}

func (r *repo) CreateClient(c osin.Client) error {
	data, err := assertToBytes(c.GetUserData())
	if err != nil {
		r.logFn("client id %s: %s", c.GetId(), err)
	}
	if data == nil {
		data = emptyJSONObject
	}

	params := []any{
		c.GetId(),
		c.GetSecret(),
		c.GetRedirectUri(),
		data,
	}

	return execQueryInTx(r.conn, upsertClientSQL, params...)
}

func (r *repo) GetClient(id string) (osin.Client, error) {
	return nil, errors.NotImplementedf("implement me")
}

const upsertAuthorizeSQL = `INSERT INTO authorize 
	(client, code, expires_in, scope, redirect_uri, state, code_challenge, code_challenge_method, extra) VALUES 
	($1, $2, $3, $4, $5, $6, $7, $8, $9) 
ON CONFLICT ON CONSTRAINT authorize_code_pkey DO UPDATE SET 
(client, expires_in, scope, redirect_uri, state, code_challenge, code_challenge_method, extra) = 
(excluded.client, excluded.expires_in, excluded.scope, excluded.redirect_uri, excluded.state, excluded.code_challenge, excluded.code_challenge_method, excluded.extra);`

func (r *repo) SaveAuthorize(data *osin.AuthorizeData) error {
	userData, err := assertToBytes(data.UserData)
	if err != nil {
		r.logFn("auhtorize id %s: %s", data.Code, err)
	}
	if userData == nil {
		userData = emptyJSONObject
	}

	params := []any{
		data.Client.GetId(),
		data.Code,
		data.ExpiresIn,
		data.Scope,
		data.RedirectUri,
		data.State,
		data.CodeChallenge,
		data.CodeChallengeMethod,
		userData,
	}

	return execQueryInTx(r.conn, upsertAuthorizeSQL, params...)
}

func (r *repo) LoadAuthorize(code string) (*osin.AuthorizeData, error) {
	return nil, errors.NotImplementedf("implement me")
}

func (r *repo) RemoveAuthorize(code string) error {
	return errors.NotImplementedf("implement me")
}

func execQueryInTx(conn *sql.DB, q string, params ...any) error {
	tx, err := conn.Begin()
	if err != nil {
		return errors.Annotatef(err, "transaction start error")
	}
	if _, err = tx.Exec(q, params...); err != nil {
		return errors.Annotatef(err, "query execution error")
	}
	if err = tx.Commit(); err != nil {
		err = errors.Annotatef(err, "failed to commit transaction")
	}
	return nil
}

const upsertAccessSQL = `INSERT INTO access 
	(token, client, authorize, previous, refresh_token, expires_in, scope, redirect_uri, created_at, extra) VALUES 
	($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) 
ON CONFLICT ON CONSTRAINT access_token_pkey DO UPDATE SET 
(client, authorize, previous, refresh_token, expires_in, scope, redirect_uri, created_at, extra) = 
(excluded.client,excluded.authorize, excluded.previous, excluded.refresh_token, excluded.expires_in, excluded.scope, excluded.redirect_uri, excluded.created_at, excluded.extra);`

func (r *repo) SaveAccess(data *osin.AccessData) error {
	userData, err := assertToBytes(data.UserData)
	if err != nil {
		r.logFn("auhtorize id %s: %s", data.AccessToken, err)
	}
	if userData == nil {
		userData = emptyJSONObject
	}

	var clientID string
	if data.Client != nil {
		clientID = data.Client.GetId()
	}
	var accessToken string
	if data.AccessData != nil {
		accessToken = data.AccessData.AccessToken
	}
	var authorizeCode string
	if data.AuthorizeData != nil {
		authorizeCode = data.AuthorizeData.Code
	}
	params := []any{
		data.AccessToken,
		clientID,
		authorizeCode,
		accessToken,
		data.RefreshToken,
		data.ExpiresIn,
		data.Scope,
		data.RedirectUri,
		data.CreatedAt,
		userData,
	}

	return execQueryInTx(r.conn, upsertAccessSQL, params...)
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

func assertToBytes(in any) ([]byte, error) {
	if in == nil {
		return nil, nil
	}
	switch data := in.(type) {
	case string:
		return []byte(data), nil
	case []byte:
		return data, nil
	case json.RawMessage:
		return data, nil
	case fmt.Stringer:
		return []byte(data.String()), nil
	case fmt.GoStringer:
		return []byte(data.GoString()), nil
	}
	return nil, errors.Errorf(`Could not assert "%v" to string`, in)
}
