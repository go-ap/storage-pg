package pg

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	vocab "github.com/go-ap/activitypub"
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

var nilClientErr = errors.Newf("nil client")

func (r *repo) CreateClient(c osin.Client) error {
	if r == nil || r.conn == nil {
		return errInvalidConnection
	}

	if c == nil {
		return nilClientErr
	}

	data, err := assertToBytes(c.GetUserData())
	if err != nil {
		return err
	}

	params := []any{
		c.GetId(),
		c.GetSecret(),
		c.GetRedirectUri(),
		data,
	}

	return execQueryInTx(r.conn, upsertClientSQL, params...)
}

func (r *repo) UpdateClient(c osin.Client) error {
	return r.CreateClient(c)
}

const deleteClientSQL = `DELETE FROM client where code = $1;`

func (r *repo) RemoveClient(id string) error {
	if r == nil || r.conn == nil {
		return errInvalidConnection
	}
	return execQueryInTx(r.conn, deleteClientSQL, id)
}

func (r *repo) GetClient(code string) (osin.Client, error) {
	if r == nil || r.conn == nil {
		return nil, errInvalidConnection
	}

	if code == "" {
		return nil, errors.NotFoundf("Empty client code")
	}

	return getClient(r.conn, code)
}

const listClientsSQL = "SELECT code, secret, redirect_uri, extra FROM client;"

func (r *repo) ListClients() ([]osin.Client, error) {
	if r == nil || r.conn == nil {
		return nil, errInvalidCollection
	}

	result := make([]osin.Client, 0)

	rows, err := r.conn.Query(listClientsSQL)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFound(err, "No clients found")
		}
		r.errFn("Error listing clients: %+s", err)
		return result, errors.Annotatef(err, "Unable to load clients")
	}
	defer rows.Close()

	for rows.Next() {
		c := new(cl)
		err = rows.Scan(&c.Id, &c.Secret, &c.RedirectUri, &c.UserData)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}

	if len(result) == 0 {
		return nil, nil
	}

	return result, nil
}

const upsertAuthorizeSQL = `INSERT INTO authorize 
	(client, code, expires_in, scope, redirect_uri, state, code_challenge, code_challenge_method, created_at, extra) VALUES 
	($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) 
ON CONFLICT ON CONSTRAINT authorize_code_pkey DO UPDATE SET 
(client, expires_in, scope, redirect_uri, state, code_challenge, code_challenge_method, created_at, extra) = 
(excluded.client, excluded.expires_in, excluded.scope, excluded.redirect_uri, excluded.state, excluded.code_challenge, excluded.code_challenge_method, excluded.created_at, excluded.extra);`

func (r *repo) SaveAuthorize(data *osin.AuthorizeData) error {
	if r == nil || r.conn == nil {
		return errInvalidConnection
	}
	if data == nil {
		return errors.Newf("unable to save nil authorization data")
	}
	userData, err := assertToBytes(data.UserData)
	if err != nil {
		r.logFn("authorize id %s: %s", data.Code, err)
	}
	if userData == nil {
		userData = emptyJSONObject
	}
	createdAt := data.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
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
		createdAt.Truncate(time.Second).UTC(),
		userData,
	}

	return execQueryInTx(r.conn, upsertAuthorizeSQL, params...)
}

func (r *repo) LoadAuthorize(code string) (*osin.AuthorizeData, error) {
	if r == nil || r.conn == nil {
		return nil, errInvalidConnection
	}
	if code == "" {
		return nil, errors.Newf("Empty authorize code")
	}

	return loadAuthorize(r.conn, code)
}

const loadAuthorizeSQL = `SELECT
    a.code a_code, expires_in, scope, a.redirect_uri a_redirect_uri, state, created_at, a.extra a_extra,
    a.code_challenge a_code_challenge, a.code_challenge_method a_code_challenge_method,
    c.code c_code, c.redirect_uri c_redirect_uri, c.secret, c.extra c_extra
FROM authorize a
INNER JOIN client c ON a.client = c.code
WHERE a.code = $1 LIMIT 1;`

func loadAuthorize(conn *sql.DB, code string) (*osin.AuthorizeData, error) {
	var a *osin.AuthorizeData

	rows, err := conn.Query(loadAuthorizeSQL, code)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NotFoundf("Unable to load authorize token")
		}
		return nil, errors.Annotatef(err, "Unable to load authorize token")
	}
	defer rows.Close()

	for rows.Next() {
		a = new(osin.AuthorizeData)
		c := new(osin.DefaultClient)
		var aUserData sql.NullString
		var aCodeChallenge sql.NullString
		var aCodeChallengeMethod sql.NullString
		var cUserData sql.NullString
		var createdAt string

		err = rows.Scan(&a.Code, &a.ExpiresIn, &a.Scope, &a.RedirectUri, &a.State, &createdAt, &aUserData,
			&aCodeChallenge, &aCodeChallengeMethod,
			&c.Id, &c.RedirectUri, &c.Secret, &cUserData)
		if err != nil {
			return nil, errors.Annotatef(err, "unable to load authorize data")
		}

		a.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
		if !a.CreatedAt.IsZero() && a.ExpireAt().Before(time.Now().UTC()) {
			return nil, errors.Errorf("Token expired at %s.", a.ExpireAt().String())
		}
		if aUserData.Valid {
			a.UserData = vocab.IRI(aUserData.String)
		}
		if cUserData.Valid {
			c.UserData = cUserData.String
		}
		if aCodeChallenge.Valid {
			a.CodeChallenge = aCodeChallenge.String
		}
		if aCodeChallengeMethod.Valid {
			a.CodeChallengeMethod = aCodeChallengeMethod.String
		}
		if len(c.Id) > 0 {
			a.Client = c
		}
		break
	}
	if a == nil {
		return nil, errors.NotFoundf("unable to load authorize data")
	}

	return a, nil
}

const deleteAuthorizeSQL = `DELETE FROM authorize where code = $1;`

func (r *repo) RemoveAuthorize(code string) error {
	if r == nil || r.conn == nil {
		return errInvalidConnection
	}
	return execQueryInTx(r.conn, deleteAuthorizeSQL, code)
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
	if r == nil || r.conn == nil {
		return errInvalidConnection
	}
	userData, err := assertToBytes(data.UserData)
	if err != nil {
		r.logFn("authorize id %s: %s", data.AccessToken, err)
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
	createdAt := data.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
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
		createdAt.Truncate(time.Second).UTC(),
		userData,
	}

	err = execQueryInTx(r.conn, upsertAccessSQL, params...)
	if err != nil {
		return errors.Annotatef(err, "Unable to create access token")
	}
	if len(data.RefreshToken) > 0 {
		if err = execQueryInTx(r.conn, saveRefresh, data.RefreshToken, data.AccessToken); err != nil {
			return err
		}
	}
	return nil
}

const saveRefresh = "INSERT INTO refresh (token, access_token) VALUES ($1, $2)"

func (r *repo) LoadAccess(code string) (*osin.AccessData, error) {
	if r == nil || r.conn == nil {
		return nil, errInvalidConnection
	}
	if code == "" {
		return nil, errors.Newf("Empty access code")
	}

	return loadAccess(r.conn, code, true)
}

const loadAccessSQL = `SELECT 
	acc.token acc_token, acc.refresh_token acc_refresh_token, acc.expires_in acc_expires_in, acc.scope acc_scope, acc.redirect_uri acc_redirect_uri, acc.created_at acc_created_at, acc.extra acc_extra, acc.previous as acc_previous,
	auth.code auth_code, auth.expires_in auth_expires_in,  auth.scope auth_scope, auth.redirect_uri auth_redirect_uri, auth.state auth_state, auth.created_at auth_created_at, auth.extra auth_extra,
	auth.code_challenge auth_code_challenge, auth.code_challenge_method auth_code_challenge_method,
	c.code c_code, c.redirect_uri c_redirect_uri, c.secret, c.extra c_extra
	FROM access acc
	INNER JOIN client c ON acc.client = c.code
	LEFT JOIN authorize auth ON acc.authorize = auth.code
WHERE acc.token = $1 LIMIT 1`

func loadAccess(conn *sql.DB, code string, loadDeps bool) (*osin.AccessData, error) {
	rows, err := conn.Query(loadAccessSQL, code)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFound(err, "Unable to load access token")
		}
		return nil, errors.Annotatef(err, "Unable to load access token")
	}
	defer rows.Close()

	for rows.Next() {
		acc := new(osin.AccessData)
		c := new(osin.DefaultClient)
		auth := new(osin.AuthorizeData)
		var accUserData, cUserData, accPrevious sql.NullString
		var accCreatedAt string
		var authCode, authScope, authUserData, authRedirectUri, authState, authCreatedAt, authCodeChallenge, authCodeChallengeMethod sql.NullString
		var authExpiresIn sql.NullInt32

		err = rows.Scan(&acc.AccessToken, &acc.RefreshToken, &acc.ExpiresIn, &acc.Scope, &acc.RedirectUri, &accCreatedAt, &accUserData, &accPrevious,
			&authCode, &authExpiresIn, &authScope, &authRedirectUri, &authState, &authCreatedAt, &authUserData, &authCodeChallenge, &authCodeChallengeMethod,
			&c.Id, &c.RedirectUri, &c.Secret, &cUserData,
		)

		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, errors.NewNotFound(err, "Unable to load access data")
			}
			return nil, errors.Annotatef(err, "unable to load access data")
		}

		acc.CreatedAt, _ = time.Parse(time.RFC3339Nano, accCreatedAt)
		if !acc.CreatedAt.IsZero() && acc.ExpireAt().Before(time.Now().UTC()) {
			return nil, errors.Errorf("Token expired at %s.", acc.ExpireAt().String())
		}
		if authCreatedAt.Valid {
			auth.CreatedAt, _ = time.Parse(time.RFC3339Nano, authCreatedAt.String)
		}
		if authRedirectUri.Valid {
			auth.RedirectUri = authRedirectUri.String
		}

		if accUserData.Valid {
			acc.UserData = vocab.IRI(accUserData.String)
		}
		if authUserData.Valid {
			auth.UserData = vocab.IRI(authUserData.String)
		}
		if authCode.Valid {
			auth.Code = authCode.String
		}
		if authScope.Valid {
			auth.Scope = authScope.String
		}
		if authState.Valid {
			auth.State = authState.String
		}
		if authExpiresIn.Valid {
			auth.ExpiresIn = authExpiresIn.Int32
		}
		if authCodeChallengeMethod.Valid {
			auth.CodeChallengeMethod = authCodeChallengeMethod.String
		}
		if authCodeChallenge.Valid {
			auth.CodeChallenge = authCodeChallenge.String
		}
		if cUserData.Valid {
			c.UserData = cUserData.String
		}
		if loadDeps {
			if accPrevious.Valid {
				prev, _ := loadAccess(conn, accPrevious.String, false)
				if prev != nil {
					acc.AccessData = prev
				}
			}
			if len(auth.Code) > 0 {
				acc.AuthorizeData = auth
			}
		}
		if len(c.Id) > 0 {
			acc.Client = c
			if acc.AuthorizeData != nil {
				acc.AuthorizeData.Client = c
			}
		}
		return acc, nil
	}
	return nil, errors.NotFoundf("unable to load access data")
}

const deleteAccessSQL = `DELETE FROM access where token = $1;`

func (r *repo) RemoveAccess(token string) error {
	if r == nil || r.conn == nil {
		return errInvalidConnection
	}
	return execQueryInTx(r.conn, deleteAccessSQL, token)
}

const loadRefresh = "SELECT access_token FROM refresh WHERE token=$1 LIMIT 1"

func (r *repo) LoadRefresh(token string) (*osin.AccessData, error) {
	if r == nil || r.conn == nil {
		return nil, errInvalidConnection
	}
	if token == "" {
		return nil, errors.Newf("Empty refresh code")
	}

	var access sql.NullString
	err := r.conn.QueryRow(loadRefresh, token).Scan(&access)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFound(err, "Unable to load refresh token")
		}
		return nil, errors.Annotatef(err, "Unable to load refresh token")
	}

	return loadAccess(r.conn, access.String, true)
}

const deleteRefreshSQL = `DELETE FROM refresh where token = $1;`

func (r *repo) RemoveRefresh(token string) error {
	if r == nil || r.conn == nil {
		return errInvalidConnection
	}
	return execQueryInTx(r.conn, deleteRefreshSQL, token)
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

type cl struct {
	Id          string
	Secret      string
	RedirectUri string
	UserData    string
}

func (c cl) GetId() string {
	return c.Id
}

func (c cl) GetSecret() string {
	return c.Secret
}

func (c cl) GetRedirectUri() string {
	return c.RedirectUri
}

func (c cl) GetUserData() any {
	return c.UserData
}

var _ osin.Client = cl{}

const getClientSQL = "SELECT code, secret, redirect_uri, extra FROM client WHERE code = $1"

func errClientNotFound(err error) error {
	if err == nil {
		return errors.NotFoundf("Client could not be found")
	}
	return errors.NewNotFound(err, "Client could not be found")
}

func getClient(conn *sql.DB, code string) (osin.Client, error) {
	row := conn.QueryRow(getClientSQL, code)
	if err := row.Err(); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errClientNotFound(err)
		}
		return nil, errors.Annotatef(err, "Unable to load client")
	}

	c := new(cl)
	var userData sql.NullString
	if err := row.Scan(&c.Id, &c.Secret, &c.RedirectUri, &userData); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errClientNotFound(err)
		}
		return nil, errors.Annotatef(err, "Unable to load client information")
	}

	if userData.Valid {
		c.UserData = userData.String
	}
	return c, nil
}
