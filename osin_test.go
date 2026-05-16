package pg

import (
	"reflect"
	"testing"
	"time"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	conformance "github.com/go-ap/storage-conformance-suite"
	"github.com/google/go-cmp/cmp"
	"github.com/openshift/osin"
)

func Test_repo_Clone(t *testing.T) {
	s := new(repo)
	ss := s.Clone()
	s1, ok := ss.(*repo)
	if !ok {
		t.Errorf("Error when cloning storage, unable to convert interface back to %T: %T", s, ss)
	}
	if !reflect.DeepEqual(s, s1) {
		t.Errorf("Error when cloning storage, invalid pointer returned %p: %p", s, s1)
	}
}

func Test_repo_CreateClient(t *testing.T) {
	tests := []struct {
		name     string
		setupFns []initFn
		arg      osin.Client
		wantErr  error
	}{
		{
			name:    "empty",
			wantErr: errInvalidConnection,
		},
		{
			name:     "default client",
			setupFns: []initFn{withOpenRoot, withBootstrap},
			arg:      &osin.DefaultClient{Id: "test", Secret: "test", RedirectUri: "/"},
		},
		{
			name:     "default client with user data",
			setupFns: []initFn{withOpenRoot, withCleanup},
			arg:      &osin.DefaultClient{Id: "test", Secret: "test", RedirectUri: "/", UserData: "https://example.com"},
		},
		{
			name:     "default client with invalid user data",
			setupFns: []initFn{withOpenRoot, withCleanup},
			arg:      &osin.DefaultClient{Id: "test", Secret: "test", RedirectUri: "/", UserData: struct{ Example string }{Example: "foobar"}},
			wantErr:  errors.Errorf("Could not assert \"{foobar}\" to string"),
		},
	}

	conf := setupContainer(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mockRepo(t, fields{Config: conf}, tt.setupFns...)
			t.Cleanup(s.Close)
			err := s.SaveClient(tt.arg)
			if !cmp.Equal(tt.wantErr, err, EquateWeakErrors) {
				t.Errorf("invalid error type received %s", cmp.Diff(tt.wantErr, err, EquateWeakErrors))
			}

			if tt.arg == nil || tt.wantErr != nil {
				return
			}

			sel := "SELECT code, secret, redirect_uri, extra FROM oauth2.client WHERE code=$1;"
			res, err := s.conn.Query(sel, tt.arg.GetId())
			if err != nil {
				t.Fatalf("error when loading client from db: %s", err)
			}

			for res.Next() {
				var code string
				var secret string
				var redir string
				var extra []byte

				err = res.Scan(&code, &secret, &redir, &extra)
				if err != nil {
					t.Fatalf("error when load client data: %s", err)
				}

				if tt.arg.GetId() != code {
					t.Errorf("invalid client ID loaded %s, expected %s", code, tt.arg.GetId())
				}
				if tt.arg.GetSecret() != secret {
					t.Errorf("invalid client secret loaded %s, expected %s", secret, tt.arg.GetSecret())
				}
				if tt.arg.GetRedirectUri() != redir {
					t.Errorf("invalid client redirect uri loaded %s, expected %s", redir, tt.arg.GetRedirectUri())
				}

				if tt.arg.GetUserData() == nil {
					return
				}
				ud, err := assertToBytes(tt.arg.GetUserData())
				if err != nil {
					t.Errorf("error when load user data for the client: %s", err)
				}
				if !cmp.Equal(ud, extra) {
					t.Errorf("user data for the client is different: %s", cmp.Diff(tt.arg.GetUserData(), ud))
				}
			}
		})
	}
}

var hellTimeStr = "2666-06-06 06:06:06"
var hellTime, _ = time.Parse("2006-01-02 15:04:05", hellTimeStr)

func Test_repo_LoadAuthorize(t *testing.T) {
	conf := setupContainer(t)

	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		want     *osin.AuthorizeData
		wantErr  error
	}{
		{
			name:    "not open",
			wantErr: errInvalidConnection,
		},
		{
			name:     "empty",
			fields:   fields{Config: conf},
			setupFns: []initFn{withOpenRoot, withBootstrap},
			wantErr:  errors.Newf("Empty authorize code"),
		},
		{
			name:   "find one",
			code:   "one",
			fields: fields{Config: conf},
			setupFns: []initFn{
				withOpenRoot,
				withCleanup,
				func(t *testing.T, r *repo) *repo {
					err := execQueryInTx(r.conn, upsertClientSQL, "client", "secret", "redir", "extra123")
					if err != nil {
						r.errFn("unable to save client data: %s", err)
					}
					err = execQueryInTx(r.conn, upsertAuthorizeSQL, "client", "one", "666", "scop", "redir", "state", "0000000000000000000000000000000000000000123", "PLAIN", hellTimeStr, "extra123")
					if err != nil {
						r.errFn("unable to save authorization data: %s", err)
					}
					return r
				},
			},
			want: &osin.AuthorizeData{
				Client: &osin.DefaultClient{
					Id:          "client",
					Secret:      "secret",
					RedirectUri: "redir",
					UserData:    "extra123",
				},
				Code:                "one",
				ExpiresIn:           666,
				Scope:               "scop",
				RedirectUri:         "redir",
				State:               "state",
				CreatedAt:           hellTime,
				UserData:            vocab.IRI("extra123"),
				CodeChallengeMethod: "PLAIN",
				CodeChallenge:       "0000000000000000000000000000000000000000123",
			},
		},
		{
			name:     "authorized",
			fields:   fields{Config: conf},
			code:     "test-code",
			setupFns: []initFn{withOpenRoot, withCleanup, withClient, withAuthorization},
			want:     mockAuth("test-code", defaultClient),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mockRepo(t, tt.fields, tt.setupFns...)
			got, err := s.LoadAuthorize(tt.code)
			if !cmp.Equal(tt.wantErr, err, EquateWeakErrors) {
				t.Errorf("invalid error type received %s", cmp.Diff(tt.wantErr, err, EquateWeakErrors))
			}
			if !cmp.Equal(tt.want, got) {
				t.Errorf("Different authorize data received, from expected: %s", cmp.Diff(tt.want, got))
			}
		})
	}
}

func Test_repo_UpdateClient(t *testing.T) {
	tests := []struct {
		name     string
		setupFns []initFn
		arg      osin.Client
		wantErr  error
	}{
		{
			name:    "empty",
			wantErr: errInvalidConnection,
		},
		{
			name: "basic",
			setupFns: []initFn{
				withOpenRoot,
				withBootstrap,
				func(t *testing.T, r *repo) *repo {
					if _, err := r.conn.Exec(upsertClientSQL, "found", "secret", "redirURI", any("extra123")); err != nil {
						r.errFn("unable to create client: %s", err)
					}
					return r
				},
			},
			arg: &osin.DefaultClient{
				Id:          "found",
				Secret:      "secret",
				RedirectUri: "redirURI",
				UserData:    any("extra123"),
			},
		},
	}

	conf := setupContainer(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mockRepo(t, fields{Config: conf}, tt.setupFns...)
			t.Cleanup(s.Close)
			err := s.SaveClient(tt.arg)
			if !cmp.Equal(tt.wantErr, err, EquateWeakErrors) {
				t.Errorf("invalid error type received %s", cmp.Diff(tt.wantErr, err, EquateWeakErrors))
			}
		})
	}
}

func Test_repo_LoadAccess(t *testing.T) {
	conf := setupContainer(t)

	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		want     *osin.AccessData
		wantErr  error
	}{
		{
			name:    "not open",
			wantErr: errInvalidConnection,
		},
		{
			name:     "empty no bootstrap",
			fields:   fields{Config: conf},
			setupFns: []initFn{withOpenRoot},
			wantErr:  errors.NotFoundf("Empty access code"),
		},
		{
			name:     "empty",
			fields:   fields{Config: conf},
			setupFns: []initFn{withOpenRoot, withBootstrap},
			code:     "",
			wantErr:  errors.Newf("Empty access code"),
		},
		{
			name:     "load access",
			fields:   fields{Config: conf},
			setupFns: []initFn{withOpenRoot, withCleanup, withClient, withAuthorization, withAccess},
			code:     "access-666",
			want:     mockAccess("access-666", defaultClient),
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			got, err := r.LoadAccess(tt.code)
			if !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("LoadAccess() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("LoadAccess() got %s", cmp.Diff(tt.want, got))
			}
		})
	}
}

func Test_repo_LoadRefresh(t *testing.T) {
	conf := setupContainer(t)

	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		want     *osin.AccessData
		wantErr  error
	}{
		{
			name:    "not open",
			wantErr: errInvalidConnection,
		},
		{
			name:     "empty no bootstrap",
			fields:   fields{},
			setupFns: []initFn{withOpenRoot},
			wantErr:  errors.Newf("Empty refresh code"),
		},
		{
			name:     "empty",
			fields:   fields{Config: conf},
			setupFns: []initFn{withOpenRoot, withBootstrap},
			wantErr:  errors.Newf("Empty refresh code"),
		},
		{
			name:     "with refresh",
			fields:   fields{Config: conf},
			code:     "refresh-666",
			setupFns: []initFn{withOpenRoot, withCleanup, withClient, withAuthorization, withAccess},
			want:     mockAccess("access-666", defaultClient),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			got, err := r.LoadRefresh(tt.code)
			if !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("LoadRefresh() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("LoadRefresh() got %s", cmp.Diff(tt.want, got))
			}
		})
	}
}

func Test_repo_RemoveAccess(t *testing.T) {
	conf := setupContainer(t)

	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		wantErr  error
	}{
		{
			name:    "empty",
			wantErr: errInvalidConnection,
		},
		{
			name:    "empty",
			code:    "test",
			wantErr: errInvalidConnection,
		},
		{
			name:     "remove access",
			fields:   fields{Config: conf},
			setupFns: []initFn{withOpenRoot, withBootstrap, withClient, withAuthorization, withAccess},
			code:     "access-666",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			if err := r.RemoveAccess(tt.code); !errors.Is(err, tt.wantErr) {
				t.Errorf("RemoveAccess() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_RemoveAuthorize(t *testing.T) {
	conf := setupContainer(t)

	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errInvalidConnection,
		},
		{
			name:     "remove auth",
			fields:   fields{Config: conf},
			setupFns: []initFn{withOpenRoot, withBootstrap, withClient, withAuthorization},
			code:     "test-auth",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			if err := r.RemoveAuthorize(tt.code); !errors.Is(err, tt.wantErr) {
				t.Errorf("RemoveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_RemoveClient(t *testing.T) {
	conf := setupContainer(t)

	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errInvalidConnection,
		},
		{
			name:    "empty",
			fields:  fields{},
			code:    "test",
			wantErr: errInvalidConnection,
		},
		{
			name:     "remove client",
			fields:   fields{Config: conf},
			setupFns: []initFn{withOpenRoot, withBootstrap, withClient},
			code:     "test-client",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			if err := r.RemoveClient(tt.code); !errors.Is(err, tt.wantErr) {
				t.Errorf("RemoveClient() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_RemoveRefresh(t *testing.T) {
	conf := setupContainer(t)

	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		wantErr  error
	}{
		{
			name:    "not open",
			fields:  fields{Config: conf},
			wantErr: errInvalidConnection,
		},
		{
			name:    "empty not open",
			fields:  fields{},
			code:    "test",
			wantErr: errInvalidConnection,
		},
		{
			name:     "empty",
			fields:   fields{Config: conf},
			setupFns: []initFn{withOpenRoot, withBootstrap},
			code:     "test",
		},
		{
			name:     "mock access",
			fields:   fields{Config: conf},
			setupFns: []initFn{withOpenRoot, withCleanup, withClient, withAuthorization, withAccess},
			code:     "access-666",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			if err := r.RemoveRefresh(tt.code); !errors.Is(err, tt.wantErr) {
				t.Errorf("RemoveRefresh() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_SaveAuthorize(t *testing.T) {
	conf := setupContainer(t)

	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		auth     *osin.AuthorizeData
		wantErr  error
	}{
		{
			name:    "not open",
			wantErr: errInvalidConnection,
		},
		{
			name:     "empty",
			fields:   fields{Config: conf},
			setupFns: []initFn{withOpenRoot},
			wantErr:  errors.Newf("unable to save nil authorization data"),
		},
		{
			name:     "save mock auth",
			fields:   fields{Config: conf},
			setupFns: []initFn{withOpenRoot, withBootstrap, withClient},
			auth:     mockAuth("test-code123", defaultClient),
			wantErr:  nil,
		},
		{
			name:     "save mock auth with PKCE",
			fields:   fields{Config: conf},
			setupFns: []initFn{withOpenRoot, withCleanup, withClient},
			auth:     mockAuthWithCodeChallenge("test-code123", defaultClient),
			wantErr:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, fields{Config: conf}, tt.setupFns...)
			t.Cleanup(r.Close)

			err := r.SaveAuthorize(tt.auth)
			if tt.wantErr != nil {
				if err != nil {
					if tt.wantErr.Error() != err.Error() {
						t.Errorf("SaveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
					}
				} else {
					t.Errorf("SaveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			got, err := r.LoadAuthorize(tt.auth.Code)
			if tt.wantErr != nil {
				if err != nil {
					if tt.wantErr.Error() != err.Error() {
						t.Errorf("LoadAuthorize() after SaveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
					}
				} else {
					t.Errorf("LoadAuthorize() after SaveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			if !cmp.Equal(got, tt.auth) {
				t.Errorf("SaveAuthorize() diff %s", cmp.Diff(got, tt.auth))
			}
		})
	}
}

func Test_repo_SaveAccess(t *testing.T) {
	conf := setupContainer(t)

	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		data     *osin.AccessData
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errInvalidConnection,
		},
		{
			name:     "save access",
			fields:   fields{Config: conf},
			setupFns: []initFn{withOpenRoot, withBootstrap, withClient, withAuthorization},
			data:     mockAccess("access-666", defaultClient),
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			if err := r.SaveAccess(tt.data); !errors.Is(err, tt.wantErr) {
				t.Errorf("SaveAccess() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

var _ conformance.OSINStorage = new(repo)
var _ conformance.ClientSaver = new(repo)
