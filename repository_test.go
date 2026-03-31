package pg

import (
	"fmt"
	"testing"

	vocab "github.com/go-ap/activitypub"
	conformance "github.com/go-ap/storage-conformance-suite"
	"github.com/google/go-cmp/cmp"
)

var _ conformance.ActivityPubStorage = new(repo)

func Test_repo_Save(t *testing.T) {
	type test struct {
		name     string
		fields   fields
		setupFns []initFn
		it       vocab.Item
		want     vocab.Item
		wantErr  error
	}

	conf := setupContainer(t)
	tests := []test{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errInvalidConnection,
		},
		{
			name:     "empty item can't be saved",
			fields:   fields{Config: conf},
			setupFns: []initFn{withOpenRoot, withBootstrap},
			wantErr:  errNilItem,
		},
		{
			name:     "save item collection",
			setupFns: []initFn{withOpenRoot, withCleanup},
			fields:   fields{Config: conf},
			it:       mockItems,
			want:     mockItems,
		},
	}
	for i, mockIt := range mockItems {
		tests = append(tests, test{
			name:     fmt.Sprintf("save %d %T to repo", i, mockIt),
			setupFns: []initFn{withOpenRoot, withCleanup},
			fields:   fields{Config: conf},
			it:       mockIt,
			want:     mockIt,
		})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			got, err := r.Save(tt.it)
			if !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("Save() error = %s", cmp.Diff(tt.wantErr, err, EquateWeakErrors))
				return
			}
			if !cmp.Equal(got, tt.want, conformance.EquateItems) {
				t.Errorf("Save() got = %s", cmp.Diff(tt.want, got, conformance.EquateItems))
			}
		})
	}
}

func Test_repo_AddTo(t *testing.T) {
	conf := setupContainer(t)
	type args struct {
		col   vocab.IRI
		items []vocab.Item
	}
	tests := []struct {
		name     string
		setupFns []initFn
		fields   fields
		args     args
		wantErr  error
	}{
		{
			name:    "empty",
			args:    args{},
			wantErr: errInvalidConnection,
		},
		{
			name: "with config",
			fields: fields{
				Config: conf,
			},
			wantErr: errInvalidConnection,
		},
		{
			name:     "empty collection IRI",
			setupFns: []initFn{withOpenRoot, withBootstrap},
			fields:   fields{Config: conf},
			args:     args{},
			wantErr:  errInvalidCollection,
		},
		{
			name:     "nil items to add",
			setupFns: []initFn{withOpenRoot, withCleanup},
			fields:   fields{Config: conf},
			args: args{
				col:   "https://example.com/invalid-collection",
				items: nil,
			},
			wantErr: nil,
		},
		{
			name:     "nil item to add",
			setupFns: []initFn{withOpenRoot, withCleanup},
			fields:   fields{Config: conf},
			args: args{
				col:   "https://example.com/invalid-collection",
				items: vocab.ItemCollection{vocab.Item(nil)},
			},
			wantErr: errNilItem,
		},
		{
			name:     "IRI to add to invalid collection",
			setupFns: []initFn{withOpenRoot, withCleanup},
			fields:   fields{Config: conf},
			args: args{
				col:   "https://example.com/invalid-collection",
				items: vocab.ItemCollection{vocab.IRI("https://example.com")},
			},
			wantErr: errNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)
			if err := r.AddTo(tt.args.col, tt.args.items...); !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("AddTo() error = %s", cmp.Diff(tt.wantErr, err, EquateWeakErrors))
			}
		})
	}
}
