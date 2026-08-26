package server

import (
	"strings"
	"testing"
	"time"

	naming "github.com/kcmvp/redisx/internal/naming"
	"github.com/kcmvp/redisx/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestClassifyArg1(t *testing.T) {
	tests := []struct {
		name string
		arg1 string
		want argShape
	}{
		{name: "empty -> doc", arg1: "", want: argShapeDoc},
		{name: "no colon ns -> doc", arg1: "user", want: argShapeDoc},
		{name: "no colon multiword -> doc", arg1: "order_item", want: argShapeDoc},
		{name: "with colon kv -> kv", arg1: "user:abc", want: argShapeKV},
		{name: "mem prefix with colon -> kv", arg1: "_m_:user:abc", want: argShapeKV},
		{name: "trailing colon alone (ns w/ suffix empty) -> kv", arg1: "user:", want: argShapeKV},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, classifyArg1(tt.arg1))
		})
	}
}

func TestValidateKVFullKey(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantErr bool
		phrase  string
	}{
		{name: "no colon bare key", key: "userkey", wantErr: true, phrase: "namespace separator"},
		{name: "internal doc underscore prefix READ ALLOWED (passthrough for GET/KEYS)", key: "_doc_:abc", wantErr: false},
		{name: "internal idx underscore prefix READ ALLOWED", key: "_idx_:user_age:v_123", wantErr: false},
		{name: "internal auth underscore prefix READ ALLOWED", key: "_auth_:somekey", wantErr: false},
		{name: "unrecognised underscore shape (future expansion) REJECTED even w/ colon", key: "_future_:abc", wantErr: true, phrase: "reserved internal prefix"},
		{name: "plain kv with colon", key: "app:config:item", wantErr: false},
		{name: "mem layer with colon", key: "_m_:user:abc", wantErr: false},
		{name: "simple ns:pk", key: "user:123", wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateKVFullKey(tt.key)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.phrase)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateKVMutationKey(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantErr bool
		phrase  string
	}{
		{name: "no colon bare key", key: "userkey", wantErr: true, phrase: "namespace separator"},
		{name: "_doc_: WRITE REJECTED (dedicated registry cmd)", key: "_doc_:abc", wantErr: true, phrase: "managed exclusively via dedicated"},
		{name: "_idx_: WRITE REJECTED", key: "_idx_:user_age:v_123", wantErr: true, phrase: "dedicated registry commands"},
		{name: "_auth_: WRITE REJECTED", key: "_auth_:leak", wantErr: true, phrase: "REGSCH/DROPSCH/REGIDX/DROPIDX/AUTH"},
		{name: "unrecognised underscore WRITE REJECTED", key: "_future_:abc", wantErr: true, phrase: "reserved internal prefix"},
		{name: "normal user:pk WRITE OK", key: "user:123", wantErr: false},
		{name: "mem layer WRITE OK (not internal)", key: "_m_:user:abc", wantErr: false},
		{name: "app:prefix WRITE OK", key: "app:config:item", wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateKVMutationKey(tt.key)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.phrase)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidatePKSuffixNoColon(t *testing.T) {
	require.NoError(t, validatePKSuffixNoColon(""))
	require.NoError(t, validatePKSuffixNoColon("123"))
	require.NoError(t, validatePKSuffixNoColon(strings.ReplaceAll("a:b:c", ":", "_")))
	err := validatePKSuffixNoColon("has:colon")
	require.Error(t, err)
	require.Contains(t, err.Error(), "namespace separator")
}

func TestStorageNsFromKRAnchor(t *testing.T) {
	tests := []struct {
		name      string
		anchor    string
		wantNs    string
		wantEmpty bool
		wantErr   bool
		phrase    string
	}{
		{name: "empty anchor err", anchor: "", wantErr: true, phrase: "empty"},
		{name: "leading wildcard star err", anchor: "*:abc", wantErr: true, phrase: "cannot start with wildcard"},
		{name: "leading glob q err", anchor: "?user:abc", wantErr: true, phrase: "cannot start with wildcard"},
		{name: "valid ns:suffix", anchor: "user:abc", wantNs: "user"},
		{name: "internal idx ns ignored empty", anchor: "_idx_:x", wantEmpty: true},
		{name: "internal doc ns ignored empty", anchor: "_doc_:user", wantEmpty: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns, err := storageNsFromKRAnchor(tt.anchor)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.phrase)
				return
			}
			require.NoError(t, err)
			if tt.wantEmpty {
				require.Empty(t, ns)
			} else {
				require.Equal(t, tt.wantNs, ns)
			}
		})
	}
}

func TestDeriveDocKey(t *testing.T) {
	spec := docSpec{Namespace: "user", Mem: false, KeyAttrs: []string{"id", "org"}}
	storageNs := spec.StorageNs()

	dk, err := deriveDocKey(spec, storageNs, `{"id":"1","org":"acme","age":30}`)
	require.NoError(t, err)
	require.Equal(t, naming.BuildStorageKey(storageNs, naming.JoinPKAttrValues([]string{"1", "acme"})), dk.FullStorageKey)
	require.Equal(t, naming.JoinPKAttrValues([]string{"1", "acme"}), dk.PKSuffix)
	require.Equal(t, `{"id":"1","org":"acme","age":30}`, dk.DocJSON)

	_, err = deriveDocKey(spec, storageNs, `{"id":"1"}`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing key attr: org")

	_, err = deriveDocKey(spec, storageNs, `not-a-json`)
	require.Error(t, err)

	_, err = deriveDocKey(spec, storageNs, `[]`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "document must be a json object")

	pkBad := docSpec{Namespace: "user", Mem: false, KeyAttrs: []string{"id"}}
	_, err = deriveDocKey(pkBad, pkBad.StorageNs(), `{"id":"1:2"}`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must not contain")

	zeroAttr := docSpec{Namespace: "cfg", Mem: false, KeyAttrs: nil}
	dk, err = deriveDocKey(zeroAttr, zeroAttr.StorageNs(), `{"foo":1}`)
	require.NoError(t, err)
	require.Equal(t, naming.BuildStorageKey(zeroAttr.StorageNs(), ""), dk.FullStorageKey)
}

func TestLookupDocByLogicalOrStorageNs(t *testing.T) {
	db := openDB(testutil.DBPath(t))
	require.NotNil(t, db)
	defer func() { _ = db.Close() }()
	require.NoError(t, db.writeDocSpec(docSpec{Namespace: "user", Mem: false, KeyAttrs: []string{"id"}, TTL: time.Hour}))
	require.NoError(t, db.writeDocSpec(docSpec{Namespace: "session", Mem: true, KeyAttrs: []string{"sid"}, TTL: 0}))

	disk := naming.BuildStorageNs("user", false)
	mem := naming.BuildStorageNs("session", true)

	ld, ok := db.lookupDocByLogicalOrStorageNs("user")
	require.True(t, ok)
	require.Equal(t, disk, ld.StorageNs)
	require.Equal(t, "user", ld.Spec.Namespace)

	ld, ok = db.lookupDocByLogicalOrStorageNs(disk)
	require.True(t, ok)
	require.Equal(t, disk, ld.StorageNs)

	ld, ok = db.lookupDocByLogicalOrStorageNs("session")
	require.True(t, ok)
	require.Equal(t, mem, ld.StorageNs)

	ld, ok = db.lookupDocByStorageNs(mem)
	require.True(t, ok)
	require.Equal(t, mem, ld.StorageNs)
	require.Equal(t, "session", ld.Spec.Namespace)

	_, ok = db.lookupDocByLogicalOrStorageNs("nope")
	require.False(t, ok)
}

func TestAutoTTLFromKey(t *testing.T) {
	db := openDB(testutil.DBPath(t))
	require.NotNil(t, db)
	defer func() { _ = db.Close() }()
	require.NoError(t, db.writeDocSpec(docSpec{Namespace: "user", Mem: false, KeyAttrs: []string{"id"}, TTL: time.Hour}))

	key := naming.BuildStorageKey(naming.BuildStorageNs("user", false), "abc")
	require.Equal(t, 3*time.Hour, db.autoTTLFromKey(key, 3*time.Hour), "explicit wins")
	require.Equal(t, time.Hour, db.autoTTLFromKey(key, 0), "doc TTL when explicit 0")

	uk := naming.BuildStorageKey(naming.BuildStorageNs("unknownns", false), "x")
	require.Equal(t, time.Duration(0), db.autoTTLFromKey(uk, 0))

	internal := naming.DocMetaKey("any", "placeholder")
	require.Equal(t, time.Duration(0), db.autoTTLFromKey(internal, 0))
}
