package sealed

import (
	"bytes"
	"errors"
	"testing"
)

func keys(t *testing.T) (priv, pub []byte) {
	t.Helper()
	priv, pub, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return priv, pub
}

func TestRoundTrip(t *testing.T) {
	priv, pub := keys(t)
	secret := []byte(`{"api_key":"k","email":"a@b.co","password":"hunter2"}`)
	blob, err := Seal(pub, []byte("capital|demo"), secret)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(blob, []byte("hunter2")) || bytes.Contains(blob, []byte("a@b.co")) {
		t.Fatal("the sealed value must not contain the secret")
	}
	got, err := Open(priv, []byte("capital|demo"), blob)
	if err != nil || !bytes.Equal(got, secret) {
		t.Fatalf("open: %q %v", got, err)
	}
}

func TestSealingTwiceLooksUnrelated(t *testing.T) {
	_, pub := keys(t)
	a, _ := Seal(pub, nil, []byte("same"))
	b, _ := Seal(pub, nil, []byte("same"))
	if bytes.Equal(a, b) {
		t.Fatal("two seals of the same secret must differ")
	}
}

func TestOnlyTheRightKeyAndContextOpensIt(t *testing.T) {
	priv, pub := keys(t)
	otherPriv, _ := keys(t)
	blob, _ := Seal(pub, []byte("capital|demo"), []byte("secret"))

	if _, err := Open(otherPriv, []byte("capital|demo"), blob); !errors.Is(err, ErrOpen) {
		t.Error("a different private key must not open it")
	}
	// Sealed for demo, presented as live: must not open.
	if _, err := Open(priv, []byte("capital|live"), blob); !errors.Is(err, ErrOpen) {
		t.Error("a different context must not open it")
	}
	if _, err := Open(priv, nil, blob); !errors.Is(err, ErrOpen) {
		t.Error("a missing context must not open it")
	}
}

func TestAlteredOrMalformedValuesDoNotOpen(t *testing.T) {
	priv, pub := keys(t)
	blob, _ := Seal(pub, []byte("c"), []byte("secret"))
	for i := range blob {
		bad := append([]byte{}, blob...)
		bad[i] ^= 0x01
		if _, err := Open(priv, []byte("c"), bad); err == nil {
			t.Fatalf("flipping byte %d must break it", i)
		}
	}
	for _, b := range [][]byte{nil, {}, {1}, blob[:10], blob[:len(blob)-1]} {
		if _, err := Open(priv, []byte("c"), b); !errors.Is(err, ErrOpen) {
			t.Errorf("malformed value %d bytes should not open", len(b))
		}
	}
}

func TestPublicKeyMatchesAndBadKeysAreRefused(t *testing.T) {
	priv, pub := keys(t)
	got, err := PublicKey(priv)
	if err != nil || !bytes.Equal(got, pub) {
		t.Fatal("PublicKey must return the pair's public key")
	}
	if _, err := Seal([]byte("short"), nil, []byte("x")); err == nil {
		t.Error("a malformed public key must be refused")
	}
	if _, err := PublicKey([]byte("short")); err == nil {
		t.Error("a malformed private key must be refused")
	}
}
