// Package sealed encrypts a small secret (broker credentials) so that only the
// holder of a private key can read it.
//
// A sender seals to aegis's public key; aegis opens with its private key. Nobody
// else, including the sender afterwards, can open what was sealed. Each sealing
// uses a fresh one-time key, so two seals of the same secret look unrelated.
//
// Scheme: X25519 key agreement, HKDF-SHA256 to derive a key, AES-256-GCM to
// encrypt. Layout of a sealed value:
//
//	0x01 | one-time public key (32) | nonce (12) | ciphertext+tag
//
// The context argument (aad) is authenticated but not encrypted. Callers use it
// to tie a sealed value to where it may be used (for example the broker and
// environment), so a value sealed for one place fails to open anywhere else.
package sealed

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
)

const (
	version   = 1
	keyLen    = 32
	nonceLen  = 12
	headerLen = 1 + keyLen + nonceLen
	info      = "aegis/sealed/v1"
)

// ErrOpen is returned for every failure to open a sealed value. It deliberately
// says nothing about why, so it cannot be used to probe.
var ErrOpen = errors.New("sealed: cannot open")

// GenerateKey returns a new private/public key pair (32 bytes each).
func GenerateKey() (private, public []byte, err error) {
	k, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	return k.Bytes(), k.PublicKey().Bytes(), nil
}

// PublicKey derives the public key for a private key.
func PublicKey(private []byte) ([]byte, error) {
	k, err := ecdh.X25519().NewPrivateKey(private)
	if err != nil {
		return nil, fmt.Errorf("sealed: bad private key: %w", err)
	}
	return k.PublicKey().Bytes(), nil
}

func derive(shared, ephPub, recipientPub []byte) (cipher.AEAD, error) {
	salt := append(append([]byte{}, ephPub...), recipientPub...)
	key, err := hkdf.Key(sha256.New, shared, salt, info, keyLen)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// Seal encrypts plaintext to the recipient's public key, bound to aad.
func Seal(recipientPublic, aad, plaintext []byte) ([]byte, error) {
	pub, err := ecdh.X25519().NewPublicKey(recipientPublic)
	if err != nil {
		return nil, fmt.Errorf("sealed: bad public key: %w", err)
	}
	eph, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	shared, err := eph.ECDH(pub)
	if err != nil {
		return nil, err
	}
	aead, err := derive(shared, eph.PublicKey().Bytes(), pub.Bytes())
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, headerLen+len(plaintext)+aead.Overhead())
	out = append(out, version)
	out = append(out, eph.PublicKey().Bytes()...)
	nonce := make([]byte, nonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	out = append(out, nonce...)
	return aead.Seal(out, nonce, plaintext, aad), nil
}

// Open decrypts a sealed value with the recipient's private key. It fails (with
// ErrOpen, whatever the reason) if the value was altered, sealed for a different
// key, or sealed for a different aad.
func Open(private, aad, sealed []byte) ([]byte, error) {
	if len(sealed) < headerLen+16 || sealed[0] != version {
		return nil, ErrOpen
	}
	priv, err := ecdh.X25519().NewPrivateKey(private)
	if err != nil {
		return nil, ErrOpen
	}
	ephPub, err := ecdh.X25519().NewPublicKey(sealed[1 : 1+keyLen])
	if err != nil {
		return nil, ErrOpen
	}
	shared, err := priv.ECDH(ephPub)
	if err != nil {
		return nil, ErrOpen
	}
	aead, err := derive(shared, ephPub.Bytes(), priv.PublicKey().Bytes())
	if err != nil {
		return nil, ErrOpen
	}
	nonce := sealed[1+keyLen : headerLen]
	plain, err := aead.Open(nil, nonce, sealed[headerLen:], aad)
	if err != nil {
		return nil, ErrOpen
	}
	return plain, nil
}
