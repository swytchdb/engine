/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 *
 * Swytch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Swytch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Swytch. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permissions under GNU AGPL version 3 section 7 apply to
 * this file. See the NOTICE.md file distributed with this source code
 * for the current set of additional permissions.
 */

package cluster

import (
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"fmt"
	"math/big"
	"net"
	"sync"
	"time"

	"golang.org/x/crypto/hkdf"
)

var (
	// Fixed epoch for deterministic CA cert validity. All nodes must produce
	// identical CA certificate bytes, so the validity window is anchored to
	// a fixed point in time rather than "now".
	caNotBefore = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	caNotAfter  = caNotBefore.Add(100 * 365 * 24 * time.Hour) // ~100 years

	// Leaf certs are short-lived; leafSource re-mints them at half-life.
	leafValidity = 7 * 24 * time.Hour
)

// DeriveCAFromPassphrase deterministically derives an Ed25519 CA key pair and
// self-signed CA certificate from a passphrase. Every node with the same
// passphrase produces identical output.
func DeriveCAFromPassphrase(passphrase string) (ed25519.PrivateKey, *x509.Certificate, error) {
	// HKDF-SHA256: passphrase → 32-byte Ed25519 seed
	hkdfReader := hkdf.New(sha256.New, []byte(passphrase), []byte("swytch-cluster-ca-v1"), []byte("ca-key"))
	seed := make([]byte, ed25519.SeedSize)
	if _, err := hkdfReader.Read(seed); err != nil {
		return nil, nil, fmt.Errorf("HKDF derivation failed: %w", err)
	}

	caKey := ed25519.NewKeyFromSeed(seed)

	// Deterministic serial number derived from the public key.
	pubHash := sha256.Sum256(caKey.Public().(ed25519.PublicKey))
	serial := new(big.Int).SetBytes(pubHash[:16])

	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "swytch-cluster-ca"},
		NotBefore:             caNotBefore,
		NotAfter:              caNotAfter,
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}

	// Self-sign. Because template == parent and the key is deterministic,
	// every node produces byte-identical DER output.
	caDER, err := x509.CreateCertificate(deterministicReader{}, template, template, caKey.Public(), caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create CA certificate: %w", err)
	}

	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse CA certificate: %w", err)
	}

	return caKey, caCert, nil
}

// GenerateLeafCert creates an ephemeral leaf certificate signed by the given CA.
// The leaf key is randomly generated (not deterministic) and the cert is
// short-lived. nodeAddr is embedded as a DNS SAN (or IP SAN if it parses as IP).
func GenerateLeafCert(caKey crypto.Signer, caCert *x509.Certificate, nodeAddr string) (tls.Certificate, error) {
	// Ephemeral leaf key — fresh each startup.
	_, leafKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to generate leaf key: %w", err)
	}

	// Strip port if present.
	host := nodeAddr
	if h, _, err := net.SplitHostPort(nodeAddr); err == nil {
		host = h
	}

	// Random serial for the leaf.
	serialBytes := make([]byte, 16)
	if _, err := rand.Read(serialBytes); err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to generate serial: %w", err)
	}
	serial := new(big.Int).SetBytes(serialBytes)

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: host},
		NotBefore:    now.Add(-5 * time.Minute), // small clock-skew tolerance
		NotAfter:     now.Add(leafValidity),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}

	if ip := net.ParseIP(host); ip != nil {
		template.IPAddresses = []net.IP{ip}
	} else {
		template.DNSNames = []string{host}
	}

	leafDER, err := x509.CreateCertificate(rand.Reader, template, caCert, leafKey.Public(), caKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to create leaf certificate: %w", err)
	}

	leafCert, err := x509.ParseCertificate(leafDER)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to parse leaf certificate: %w", err)
	}

	return tls.Certificate{
		Certificate: [][]byte{leafDER, caCert.Raw},
		PrivateKey:  leafKey,
		Leaf:        leafCert,
	}, nil
}

// leafSource hands out this node's leaf certificate for TLS handshakes,
// re-minting it at half-life so nodes that outlive leafValidity keep
// forming new connections. Established connections never recheck cert
// validity, so rotation only ever affects handshakes.
type leafSource struct {
	caKey    crypto.Signer
	caCert   *x509.Certificate
	nodeAddr string

	mu   sync.Mutex
	leaf *tls.Certificate
}

func newLeafSource(caKey crypto.Signer, caCert *x509.Certificate, nodeAddr string) (*leafSource, error) {
	s := &leafSource{caKey: caKey, caCert: caCert, nodeAddr: nodeAddr}
	if err := s.mint(); err != nil {
		return nil, err
	}
	return s, nil
}

// mint replaces the cached leaf. Callers other than newLeafSource must hold mu.
func (s *leafSource) mint() error {
	leaf, err := GenerateLeafCert(s.caKey, s.caCert, s.nodeAddr)
	if err != nil {
		return err
	}
	s.leaf = &leaf
	return nil
}

// current returns the cached leaf, re-minting once less than half of
// leafValidity remains. A re-mint swaps the pointer rather than mutating
// the certificate, so callers may hold the result across a handshake.
func (s *leafSource) current() (*tls.Certificate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Until(s.leaf.Leaf.NotAfter) < leafValidity/2 {
		if err := s.mint(); err != nil {
			return nil, err
		}
	}
	return s.leaf, nil
}

// GeneratePassphrase returns a cryptographically random passphrase suitable
// for use with DeriveCAFromPassphrase. The result is 32 random bytes encoded
// as base64 RawURL (43 characters, no padding).
func GeneratePassphrase() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate random bytes: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// deterministicReader is an io.Reader that always returns zeros.
// Used when creating the CA certificate so that x509.CreateCertificate
// produces deterministic output (Ed25519 signing is deterministic and
// doesn't consume randomness, but the function signature requires a reader).
type deterministicReader struct{}

func (deterministicReader) Read(b []byte) (int, error) {
	clear(b)
	return len(b), nil
}
