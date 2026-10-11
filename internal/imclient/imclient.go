package imclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/MrPickle311/redis-operator/internal/instancemanager"
	"github.com/MrPickle311/redis-operator/internal/pki"
)

// clientCertValidity only has to cover a single TLS handshake.
const clientCertValidity = time.Hour

// Client talks to the Instance Managers over mTLS.
type Client struct {
	http *http.Client
	port int
}

// New returns a client that trusts only Instance Managers with a certificate
// from ca and authenticates as the operator with a certificate from it.
func New(ca pki.KeyPair) (*Client, error) {
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca.CertPEM) {
		return nil, errors.New("no CA certificate found")
	}

	transport := &http.Transport{TLSClientConfig: &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    roots,
		// A fresh certificate per handshake never expires in a long-running
		// operator, and issuing one takes well under a millisecond.
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert, err := pki.IssueClientCert(ca, instancemanager.OperatorCommonName, clientCertValidity)
			if err != nil {
				return nil, err
			}
			pair, err := tls.X509KeyPair(cert.CertPEM, cert.KeyPEM)
			return &pair, err
		},
	}}

	return &Client{
		http: &http.Client{Transport: transport, Timeout: 10 * time.Second},
		port: instancemanager.StatusPort,
	}, nil
}

// Status returns the replication state of the instance at host, its Pod DNS name.
func (c *Client) Status(ctx context.Context, host string) (instancemanager.InstanceStatus, error) {
	var status instancemanager.InstanceStatus

	url := "https://" + net.JoinHostPort(host, strconv.Itoa(c.port)) + "/status"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return status, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return status, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return status, fmt.Errorf("GET %s: %s: %s", url, resp.Status, bytes.TrimSpace(body))
	}
	err = json.NewDecoder(resp.Body).Decode(&status)
	return status, err
}
