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
	err := c.do(ctx, http.MethodGet, host, "/status", nil, &status)
	return status, err
}

// ReplicaOf makes the instance at host replicate from primaryHost. It returns
// true when the replication target had to change. The Instance Manager rejects
// the command when it has already accepted a higher epoch.
func (c *Client) ReplicaOf(ctx context.Context, host string, epoch int64, primaryHost string) (bool, error) {
	var resp instancemanager.ReplicaOfResponse
	req := instancemanager.ReplicaOfRequest{Epoch: epoch, PrimaryHost: primaryHost}
	err := c.do(ctx, http.MethodPost, host, "/replicaof", req, &resp)
	return resp.Changed, err
}

// do sends body as JSON, unless it is nil, and decodes the response into out.
func (c *Client) do(ctx context.Context, method, host, path string, body, out any) error {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reqBody = bytes.NewReader(data)
	}

	url := "https://" + net.JoinHostPort(host, strconv.Itoa(c.port)) + path
	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("%s %s: %s: %s", method, url, resp.Status, bytes.TrimSpace(msg))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
