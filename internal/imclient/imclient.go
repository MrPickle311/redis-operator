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
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

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
	var resp instancemanager.CommandResponse
	req := instancemanager.ReplicaOfRequest{Epoch: epoch, PrimaryHost: primaryHost}
	err := c.do(ctx, http.MethodPost, host, "/replicaof", req, &resp)
	return resp.Changed, err
}

// Fence makes the instance at host hold back writes until it is promoted or
// made a replica of another instance.
func (c *Client) Fence(ctx context.Context, host string, epoch int64) error {
	var resp instancemanager.CommandResponse
	return c.do(ctx, http.MethodPost, host, "/fence", instancemanager.EpochRequest{Epoch: epoch}, &resp)
}

// Promote makes the instance at host a primary that accepts writes. It returns
// true when the instance was a replica before.
func (c *Client) Promote(ctx context.Context, host string, epoch int64) (bool, error) {
	var resp instancemanager.CommandResponse
	err := c.do(ctx, http.MethodPost, host, "/promote", instancemanager.EpochRequest{Epoch: epoch}, &resp)
	return resp.Changed, err
}

// Config makes the running Redis at host match req. Options Redis refuses
// are in the response; the others are applied anyway.
func (c *Client) Config(ctx context.Context, host string, req instancemanager.ConfigRequest) (instancemanager.ConfigResponse, error) {
	var resp instancemanager.ConfigResponse
	err := c.do(ctx, http.MethodPost, host, "/config", req, &resp)
	return resp, err
}

// Cluster runs POST /cluster/<command> on the node of a Redis Cluster at
// host: meet, addslots, replicate, failover, forget, importing or setnode.
// It returns true when the command changed the node.
func (c *Client) Cluster(ctx context.Context, host, command string, req instancemanager.ClusterRequest) (bool, error) {
	var resp instancemanager.CommandResponse
	err := c.do(ctx, http.MethodPost, host, "/cluster/"+command, req, &resp)
	return resp.Changed, err
}

// Migrate runs POST /cluster/migrate on the master at host, which moves keys
// of req.Slot to the master req.NodeID for a few seconds.
func (c *Client) Migrate(ctx context.Context, host string, req instancemanager.ClusterRequest) (instancemanager.MigrateResponse, error) {
	var resp instancemanager.MigrateResponse
	err := c.do(ctx, http.MethodPost, host, "/cluster/migrate", req, &resp)
	return resp, err
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
	start := time.Now()
	resp, err := c.http.Do(req)
	observe(host, path, resp, err, time.Since(start))
	if err != nil {
		return withNetworkPolicyHint(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("%s %s: %s: %s", method, url, resp.Status, bytes.TrimSpace(msg))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// withNetworkPolicyHint names the usual cause of a timeout: a NetworkPolicy
// drops the packets, so the operator gets no answer instead of a refusal.
func withNetworkPolicyHint(err error) error {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return fmt.Errorf("%w (no answer on port %d: is a NetworkPolicy of the namespace blocking the operator?)", err, instancemanager.StatusPort)
	}
	return err
}

// The calls of the operator to the Instance Managers, by the instance and Pod
// they went to.
var (
	requestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "redis_operator_im_requests_total",
		Help: "Calls of the operator to the API of the Instance Managers, by path and HTTP status, or error.",
	}, []string{"namespace", "redis_instance", "pod", "path", "code"})
	requestSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "redis_operator_im_request_duration_seconds",
		Help:    "How long a call of the operator to an Instance Manager took.",
		Buckets: []float64{.002, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"path"})
)

func init() {
	metrics.Registry.MustRegister(requestsTotal, requestSeconds)
}

// observe counts a call to host, a Pod DNS name "<pod>.<instance>-hl.<namespace>.svc".
func observe(host, path string, resp *http.Response, err error, took time.Duration) {
	parts := strings.Split(host, ".")
	pod, instance, namespace := parts[0], "", ""
	if len(parts) > 2 {
		instance, namespace = strings.TrimSuffix(parts[1], "-hl"), parts[2]
	}
	code := "error"
	if err == nil {
		code = strconv.Itoa(resp.StatusCode)
	}
	requestsTotal.WithLabelValues(namespace, instance, pod, path, code).Inc()
	requestSeconds.WithLabelValues(path).Observe(took.Seconds())
}
