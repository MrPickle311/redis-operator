package controller

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	labelKind = "kind" // the label, and log key, that names the kind of an object
	verbGet   = "get"
)

// What the operator does for every object, which controller-runtime counts
// per controller only: the reconciles of each RedisInstance, RedisCluster and
// RedisUser, and the calls to the API server they make.
var (
	reconcilesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "redis_operator_reconcile_total",
		Help: "Reconciles of every object, by result: success, requeue, requeue_after or error.",
	}, []string{labelKind, "namespace", "name", "result"})
	reconcileSeconds = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "redis_operator_reconcile_seconds_total",
		Help: "Time spent reconciling every object.",
	}, []string{labelKind, "namespace", "name"})
	apiRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "redis_operator_api_requests_total",
		Help: "Calls of the client of the reconcile of an object, by verb and the kind they are about; " +
			"source cache for reads the informers answer, api for writes to the API server.",
	}, []string{labelKind, "namespace", "name", "verb", "resource", "source"})
)

func init() {
	metrics.Registry.MustRegister(reconcilesTotal, reconcileSeconds, apiRequestsTotal)
}

// reconcileTarget is the object a reconcile is about, in its context.
type reconcileTarget struct{ kind, namespace, name string }

type reconcileTargetKey struct{}

// Instrument counts the reconciles of r per object, and tells the client of
// CountingClient which object its calls are for.
func Instrument(kind string, r reconcile.Reconciler) reconcile.Reconciler {
	return reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
		target := reconcileTarget{kind, req.Namespace, req.Name}
		start := time.Now()
		res, err := r.Reconcile(context.WithValue(ctx, reconcileTargetKey{}, target), req)
		result := "success"
		switch {
		case err != nil:
			result = "error"
		case res.RequeueAfter > 0:
			result = "requeue_after"
		case !res.IsZero():
			result = "requeue"
		}
		reconcilesTotal.WithLabelValues(kind, req.Namespace, req.Name, result).Inc()
		reconcileSeconds.WithLabelValues(kind, req.Namespace, req.Name).Add(time.Since(start).Seconds())
		return res, err
	})
}

// CountingClient counts every call of c in redis_operator_api_requests_total,
// by the object whose reconcile made it (see Instrument).
func CountingClient(c client.Client) client.Client {
	return countingClient{Client: c}
}

type countingClient struct{ client.Client }

func (c countingClient) count(ctx context.Context, verb string, obj runtime.Object) {
	target, _ := ctx.Value(reconcileTargetKey{}).(reconcileTarget)
	resource := reflect.TypeOf(obj).Elem().Name()
	if gvk, err := apiutil.GVKForObject(obj, c.Scheme()); err == nil {
		resource = gvk.Kind
	}
	source := "api"
	if verb == verbGet || verb == "list" {
		source = "cache"
	}
	apiRequestsTotal.WithLabelValues(target.kind, target.namespace, target.name, verb,
		strings.TrimSuffix(resource, "List"), source).Inc()
}

func (c countingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.count(ctx, verbGet, obj)
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c countingClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.count(ctx, "list", list)
	return c.Client.List(ctx, list, opts...)
}

func (c countingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	c.count(ctx, "create", obj)
	return c.Client.Create(ctx, obj, opts...)
}

func (c countingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	c.count(ctx, "update", obj)
	return c.Client.Update(ctx, obj, opts...)
}

func (c countingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	c.count(ctx, "patch", obj)
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c countingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	c.count(ctx, "delete", obj)
	return c.Client.Delete(ctx, obj, opts...)
}

func (c countingClient) Status() client.SubResourceWriter {
	return countingStatus{SubResourceWriter: c.Client.Status(), client: c}
}

type countingStatus struct {
	client.SubResourceWriter
	client countingClient
}

func (s countingStatus) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	s.client.count(ctx, "update/status", obj)
	return s.SubResourceWriter.Update(ctx, obj, opts...)
}

func (s countingStatus) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	s.client.count(ctx, "patch/status", obj)
	return s.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}
