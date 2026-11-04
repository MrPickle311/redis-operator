package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

func TestInstrument(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = redisv1.AddToScheme(scheme)
	c := CountingClient(fake.NewClientBuilder().WithScheme(scheme).Build())

	r := Instrument("RedisInstance", reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
		var pods corev1.PodList
		_ = c.List(ctx, &pods)
		_ = c.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "inst"}})
		return reconcile.Result{}, errors.New("boom")
	}))
	_, _ = r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKey{Namespace: "inst", Name: "cache"}})

	for _, check := range []struct {
		got  float64
		what string
	}{
		{testutil.ToFloat64(reconcilesTotal.WithLabelValues("RedisInstance", "inst", "cache", "error")), "one failed reconcile"},
		{testutil.ToFloat64(apiRequestsTotal.WithLabelValues("RedisInstance", "inst", "cache", "list", "Pod", "cache")), "a list of Pods from the cache"},
		{testutil.ToFloat64(apiRequestsTotal.WithLabelValues("RedisInstance", "inst", "cache", "create", "Secret", "api")), "a Secret created through the API"},
	} {
		if check.got != 1 {
			t.Errorf("%s: counted %v", check.what, check.got)
		}
	}
}
