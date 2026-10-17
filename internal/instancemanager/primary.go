package instancemanager

import (
	"context"
	"fmt"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

// HeadlessServiceName returns the name of the headless Service that gives
// every Pod of the instance a stable DNS record.
func HeadlessServiceName(instance string) string {
	return instance + "-hl"
}

// PodFQDN returns the stable DNS name of a Pod of the instance. Unlike the Pod
// IP, it does not change when the Pod is recreated.
func PodFQDN(pod, instance, namespace string) string {
	return fmt.Sprintf("%s.%s.%s.svc", pod, HeadlessServiceName(instance), namespace)
}

// waitForPrimary returns status.currentPrimary of the RedisInstance, waiting
// until the operator has set it, which it does once the first Pod has an IP.
func waitForPrimary(ctx context.Context, c client.Reader, key types.NamespacedName) (string, error) {
	var primary string
	err := wait.PollUntilContextCancel(ctx, 2*time.Second, true, func(ctx context.Context) (bool, error) {
		var instance redisv1.RedisInstance
		if err := c.Get(ctx, key, &instance); err != nil {
			// The API server may be unreachable for a moment: try again.
			fmt.Fprintln(os.Stderr, "Could not read RedisInstance:", err)
			return false, nil
		}
		primary = instance.Status.CurrentPrimary
		return primary != "", nil
	})
	return primary, err
}
