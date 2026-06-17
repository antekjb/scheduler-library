package upstreamsync

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
	fwk "k8s.io/kubernetes/pkg/scheduler/framework"
)

// LIBRARY CHANGE: New function proposed for k8s.io/kubernetes/pkg/scheduler/backend/cache/snapshot.go.
// This can be reused in preemption logic; a revert function would be useful for the reprieve phase of the preemption algorithm.
// AddPodToNode adds a new pod to the specific node and returns the corresponding revert function
func AddPodToNode(ctx context.Context, schedulerSnapshot *cache.Snapshot, pod *v1.Pod, nodeName string) (func(), error) {
	nodeInfo, _ := schedulerSnapshot.Get(nodeName)
	podInfo, _ := fwk.NewPodInfo(pod.DeepCopy())
	nodeInfo.AddPodInfo(podInfo)

	revertFn := func() {
		RemovePodFromNode(ctx, schedulerSnapshot, pod, nodeName)
	}

	return revertFn, nil
}

// LIBRARY CHANGE: New function proposed for k8s.io/kubernetes/pkg/scheduler/backend/cache/snapshot.go.
// This can be reused in preemption logic; a revert function would be useful for the reprieve phase of the preemption algorithm.
// RemovePodFromNode removes a pod from a specific node and returns the corresponding revert function.
func RemovePodFromNode(ctx context.Context, schedulerSnapshot *cache.Snapshot, pod *v1.Pod, nodeName string) (func(), error) {
	logger := klog.FromContext(ctx)
	nodeInfo, _ := schedulerSnapshot.Get(nodeName)

	nodeInfo.RemovePod(logger, pod)

	revertFn := func() {
		AddPodToNode(ctx, schedulerSnapshot, pod, nodeName)
	}

	return revertFn, nil
}
