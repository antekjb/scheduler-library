package upstreamsync

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/parallelize"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/dynamicresources"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

var clearNominatedNode = &fwk.NominatingInfo{NominatingMode: fwk.ModeOverride, NominatedNodeName: ""}

// AlgorithmResult is copied from k8s.io/kubernetes/pkg/scheduler/schedule_one.go.
// LIBRARY CHANGE: Exported to allow usage from the snapshot package.
type AlgorithmResult struct {
	// pod is the pod the result applies to.
	Pod *v1.Pod
	// scheduleResult is a scheduling algorithm result.
	ScheduleResult ScheduleResult
	// podCtx is a specific pod scheduling context used for the scheduling algorithm.
	podCtx *podSchedulingContext
	// schedulingDuration is a pod scheduling duration used for metrics recording.
	schedulingDuration time.Duration
	// requiresPreemption determines whether this pod requires a preemption to proceed or not.
	requiresPreemption bool
	// status is a scheduling algorithm status.
	Status *fwk.Status
	// permitStatus is a status of the permit check.
	// This is only set when the `status` is success or the `requiresPreemption` is true.
	permitStatus *fwk.Status
}

// podSchedulingContext is copied from k8s.io/kubernetes/pkg/scheduler/schedule_one.go.
// It holds the precomputed data needed to handle the pod scheduling.
// Each scheduling attempt in the same pod group scheduling cycle for the same pod
// should use a new podSchedulingContext.
type podSchedulingContext struct {
	logger         klog.Logger
	state          *framework.CycleState
	podsToActivate *framework.PodsToActivate
}

// ScheduleResult wraps the upstream k8s.io/kubernetes/pkg/scheduler.ScheduleResult struct
// as the upstream ScheduleResult does not export its nominatingInfo property.
type ScheduleResult struct {
	scheduler.ScheduleResult
	nominatingInfo *fwk.NominatingInfo
}

// Scheduler wraps the upstream k8s.io/kubernetes/pkg/scheduler.Scheduler. Since the upstream
// Scheduler does not export nextStartNodeIndex or nodeInfoSnapshot, we implement
// local functions for the Scheduler receiver to duplicate functionality from the upstream.
// LIBRARY CHANGE: Created custom wrapper to track snapshot and start index state.
type Scheduler struct {
	*scheduler.Scheduler
	nextStartNodeIndex int
	nodeInfoSnapshot   *cache.Snapshot
}

// NewScheduler creates a new wrapper around the upstream Scheduler.
func NewScheduler(sched *scheduler.Scheduler, snapshot *cache.Snapshot) *Scheduler {
	return &Scheduler{
		Scheduler:        sched,
		nodeInfoSnapshot: snapshot,
	}
}

// schedulingAlgorithm is copied from k8s.io/kubernetes/pkg/scheduler/schedule_one.go
// because it is not exported upstream.
func (sched *Scheduler) schedulingAlgorithm(
	ctx context.Context,
	state fwk.CycleState,
	schedFramework framework.Framework,
	podInfo *framework.QueuedPodInfo,
	start time.Time,
) (ScheduleResult, *fwk.Status) {
	defer func() {
		metrics.SchedulingAlgorithmLatency.Observe(metrics.SinceInSeconds(start))
	}()

	pod := podInfo.Pod

	logger := klog.FromContext(ctx)
	scheduleResult, err := sched.SchedulePod(ctx, schedFramework, state, podInfo)
	localScheduleRes := ScheduleResult{
		ScheduleResult: scheduleResult,
	}

	if err != nil {
		if err == scheduler.ErrNoNodesAvailable {
			status := fwk.NewStatus(fwk.UnschedulableAndUnresolvable).WithError(err)
			return ScheduleResult{nominatingInfo: clearNominatedNode}, status
		}

		fitError, ok := err.(*framework.FitError)
		if !ok {
			logger.Error(err, "Error selecting node for pod", "pod", klog.KObj(pod))
			return ScheduleResult{nominatingInfo: clearNominatedNode}, fwk.AsStatus(err)
		}

		// SchedulePod() may have failed because the pod would not fit on any host, so we try to
		// preempt, with the expectation that the next time the pod is tried for scheduling it
		// will fit due to the preemption. It is also possible that a different pod will schedule
		// into the resources that were preempted, but this is harmless.

		if !schedFramework.HasPostFilterPlugins() {
			logger.V(3).Info("No PostFilter plugins are registered, so no preemption will be performed")
			return ScheduleResult{nominatingInfo: clearNominatedNode}, fwk.NewStatus(fwk.Unschedulable).WithError(err)
		}

		// Run PostFilter plugins to attempt to make the pod schedulable in a future scheduling cycle.
		result, status := schedFramework.RunPostFilterPlugins(ctx, state, pod, fitError.Diagnosis.NodeToStatus)
		msg := status.Message()
		fitError.Diagnosis.PostFilterMsg = msg
		if status.Code() == fwk.Error {
			utilruntime.HandleErrorWithContext(ctx, nil, "Status after running PostFilter plugins for pod", "pod", klog.KObj(pod), "status", msg)
		} else {
			logger.V(5).Info("Status after running PostFilter plugins for pod", "pod", klog.KObj(pod), "status", msg)
		}

		var nominatingInfo *fwk.NominatingInfo
		if result != nil {
			nominatingInfo = result.NominatingInfo
		}
		return ScheduleResult{nominatingInfo: nominatingInfo}, fwk.NewStatus(fwk.Unschedulable).WithError(err)
	}
	return localScheduleRes, nil
}

// tryScheduling performs a tentative scheduling of a pod by running the scheduling
// algorithm and assuming the pod in memory.
// LIBRARY CHANGE: This new function extracts the scheduling and assumption logic from the
// k8s.io/kubernetes/pkg/scheduler/schedule_one_podgroup.go file's podGroupPodSchedulingAlgorithm function.
// It also provides a corresponding revertFn, which can be reused by both the podGroupPodSchedulingAlgorithm and ScheduleOnePod functions.
func (sched *Scheduler) tryScheduling(ctx context.Context,
	state fwk.CycleState,
	schedFramework framework.Framework,
	podInfo *framework.QueuedPodInfo,
) (*AlgorithmResult, *framework.QueuedPodInfo, func()) {
	pod := podInfo.GetPod()

	requiresPreemption := false
	scheduleResult, status := sched.schedulingAlgorithm(ctx, state, schedFramework, podInfo, time.Now())
	if !status.IsSuccess() {
		if scheduleResult.nominatingInfo != nil && scheduleResult.nominatingInfo.NominatedNodeName != "" {
			// If the NominatedNodeName is set, the preemption is required.
			// Continue with assuming and reserving, because the subsequent pods from this group
			// have to see this one as already scheduled on its nominated place.
			// Set SuggestedHost to NominatedNodeName to handle the pod similarly to one that is feasible.
			scheduleResult.SuggestedHost = scheduleResult.nominatingInfo.NominatedNodeName
			requiresPreemption = true
		} else {
			// In case of pod being just unschedulable or having an error, just return now.
			return &AlgorithmResult{
				Pod:            pod,
				ScheduleResult: scheduleResult,
				Status:         status,
			}, nil, nil
		}
	}
	assumedPodInfo, assumeStatus := sched.assumeAndReserve(ctx, state, schedFramework, podInfo, scheduleResult.ScheduleResult)
	if !assumeStatus.IsSuccess() {
		return &AlgorithmResult{
			Pod:            pod,
			ScheduleResult: ScheduleResult{nominatingInfo: clearNominatedNode},
			Status:         assumeStatus,
		}, nil, nil
	}

	revertFn := func() {
		err := sched.unreserveAndForget(ctx, state, schedFramework, assumedPodInfo, scheduleResult.SuggestedHost)
		if err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "ForgetPod failed")
		}
	}

	return &AlgorithmResult{
		Pod:                pod,
		ScheduleResult:     scheduleResult,
		Status:             status,
		requiresPreemption: requiresPreemption,
	}, assumedPodInfo, revertFn

}

// ScheduleOnePod simulates a single scheduling cycle for a pod against the given candidate nodes.
// LIBRARY CHANGE: Completely custom entrypoint API that wraps simulation in withPlacement.
func (sched *Scheduler) ScheduleOnePod(ctx context.Context, pod *v1.Pod, candidateNodes []string) (*AlgorithmResult, func(), error) {
	fwk, err := sched.FrameworkForPod(pod)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get framework for pod %s: %w", klog.KObj(pod), err)
	}

	cycleState := framework.NewCycleState()
	cycleState.SetPodGroupSchedulingCycle(framework.NewCycleState())
	podInfo, err := framework.NewPodInfo(pod)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create pod info for pod %s: %w", klog.KObj(pod), err)
	}

	var simRes *AlgorithmResult
	var assumedPodInfo *framework.QueuedPodInfo
	var revertFn func()

	err = sched.withPlacement(candidateNodes, func() error {
		simRes, assumedPodInfo, revertFn = sched.tryScheduling(ctx, cycleState, fwk, &framework.QueuedPodInfo{PodInfo: podInfo})
		return nil
	})

	if err != nil {
		return nil, nil, fmt.Errorf("failed to simulate scheduling for pod %s: %w", klog.KObj(pod), err)
	}

	if !simRes.Status.IsSuccess() {
		return simRes, nil, nil
	}

	if assumedPodInfo == nil {
		return nil, nil, fmt.Errorf("pod %s was not assumed", klog.KObj(pod))
	}

	pod.Spec.NodeName = simRes.ScheduleResult.SuggestedHost
	return simRes, revertFn, nil
}

// LIBRARY CHANGE: Custom placement wrapper implementing snapshot modifications during callback.
func (sched *Scheduler) withPlacement(candidates []string, fn func() error) error {
	nodes := make([]fwk.NodeInfo, 0, len(candidates))
	for _, name := range candidates {
		ni, err := sched.nodeInfoSnapshot.NodeInfos().Get(name)
		if err != nil {
			return fmt.Errorf("node %s not in snapshot: %w", name, err)
		}
		nodes = append(nodes, ni)
	}
	if err := sched.nodeInfoSnapshot.AssumePlacement(&fwk.Placement{Nodes: nodes}); err != nil {
		return err
	}
	defer sched.nodeInfoSnapshot.ForgetPlacement()
	return fn()
}

// FrameworkForPod is copied from k8s.io/kubernetes/pkg/scheduler/scheduler.go
// because it is not exported upstream.
// LIBRARY CHANGE: Exported to be accessible from other packages.
func (sched *Scheduler) FrameworkForPod(pod *v1.Pod) (framework.Framework, error) {
	schedulerName := pod.Spec.SchedulerName
	if schedulerName == "" {
		schedulerName = v1.DefaultSchedulerName
	}
	fwk, ok := sched.Profiles[schedulerName]
	if !ok {
		return nil, fmt.Errorf("profile not found for scheduler name %q", schedulerName)
	}
	return fwk, nil
}

// assumeAndReserve is copied from k8s.io/kubernetes/pkg/scheduler/schedule_one.go
// because it is not exported upstream.
func (sched *Scheduler) assumeAndReserve(
	ctx context.Context,
	state fwk.CycleState,
	schedFramework framework.Framework,
	podInfo *framework.QueuedPodInfo,
	scheduleResult scheduler.ScheduleResult,
) (*framework.QueuedPodInfo, *fwk.Status) {
	logger := klog.FromContext(ctx)
	// Tell the cache to assume that a pod now is running on a given node, even though it hasn't been bound yet.
	// This allows us to keep scheduling without waiting on binding to occur.
	assumedPodInfo := podInfo.DeepCopy()
	assumedPod := assumedPodInfo.Pod
	// assume modifies `assumedPod` by setting NodeName=scheduleResult.SuggestedHost
	err := sched.assume(logger, state, assumedPodInfo, scheduleResult.SuggestedHost)
	if err != nil {
		// This is most probably result of a BUG in retrying logic.
		// We report an error here so that pod scheduling can be retried.
		// This relies on the fact that Error will check if the pod has been bound
		// to a node and if so will not add it back to the unscheduled pods queue
		// (otherwise this would cause an infinite loop).
		return assumedPodInfo, fwk.AsStatus(err)
	}

	// Run the Reserve method of reserve plugins.
	if sts := schedFramework.RunReservePluginsReserve(ctx, state, assumedPod, scheduleResult.SuggestedHost); !sts.IsSuccess() {
		// trigger un-reserve to clean up state associated with the reserved Pod
		err := sched.unreserveAndForget(ctx, state, schedFramework, assumedPodInfo, scheduleResult.SuggestedHost)
		if err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "ForgetPod failed")
		}

		if sts.IsRejected() {
			fitErr := &framework.FitError{
				NumAllNodes: 1,
				Pod:         podInfo.Pod,
				Diagnosis: framework.Diagnosis{
					NodeToStatus: framework.NewDefaultNodeToStatus(),
				},
			}
			fitErr.Diagnosis.NodeToStatus.Set(scheduleResult.SuggestedHost, sts)
			fitErr.Diagnosis.AddPluginStatus(sts)
			return assumedPodInfo, fwk.NewStatus(sts.Code()).WithError(fitErr)
		}
		return assumedPodInfo, sts
	}
	return assumedPodInfo, nil
}

// assume is copied from k8s.io/kubernetes/pkg/scheduler/schedule_one.go
// because it is not exported upstream.
func (sched *Scheduler) assume(logger klog.Logger, state fwk.CycleState, assumedPodInfo *framework.QueuedPodInfo, host string) error {
	// Optimistically assume that the binding will succeed and send it to apiserver
	// in the background.
	// If the binding fails, scheduler will release resources allocated to assumed pod
	// immediately.
	assumedPodInfo.Pod.Spec.NodeName = host
	if utilfeature.DefaultFeatureGate.Enabled(features.DRANodeAllocatableResources) {
		// If DRANodeAllocatableResources is enabled, copy the calculated node allocatable resource claim status
		// from the cycle state to the assumed pod's status. This ensures that the scheduler's
		// cached version of the pod reflects the node allocatable resources allocated by the DRA plugin
		// for this scheduling cycle, making this information available for NodeInfo cache update.
		// Any potential NodeAllocatableResourceClaimStatuses from a previously failed scheduling attempt is overwritten.
		// This field is not explicitly cleared as the Pod object is reconstructed in handleSchedulingFailure()
		// before re-queueing.
		assumedPodInfo.Pod.Status.NodeAllocatableResourceClaimStatuses = dynamicresources.ExtractPodNodeAllocatableResourceClaimStatus(logger, state, host)
	}

	if state.IsPodGroupSchedulingCycle() {
		err := sched.nodeInfoSnapshot.AssumePod(assumedPodInfo.PodInfo)
		if err != nil {
			logger.Error(err, "Scheduler snapshot AssumePod failed")
			return err
		}
	} else {
		if err := sched.Cache.AssumePod(logger, assumedPodInfo.Pod); err != nil {
			logger.Error(err, "Scheduler cache AssumePod failed")
			return err
		}
	}
	// if "assumed" is a nominated pod, we should remove it from internal cache
	if sched.SchedulingQueue != nil {
		sched.SchedulingQueue.DeleteNominatedPodIfExists(assumedPodInfo.Pod)
	}

	return nil
}

// unreserveAndForget is copied from k8s.io/kubernetes/pkg/scheduler/schedule_one.go
// because it is not exported upstream.
func (sched *Scheduler) unreserveAndForget(
	ctx context.Context,
	state fwk.CycleState,
	schedFramework framework.Framework,
	assumedPodInfo *framework.QueuedPodInfo,
	nodeName string,
) error {
	logger := klog.FromContext(ctx)

	schedFramework.RunReservePluginsUnreserve(ctx, state, assumedPodInfo.Pod, nodeName)
	if state.IsPodGroupSchedulingCycle() {
		err := sched.nodeInfoSnapshot.ForgetPod(logger, assumedPodInfo.Pod)
		if err != nil {
			return err
		}
		if assumedPodInfo.Pod.Status.NominatedNodeName != "" {
			// Assume method removed the nomination, but since we are reverting that stage for pod groups,
			// we need to revert that operation as well.
			if sched.SchedulingQueue != nil {
				nominatingInfo := &fwk.NominatingInfo{
					NominatedNodeName: assumedPodInfo.Pod.Status.NominatedNodeName,
					NominatingMode:    fwk.ModeOverride,
				}
				// AssumedPodInfo can be used here, because the whole pod object is not stored in the nominator.
				sched.SchedulingQueue.AddNominatedPod(logger, assumedPodInfo.PodInfo, nominatingInfo)
			}
		}
		return nil
	}
	return sched.Cache.ForgetPod(logger, assumedPodInfo.Pod)
}

// FindNodesThatFitPodSkippingExtenders duplicates logic from findNodesThatFitPod.
// LIBRARY CHANGE: Added a candidateNodes argument to allow filtering by specific nodes.
func (sched *Scheduler) FindNodesThatFitPodSkippingExtenders(
	ctx context.Context,
	schedFramework framework.Framework,
	state fwk.CycleState,
	podInfo *framework.QueuedPodInfo,
	candidateNodes []string,
) ([]fwk.NodeInfo, string, error) {
	logger := klog.FromContext(ctx)
	diagnosis := framework.Diagnosis{
		NodeToStatus: framework.NewDefaultNodeToStatus(),
	}

	var feasibleNodes []fwk.NodeInfo
	// LIBRARY CHANGE: Wrapping filtering logic in withPlacement and omitting extenders logic.
	err := sched.withPlacement(candidateNodes, func() error {
		allNodes, err := sched.nodeInfoSnapshot.ListNodesInPlacement()
		if err != nil {
			return err
		}

		// Run "prefilter" plugins.
		pod := podInfo.Pod
		preRes, s, unscheduledPlugins := schedFramework.RunPreFilterPlugins(ctx, state, pod)
		diagnosis.UnschedulablePlugins = unscheduledPlugins
		if !s.IsSuccess() {
			if !s.IsRejected() {
				return s.AsError()
			}
			// All nodes in NodeToStatus will have the same status so that they can be handled in the preemption.
			diagnosis.NodeToStatus.SetAbsentNodesStatus(s)

			// Record the messages from PreFilter in Diagnosis.PreFilterMsg.
			msg := s.Message()
			diagnosis.PreFilterMsg = msg
			logger.V(5).Info("Status after running PreFilter plugins for pod", "pod", klog.KObj(pod), "status", msg)
			diagnosis.AddPluginStatus(s)
			return nil
		}

		nodes := allNodes
		if !preRes.AllNodes() {
			nodes = make([]fwk.NodeInfo, 0, len(preRes.NodeNames))
			for nodeName := range preRes.NodeNames {
				// PreRes may return nodeName(s) which do not exist; we verify
				// node exists in the Snapshot within the selected placement.
				if nodeInfo, err := sched.nodeInfoSnapshot.GetNodeInPlacement(nodeName); err == nil {
					nodes = append(nodes, nodeInfo)
				}
			}
			diagnosis.NodeToStatus.SetAbsentNodesStatus(fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("node(s) didn't satisfy plugin(s) %v", sets.List(unscheduledPlugins))))
		}
		var errFilters error
		feasibleNodes, errFilters = sched.findNodesThatPassFilters(ctx, schedFramework, state, pod, &diagnosis, nodes)
		if errFilters != nil {
			return errFilters
		}
		// always try to update the sched.nextStartNodeIndex regardless of whether an error has occurred
		// this is helpful to make sure that all the nodes have a chance to be searched
		if len(allNodes) > 0 {
			processedNodes := len(feasibleNodes) + diagnosis.NodeToStatus.Len()
			sched.nextStartNodeIndex = (sched.nextStartNodeIndex + processedNodes) % len(allNodes)
		}
		return nil
	})

	if err != nil {
		return nil, "", err
	}

	return feasibleNodes, "", nil
}

// findNodesThatPassFilters is copied from k8s.io/kubernetes/pkg/scheduler/schedule_one.go
// because it is not exported upstream.
func (sched *Scheduler) findNodesThatPassFilters(
	ctx context.Context,
	schedFramework framework.Framework,
	state fwk.CycleState,
	pod *v1.Pod,
	diagnosis *framework.Diagnosis,
	nodes []fwk.NodeInfo) ([]fwk.NodeInfo, error) {
	numAllNodes := len(nodes)
	//LIBRARY CHANGE: For simplicty ignored limitation logic from k/k
	numNodesToFind := int32(numAllNodes)

	// Create feasible list with enough space to avoid growing it
	// and allow assigning.
	feasibleNodes := make([]fwk.NodeInfo, numNodesToFind)

	if !schedFramework.HasFilterPlugins() {
		for i := range feasibleNodes {
			feasibleNodes[i] = nodes[(sched.nextStartNodeIndex+i)%numAllNodes]
		}
		return feasibleNodes, nil
	}

	errCh := parallelize.NewResultChannel[error]()
	var feasibleNodesLen int32
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(errors.New("findNodesThatPassFilters has completed"))

	type nodeStatus struct {
		node   string
		status *fwk.Status
	}
	result := make([]*nodeStatus, numAllNodes)
	checkNode := func(i int) {
		// We check the nodes starting from where we left off in the previous scheduling cycle,
		// this is to make sure all nodes have the same chance of being examined across pods.
		nodeInfo := nodes[(sched.nextStartNodeIndex+i)%numAllNodes]
		status := schedFramework.RunFilterPluginsWithNominatedPods(ctx, state, pod, nodeInfo)
		if status.Code() == fwk.Error {
			errCh.SendWithCancel(status.AsError(), func() {
				cancel(errors.New("some other Filter operation failed"))
			})
			return
		}
		if status.IsSuccess() {
			length := atomic.AddInt32(&feasibleNodesLen, 1)
			if length > numNodesToFind {
				cancel(errors.New("findNodesThatPassFilters has found enough nodes"))
				atomic.AddInt32(&feasibleNodesLen, -1)
			} else {
				feasibleNodes[length-1] = nodeInfo
			}
		} else {
			result[i] = &nodeStatus{node: nodeInfo.Node().Name, status: status}
		}
	}

	beginCheckNode := time.Now()
	statusCode := fwk.Success
	defer func() {
		// We record Filter extension point latency here instead of in framework.go because framework.RunFilterPlugins
		// function is called for each node, whereas we want to have an overall latency for all nodes per scheduling cycle.
		// Note that this latency also includes latency for `addNominatedPods`, which calls framework.RunPreFilterAddPod.
		metrics.FrameworkExtensionPointDuration.WithLabelValues(metrics.Filter, statusCode.String(), schedFramework.ProfileName()).Observe(metrics.SinceInSeconds(beginCheckNode))
	}()

	// Stops searching for more nodes once the configured number of feasible nodes
	// are found.
	schedFramework.Parallelizer().Until(ctx, numAllNodes, checkNode, metrics.Filter)
	feasibleNodes = feasibleNodes[:feasibleNodesLen]
	for _, item := range result {
		if item == nil {
			continue
		}
		diagnosis.NodeToStatus.Set(item.node, item.status)
		diagnosis.AddPluginStatus(item.status)
	}
	if err := errCh.Receive(); err != nil {
		statusCode = fwk.Error
		return feasibleNodes, err
	}
	return feasibleNodes, nil
}
