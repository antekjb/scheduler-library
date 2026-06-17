// Copyright The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snapshot

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	upstreamsync "sigs.k8s.io/scheduler-library/pkg/upstream_sync"

	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
	fwk "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/profile"
)

// ClusterSnapshot wraps a scheduler snapshot and its associated frameworks.
// All ClusterSnapshot instances created from the same ClusterState share the
// same underlying cache.Snapshot. Creating a new snapshot via ClusterState.Snapshot
// updates that shared snapshot in-place, which invalidates any previously returned
// ClusterSnapshot instance — callers must not use a prior snapshot after requesting a new one.
type ClusterSnapshot struct {
	schedulerSnapshot *cache.Snapshot
	frameworks        profile.Map
	transactions      []string
	lastCommittedTx   string
	txCompensation    map[string][]func()
}

// NewClusterSnapshot creates a new ClusterSnapshot stub wrapping the provided scheduler snapshot and frameworks.
func NewClusterSnapshot(s *cache.Snapshot, frameworks profile.Map) *ClusterSnapshot {
	return &ClusterSnapshot{
		schedulerSnapshot: s,
		frameworks:        frameworks,
		txCompensation:    make(map[string][]func()),
	}
}

// Transaction executes the provided function within a transaction.
// It rolls back operations if the function returns Revert or an error.
func (s *ClusterSnapshot) Transaction(ctx context.Context, logger klog.Logger, transactionFn func() (TransactionResult, error)) error {
	txId := uuid.New().String()
	s.transactions = append(s.transactions, txId)
	s.txCompensation[txId] = []func(){}

	defer func() {
		delete(s.txCompensation, txId)
		s.transactions = s.transactions[:len(s.transactions)-1]
	}()

	result, err := transactionFn()

	if err != nil || result == Revert {
		operations := s.txCompensation[txId]
		for i := len(operations) - 1; i >= 0; i-- {
			operations[i]()
		}
	} else {
		s.lastCommittedTx = txId
	}

	if err != nil {
		return fmt.Errorf("transaction failed: %w", err)
	}
	return nil
}

// CanSchedulePod checks feasibility of a single pod on the specified nodes by running
// PreFilter and Filter plugins. Returns the names of nodes on which the pod can be scheduled.
func (s *ClusterSnapshot) CanSchedulePod(ctx context.Context, sched *upstreamsync.Scheduler, logger klog.Logger, pod SchedulablePod) ([]string, error) {
	framework, err := sched.FrameworkForPod(pod.Pod)
	if err != nil {
		return nil, fmt.Errorf("failed to get framework: %w", err)
	}
	state := fwk.NewCycleState()
	podInfo, err := fwk.NewPodInfo(pod.Pod)
	if err != nil {
		return nil, fmt.Errorf("failed to create pod info: %w", err)
	}

	nodes, _, err := sched.FindNodesThatFitPodSkippingExtenders(ctx, framework, state, &fwk.QueuedPodInfo{PodInfo: podInfo}, pod.CandidateNodeNames)
	if err != nil {
		return nil, fmt.Errorf("failed to find nodes that fit pod: %w", err)
	}

	feasibleNodes := make([]string, len(nodes))
	for i, nodeInfo := range nodes {
		feasibleNodes[i] = nodeInfo.Node().Name
	}

	return feasibleNodes, nil
}

func schedulingResult(algRes *upstreamsync.AlgorithmResult) SchedulingResult {
	return SchedulingResult{
		Pod:              algRes.Pod,
		Status:           algRes.Status,
		SelectedNodeName: algRes.ScheduleResult.SuggestedHost,
	}
}

// SchedulePods schedules the given pods onto their candidate nodes using PreFilter and Filter plugins.
// StopOnFailure controls whether the first unschedulable pod stops the loop. Note that
// node-not-found errors always propagate immediately regardless of StopOnFailure, as they
// indicate a programming error rather than a scheduling failure.
func (s *ClusterSnapshot) SchedulePods(ctx context.Context, sched *upstreamsync.Scheduler, logger klog.Logger, pods []SchedulablePod, opts SchedulePodsOptions) ([]SchedulingResult, error) {
	var currTx []func()
	if opts.DryRun {
		currTx = []func(){}
		defer func() {
			for i := len(currTx) - 1; i >= 0; i-- {
				currTx[i]()
			}
		}()
	}

	result := make([]SchedulingResult, 0)
	if len(pods) == 0 {
		return result, nil
	}

	for _, p := range pods {
		res, revertFn, err := sched.ScheduleOnePod(ctx, p.Pod, p.CandidateNodeNames)
		if err != nil {
			return result, err
		}

		if !res.Status.IsSuccess() {
			if opts.StopOnFailure {
				return result, fmt.Errorf("simulation failed: %w", res.Status.AsError())
			}
			result = append(result, schedulingResult(res))
			continue
		}

		result = append(result, schedulingResult(res))

		if opts.DryRun {
			currTx = append(currTx, revertFn)
		} else if len(s.transactions) > 0 {
			txId := s.transactions[len(s.transactions)-1]
			s.txCompensation[txId] = append(s.txCompensation[txId], revertFn)
		}
	}

	return result, nil
}

// SchedulePodsByTemplate attempts to schedule as many pods matching the template as possible.
// It assumes candidate nodes are feasible and moves to the next node only if the pod is unschedulable on the current node.
func (s *ClusterSnapshot) SchedulePodsByTemplate(ctx context.Context, sched *upstreamsync.Scheduler, logger klog.Logger, template *v1.PodTemplateSpec, candidateNodes []string, maxPods int, opts SchedulePodsByTemplateOptions) ([]SchedulingResult, error) {
	var currTx []func()
	if opts.DryRun {
		currTx = []func(){}
		defer func() {
			for i := len(currTx) - 1; i >= 0; i-- {
				currTx[i]()
			}
		}()
	}

	result := make([]SchedulingResult, 0)
	if maxPods <= 0 || len(candidateNodes) == 0 {
		return result, nil
	}

	nodeIdx := 0
	for i := range maxPods {

		pod := upstreamsync.CreatePodFromTemplate(template, i)
		scheduled := false

		for nodeIdx < len(candidateNodes) {
			res, revertFn, err := sched.ScheduleOnePod(ctx, pod, []string{candidateNodes[nodeIdx]})
			if err != nil {
				return result, err
			}

			if res.Status.IsSuccess() {
				result = append(result, schedulingResult(res))

				if opts.DryRun {
					currTx = append(currTx, revertFn)
				} else if len(s.transactions) > 0 {
					txId := s.transactions[len(s.transactions)-1]
					s.txCompensation[txId] = append(s.txCompensation[txId], revertFn)
				}

				scheduled = true
				break // Successfully scheduled, move to next pod using the same nodeIdx
			}

			// Unschedulable on this node, try next node
			nodeIdx++
		}

		if !scheduled {
			// No more nodes can fit this pod template, stop scheduling
			break
		}
	}

	return result, nil
}

// PreemptPods removes pods from the snapshot.
// It supports transaction rollbacks if called inside a transaction.
// If any pod fails to be preempted, all previously preempted pods in this call
// are automatically restored and an error is returned.
func (s *ClusterSnapshot) PreemptPods(ctx context.Context, sched *upstreamsync.Scheduler, pods []*v1.Pod) (*PreemptionSnapshot, error) {
	// Validate all pods before making any changes.
	for _, pod := range pods {
		if pod.Spec.NodeName == "" {
			return nil, fmt.Errorf("pod %s has no node name", klog.KObj(pod))
		}
	}
	var revertFns []func()

	var txId string
	insideTx := len(s.transactions) > 0
	if insideTx {
		txId = s.transactions[len(s.transactions)-1]
	}

	for _, pod := range pods {
		nodeName := pod.Spec.NodeName

		revertFn, err := upstreamsync.RemovePodFromNode(ctx, s.schedulerSnapshot, pod, nodeName)
		if err != nil {
			// Roll back all already-preempted pods.
			for i := len(revertFns) - 1; i >= 0; i-- {
				revertFns[i]()
			}
			// Remove the compensation callbacks added for the already-preempted pods,
			// since we just manually restored them. Without this, a later transaction
			// revert would try to re-add them again and double-add the pods.
			if insideTx {
				n := len(revertFns)
				comps := s.txCompensation[txId]
				s.txCompensation[txId] = comps[:len(comps)-n]
			}
			return nil, fmt.Errorf("failed to unreserve and forget pod %s: %w", klog.KObj(pod), err)
		}

		revertFns = append(revertFns, revertFn)

		if insideTx {
			s.txCompensation[txId] = append(s.txCompensation[txId], revertFn)
		}
	}

	return newPreemptionSnapshot(s, revertFns), nil
}

type PreemptionSnapshot struct {
	snapshot           *ClusterSnapshot
	revertFns          []func()
	currentTx          string
	currentTxMutations int
	lastCommittedTx    string
}

func newPreemptionSnapshot(s *ClusterSnapshot, revertFns []func()) *PreemptionSnapshot {
	var currentTx string
	if len(s.transactions) > 0 {
		currentTx = s.transactions[len(s.transactions)-1]
	}
	var currentTxMutations int
	if currentTx != "" {
		currentTxMutations = len(s.txCompensation[currentTx])
	}

	return &PreemptionSnapshot{
		snapshot:           s,
		revertFns:          revertFns,
		currentTx:          currentTx,
		currentTxMutations: currentTxMutations,
		lastCommittedTx:    s.lastCommittedTx,
	}
}

// Unpreempt undos the preemption done by the PreemptPods.
func (ps *PreemptionSnapshot) Unpreempt() error {
	newTxWasCommitedAfterPreemption := ps.lastCommittedTx != ps.snapshot.lastCommittedTx

	var currentSnapshotTx string
	if len(ps.snapshot.transactions) > 0 {
		currentSnapshotTx = ps.snapshot.transactions[len(ps.snapshot.transactions)-1]
	}
	newTxStarted := ps.currentTx != currentSnapshotTx

	newMutationForCurrentTx := !newTxStarted && ps.currentTxMutations != len(ps.snapshot.txCompensation[ps.currentTx])

	if newTxWasCommitedAfterPreemption || newTxStarted || newMutationForCurrentTx {
		return fmt.Errorf("snapshot was mutated after preemption")
	}

	for i := len(ps.revertFns) - 1; i >= 0; i-- {
		ps.revertFns[i]()
	}

	// Non transaction scope, nothing to revert
	if ps.currentTx == "" {
		return nil
	}

	txId := ps.currentTx
	// Number of pods being manually re-added right now
	numPods := len(ps.revertFns)

	compensationFuncs := ps.snapshot.txCompensation[txId]
	if len(compensationFuncs) < numPods {
		return fmt.Errorf("unexpected number of mutations in transaction compensation list")
	}

	ps.snapshot.txCompensation[txId] = compensationFuncs[:len(compensationFuncs)-numPods]

	return nil
}
