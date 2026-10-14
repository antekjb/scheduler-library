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

// Package preemption provides workload preemption types and simulation adapters
// that bridge the library's PreemptionVictim and PreemptionFilter interfaces to
// the scheduler framework's PreemptionManager and ReprieveFilter interfaces.
package preemption

import (
	"context"
	"fmt"

	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	upstreampreemption "sigs.k8s.io/scheduler-library/pkg/upstreamsync/preemption"
)

// EvaluateWorkloadPreemption runs prefix search (stage 1b) and reprieval (stage 2)
// over opts.PotentialVictims using the framework's PodGroupPostFilter plugins.
// It returns the last successful PodGroupAssignments and the non-reprieved victims
// on success, an Unschedulable status if all potential victims are insufficient,
// or an Error status if victim construction or post-filter evaluation fails.
func EvaluateWorkloadPreemption(
	ctx context.Context,
	schedFramework *upstreampreemption.PreemptionFramework,
	podGroupCycleState *framework.CycleState,
	podGroupInfo fwk.PodGroupInfo,
	baseSchedulingFunc fwk.PodGroupSchedulingFunc,
	opts WorkloadPreemptionOptions,
) (*fwk.PodGroupAssignments, []PreemptionVictim, *fwk.Status) {
	if opts.PotentialVictims == nil {
		return nil, nil, fwk.NewStatus(fwk.Unschedulable)
	}

	pm := newPrefixPreemptionManager(opts.PreemptionFilter)
	schedFramework.SetPreemptionManager(pm)
	defer schedFramework.SetPreemptionManager(&upstreampreemption.NoopPreemptionManager{})

	var lastAssignments *fwk.PodGroupAssignments
	pgSchedulingFunc := func(ctx context.Context) (*fwk.PodGroupAssignments, *fwk.Status) {
		assignments, status := baseSchedulingFunc(ctx)
		if status.IsSuccess() {
			lastAssignments = assignments
		}
		return assignments, status
	}

	for v := range opts.PotentialVictims {
		if v == nil || len(v.Pods()) == 0 {
			continue
		}
		sv, err := newSimPreemptionVictim(v)
		if err != nil {
			return nil, nil, fwk.AsStatus(err)
		}
		pm.addVictim(sv)

		if opts.PreemptionFilter != nil {
			opts.PreemptionFilter.PreemptVictim(v)
			if !opts.PreemptionFilter.MayFit() {
				continue
			}
		}

		_, postFilterStatus := schedFramework.RunPodGroupPostFilterPlugins(ctx, podGroupCycleState, podGroupInfo, pgSchedulingFunc)
		if postFilterStatus.IsError() {
			return nil, nil, postFilterStatus
		}
		if postFilterStatus.IsSuccess() && pm.actuated {
			return lastAssignments, pm.preemptedVictims(), fwk.NewStatus(fwk.Success)
		}
	}

	return nil, nil, fwk.NewStatus(fwk.Unschedulable)
}

// simPreemptionVictim adapts a PreemptionVictim to the framework's PreemptionVictim interface.
type simPreemptionVictim struct {
	victim    PreemptionVictim
	pods      []fwk.PodInfo
	reprieved bool
}

var _ upstreampreemption.PreemptionVictim = (*simPreemptionVictim)(nil)

func newSimPreemptionVictim(v PreemptionVictim) (*simPreemptionVictim, error) {
	if v == nil {
		return nil, fmt.Errorf("preemption victim cannot be nil")
	}
	rawPods := v.Pods()
	podInfos := make([]fwk.PodInfo, 0, len(rawPods))
	for _, p := range rawPods {
		if p == nil {
			return nil, fmt.Errorf("preemption victim pod cannot be nil")
		}
		pi, err := framework.NewPodInfo(p)
		if err != nil {
			return nil, fmt.Errorf("failed to create PodInfo for victim pod: %w", err)
		}
		podInfos = append(podInfos, pi)
	}
	return &simPreemptionVictim{
		victim: v,
		pods:   podInfos,
	}, nil
}

func (v *simPreemptionVictim) Pods() []fwk.PodInfo {
	return v.pods
}

func (v *simPreemptionVictim) NumPDBViolations() int {
	return 0
}

// prefixPreemptionManager provides candidate victims and creates a ReprieveFilter
// for prefix-search and reprieval preemption simulations.
type prefixPreemptionManager struct {
	upstreampreemption.NoopPreemptionManager
	victims          []*simPreemptionVictim
	preemptionFilter PreemptionFilter
	actuated         bool
}

var _ upstreampreemption.PreemptionManager = (*prefixPreemptionManager)(nil)
var _ upstreampreemption.PreemptionExecutor = (*prefixPreemptionManager)(nil)

func newPrefixPreemptionManager(filter PreemptionFilter) *prefixPreemptionManager {
	return &prefixPreemptionManager{
		preemptionFilter: filter,
	}
}

func (pm *prefixPreemptionManager) addVictim(sv *simPreemptionVictim) {
	pm.victims = append(pm.victims, sv)
}

func (pm *prefixPreemptionManager) Executor() upstreampreemption.PreemptionExecutor {
	return pm
}

func (pm *prefixPreemptionManager) ActuatePodGroupPreemption(_ context.Context, _ upstreampreemption.PreemptionCandidate, _ fwk.PodGroupInfo, _ string) *fwk.Status {
	pm.actuated = true
	return nil
}

func (pm *prefixPreemptionManager) GenerateVictims(
	_ context.Context,
	_ fwk.PodGroupInfo,
) ([]upstreampreemption.PreemptionVictim, *fwk.Status) {
	res := make([]upstreampreemption.PreemptionVictim, len(pm.victims))
	for i, sv := range pm.victims {
		res[i] = sv
	}
	return res, fwk.NewStatus(fwk.Success)
}

func (pm *prefixPreemptionManager) NewReprieveFilter(
	_ context.Context,
	_ []upstreampreemption.PreemptionVictim,
) upstreampreemption.ReprieveFilter {
	return &workloadReprieveFilter{filter: pm.preemptionFilter}
}

func (pm *prefixPreemptionManager) preemptedVictims() []PreemptionVictim {
	var res []PreemptionVictim
	for _, sv := range pm.victims {
		if !sv.reprieved {
			res = append(res, sv.victim)
		}
	}
	return res
}

// workloadReprieveFilter evaluates and tracks victim reprieval using PreemptionFilter.
type workloadReprieveFilter struct {
	filter PreemptionFilter
}

var _ upstreampreemption.ReprieveFilter = (*workloadReprieveFilter)(nil)

func (rf *workloadReprieveFilter) ShouldAttemptReprieval(
	_ context.Context,
	victim upstreampreemption.PreemptionVictim,
) (bool, error) {
	if rf.filter == nil {
		return true, nil
	}
	sv, ok := victim.(*simPreemptionVictim)
	if !ok {
		return false, fmt.Errorf("unexpected PreemptionVictim type %T", victim)
	}
	rf.filter.UnpreemptVictim(sv.victim)
	mayFit := rf.filter.MayFit()
	rf.filter.PreemptVictim(sv.victim)
	return mayFit, nil
}

func (rf *workloadReprieveFilter) OnVictimReprieved(
	_ context.Context,
	victim upstreampreemption.PreemptionVictim,
) error {
	sv, ok := victim.(*simPreemptionVictim)
	if !ok {
		return fmt.Errorf("unexpected PreemptionVictim type %T", victim)
	}
	sv.reprieved = true
	if rf.filter != nil {
		rf.filter.UnpreemptVictim(sv.victim)
	}
	return nil
}
