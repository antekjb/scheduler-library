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

package preemption

import (
	"context"
	"errors"
	"slices"
	"testing"

	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	upstreampreemption "sigs.k8s.io/scheduler-library/pkg/upstreamsync/preemption"
)

type testVictim struct {
	pods []*v1.Pod
}

func (v *testVictim) Pods() []*v1.Pod {
	return v.pods
}

type fakeFilter struct {
	fit             bool
	minVictims      int
	preemptCalls    int
	unpreemptCalls  int
	activeVictimSet map[PreemptionVictim]bool
}

func newFakeFilter(fit bool) *fakeFilter {
	return &fakeFilter{
		fit:             fit,
		activeVictimSet: make(map[PreemptionVictim]bool),
	}
}

func (f *fakeFilter) PreemptVictim(v PreemptionVictim) {
	f.preemptCalls++
	f.activeVictimSet[v] = true
}

func (f *fakeFilter) UnpreemptVictim(v PreemptionVictim) {
	f.unpreemptCalls++
	delete(f.activeVictimSet, v)
}

func (f *fakeFilter) MayFit() bool {
	if f.minVictims > 0 {
		return len(f.activeVictimSet) >= f.minVictims
	}
	return f.fit
}

type mockFramework struct {
	framework.Framework
	postFilterCalls int
	postFilterFn    func(ctx context.Context, state *framework.CycleState, podGroupInfo fwk.PodGroupInfo, pgSchedulingFunc fwk.PodGroupSchedulingFunc) (*fwk.PodGroupPostFilterResult, *fwk.Status)
}

func (m *mockFramework) RunPodGroupPostFilterPlugins(
	ctx context.Context,
	state *framework.CycleState,
	podGroupInfo fwk.PodGroupInfo,
	pgSchedulingFunc fwk.PodGroupSchedulingFunc,
) (*fwk.PodGroupPostFilterResult, *fwk.Status) {
	m.postFilterCalls++
	if m.postFilterFn != nil {
		return m.postFilterFn(ctx, state, podGroupInfo, pgSchedulingFunc)
	}
	return nil, fwk.NewStatus(fwk.Unschedulable)
}

func TestPrefixPreemptionManager(t *testing.T) {
	ctx := context.Background()
	pod1 := st.MakePod().Namespace("default").Name("p1").Node("n1").Obj()
	pod2 := st.MakePod().Namespace("default").Name("p2").Node("n1").Obj()

	v1Victim := &testVictim{pods: []*v1.Pod{pod1}}
	v2Victim := &testVictim{pods: []*v1.Pod{pod2}}

	t.Run("newSimPreemptionVictim validation", func(t *testing.T) {
		if _, err := newSimPreemptionVictim(nil); err == nil {
			t.Errorf("expected error for nil victim, got nil")
		}
		if _, err := newSimPreemptionVictim(&testVictim{pods: []*v1.Pod{nil}}); err == nil {
			t.Errorf("expected error for nil pod in victim, got nil")
		}
	})

	t.Run("Prefix search and reprieval tracking", func(t *testing.T) {
		filter := newFakeFilter(true)
		pm := newPrefixPreemptionManager(filter)

		sv1, err := newSimPreemptionVictim(v1Victim)
		if err != nil {
			t.Fatalf("unexpected error creating sv1: %v", err)
		}
		sv2, err := newSimPreemptionVictim(v2Victim)
		if err != nil {
			t.Fatalf("unexpected error creating sv2: %v", err)
		}
		pm.addVictim(sv1)
		pm.addVictim(sv2)
		filter.PreemptVictim(v1Victim)
		filter.PreemptVictim(v2Victim)

		victims, status := pm.GenerateVictims(ctx, nil)
		if !status.IsSuccess() {
			t.Fatalf("expected GenerateVictims success, got %v", status)
		}
		if len(victims) != 2 {
			t.Fatalf("expected 2 victims, got %d", len(victims))
		}

		rf := pm.NewReprieveFilter(ctx, victims)
		shouldAttempt, err := rf.ShouldAttemptReprieval(ctx, victims[0])
		if err != nil {
			t.Fatalf("ShouldAttemptReprieval returned error: %v", err)
		}
		if !shouldAttempt {
			t.Errorf("expected ShouldAttemptReprieval to return true")
		}
		// ShouldAttemptReprieval temporarily unpreempts to check MayFit, then re-preempts.
		if !filter.activeVictimSet[v1Victim] {
			t.Errorf("expected v1Victim to remain preempted after ShouldAttemptReprieval")
		}

		// Reprieve v1Victim.
		if err := rf.OnVictimReprieved(ctx, victims[0]); err != nil {
			t.Fatalf("OnVictimReprieved returned error: %v", err)
		}
		if filter.activeVictimSet[v1Victim] {
			t.Errorf("expected v1Victim to be unpreempted in filter after OnVictimReprieved")
		}

		if pm.actuated {
			t.Errorf("expected actuated to be false before ActuatePodGroupPreemption")
		}
		if status := pm.Executor().ActuatePodGroupPreemption(ctx, nil, nil, ""); !status.IsSuccess() {
			t.Fatalf("expected ActuatePodGroupPreemption success, got %v", status)
		}
		if !pm.actuated {
			t.Errorf("expected actuated to be true after ActuatePodGroupPreemption")
		}

		preempted := pm.preemptedVictims()
		if len(preempted) != 1 || preempted[0] != v2Victim {
			t.Errorf("expected only v2Victim in preemptedVictims(), got %v", preempted)
		}
	})
}

func TestEvaluateWorkloadPreemption(t *testing.T) {
	ctx := context.Background()
	pod1 := st.MakePod().Namespace("default").Name("p1").Node("n1").Obj()
	pod2 := st.MakePod().Namespace("default").Name("p2").Node("n1").Obj()
	pod3 := st.MakePod().Namespace("default").Name("p3").Node("n1").Obj()

	v1Victim := &testVictim{pods: []*v1.Pod{pod1}}
	v2Victim := &testVictim{pods: []*v1.Pod{pod2}}
	v3Victim := &testVictim{pods: []*v1.Pod{pod3}}

	assignments1 := &fwk.PodGroupAssignments{}
	assignments2 := &fwk.PodGroupAssignments{}

	t.Run("nil PotentialVictims returns Unschedulable", func(t *testing.T) {
		mf := &mockFramework{}
		pf := upstreampreemption.NewPreemptionFramework(mf, nil)

		gotAssignments, gotVictims, gotStatus := EvaluateWorkloadPreemption(
			ctx, pf, framework.NewCycleState(), nil, nil, WorkloadPreemptionOptions{},
		)
		if gotStatus.Code() != fwk.Unschedulable {
			t.Errorf("expected Unschedulable, got %v", gotStatus)
		}
		if gotAssignments != nil || gotVictims != nil {
			t.Errorf("expected nil assignments and victims, got %v, %v", gotAssignments, gotVictims)
		}
		if mf.postFilterCalls != 0 {
			t.Errorf("expected 0 RunPodGroupPostFilterPlugins calls, got %d", mf.postFilterCalls)
		}
	})

	t.Run("nil and empty-pod victims are skipped", func(t *testing.T) {
		mf := &mockFramework{}
		pf := upstreampreemption.NewPreemptionFramework(mf, nil)
		filter := newFakeFilter(true)

		gotAssignments, gotVictims, gotStatus := EvaluateWorkloadPreemption(
			ctx, pf, framework.NewCycleState(), nil, nil,
			WorkloadPreemptionOptions{
				PotentialVictims: slices.Values([]PreemptionVictim{
					nil,
					&testVictim{pods: nil},
					&testVictim{pods: []*v1.Pod{}},
				}),
				PreemptionFilter: filter,
			},
		)
		if gotStatus.Code() != fwk.Unschedulable {
			t.Errorf("expected Unschedulable, got %v", gotStatus)
		}
		if gotAssignments != nil || gotVictims != nil {
			t.Errorf("expected nil assignments and victims, got %v, %v", gotAssignments, gotVictims)
		}
		if mf.postFilterCalls != 0 || filter.preemptCalls != 0 {
			t.Errorf("expected 0 postFilter and preempt calls, got %d, %d", mf.postFilterCalls, filter.preemptCalls)
		}
	})

	t.Run("victim with nil pod returns Error status", func(t *testing.T) {
		mf := &mockFramework{}
		pf := upstreampreemption.NewPreemptionFramework(mf, nil)

		gotAssignments, gotVictims, gotStatus := EvaluateWorkloadPreemption(
			ctx, pf, framework.NewCycleState(), nil, nil,
			WorkloadPreemptionOptions{
				PotentialVictims: slices.Values([]PreemptionVictim{
					&testVictim{pods: []*v1.Pod{nil}},
				}),
			},
		)
		if !gotStatus.IsError() {
			t.Errorf("expected Error status, got %v", gotStatus)
		}
		if gotAssignments != nil || gotVictims != nil {
			t.Errorf("expected nil assignments and victims, got %v, %v", gotAssignments, gotVictims)
		}
		if mf.postFilterCalls != 0 {
			t.Errorf("expected 0 RunPodGroupPostFilterPlugins calls, got %d", mf.postFilterCalls)
		}
	})

	t.Run("prefix search stops at first fitting prefix and resets PreemptionManager", func(t *testing.T) {
		var pf *upstreampreemption.PreemptionFramework
		mf := &mockFramework{
			postFilterFn: func(ctx context.Context, _ *framework.CycleState, pgInfo fwk.PodGroupInfo, pgSchedulingFunc fwk.PodGroupSchedulingFunc) (*fwk.PodGroupPostFilterResult, *fwk.Status) {
				pm := pf.PreemptionManager()
				victims, status := pm.GenerateVictims(ctx, pgInfo)
				if !status.IsSuccess() {
					return nil, status
				}
				if len(victims) < 2 {
					return nil, fwk.NewStatus(fwk.Unschedulable)
				}
				if _, schedStatus := pgSchedulingFunc(ctx); !schedStatus.IsSuccess() {
					return nil, schedStatus
				}
				pm.Executor().ActuatePodGroupPreemption(ctx, nil, pgInfo, "")
				return &fwk.PodGroupPostFilterResult{}, fwk.NewStatus(fwk.Success)
			},
		}
		pf = upstreampreemption.NewPreemptionFramework(mf, nil)

		yieldedCount := 0
		victimsSeq := func(yield func(PreemptionVictim) bool) {
			for _, v := range []PreemptionVictim{v1Victim, v2Victim, v3Victim} {
				yieldedCount++
				if !yield(v) {
					return
				}
			}
		}

		baseSchedFn := func(_ context.Context) (*fwk.PodGroupAssignments, *fwk.Status) {
			return assignments1, fwk.NewStatus(fwk.Success)
		}

		gotAssignments, gotVictims, gotStatus := EvaluateWorkloadPreemption(
			ctx, pf, framework.NewCycleState(), nil, baseSchedFn,
			WorkloadPreemptionOptions{
				PotentialVictims: victimsSeq,
			},
		)
		if !gotStatus.IsSuccess() {
			t.Fatalf("expected Success, got %v", gotStatus)
		}
		if gotAssignments != assignments1 {
			t.Errorf("expected assignments1, got %v", gotAssignments)
		}
		if !slices.Equal(gotVictims, []PreemptionVictim{v1Victim, v2Victim}) {
			t.Errorf("expected [v1Victim, v2Victim], got %v", gotVictims)
		}
		if mf.postFilterCalls != 2 {
			t.Errorf("expected 2 RunPodGroupPostFilterPlugins calls, got %d", mf.postFilterCalls)
		}
		if yieldedCount != 2 {
			t.Errorf("expected iterator to yield 2 victims before stopping, got %d", yieldedCount)
		}
		if _, ok := pf.PreemptionManager().(*upstreampreemption.NoopPreemptionManager); !ok {
			t.Errorf("expected PreemptionManager to be reset to NoopPreemptionManager, got %T", pf.PreemptionManager())
		}
	})

	t.Run("PreemptionFilter skips RunPodGroupPostFilterPlugins until MayFit is true", func(t *testing.T) {
		var pf *upstreampreemption.PreemptionFramework
		mf := &mockFramework{
			postFilterFn: func(ctx context.Context, _ *framework.CycleState, pgInfo fwk.PodGroupInfo, pgSchedulingFunc fwk.PodGroupSchedulingFunc) (*fwk.PodGroupPostFilterResult, *fwk.Status) {
				pm := pf.PreemptionManager()
				victims, _ := pm.GenerateVictims(ctx, pgInfo)
				if len(victims) != 2 {
					t.Errorf("expected 2 accumulated victims when PostFilter runs, got %d", len(victims))
				}
				if _, schedStatus := pgSchedulingFunc(ctx); !schedStatus.IsSuccess() {
					return nil, schedStatus
				}
				pm.Executor().ActuatePodGroupPreemption(ctx, nil, pgInfo, "")
				return &fwk.PodGroupPostFilterResult{}, fwk.NewStatus(fwk.Success)
			},
		}
		pf = upstreampreemption.NewPreemptionFramework(mf, nil)

		filter := newFakeFilter(false)
		filter.minVictims = 2

		baseSchedFn := func(_ context.Context) (*fwk.PodGroupAssignments, *fwk.Status) {
			return assignments1, fwk.NewStatus(fwk.Success)
		}

		gotAssignments, gotVictims, gotStatus := EvaluateWorkloadPreemption(
			ctx, pf, framework.NewCycleState(), nil, baseSchedFn,
			WorkloadPreemptionOptions{
				PotentialVictims: slices.Values([]PreemptionVictim{v1Victim, v2Victim, v3Victim}),
				PreemptionFilter: filter,
			},
		)
		if !gotStatus.IsSuccess() {
			t.Fatalf("expected Success, got %v", gotStatus)
		}
		if gotAssignments != assignments1 {
			t.Errorf("expected assignments1, got %v", gotAssignments)
		}
		if !slices.Equal(gotVictims, []PreemptionVictim{v1Victim, v2Victim}) {
			t.Errorf("expected [v1Victim, v2Victim], got %v", gotVictims)
		}
		if filter.preemptCalls != 2 {
			t.Errorf("expected 2 PreemptVictim calls, got %d", filter.preemptCalls)
		}
		if mf.postFilterCalls != 1 {
			t.Errorf("expected 1 RunPodGroupPostFilterPlugins call (skipped on v1), got %d", mf.postFilterCalls)
		}
	})

	t.Run("reprieval filters out reprieved victims and preserves last successful assignments", func(t *testing.T) {
		var pf *upstreampreemption.PreemptionFramework
		mf := &mockFramework{
			postFilterFn: func(ctx context.Context, _ *framework.CycleState, pgInfo fwk.PodGroupInfo, pgSchedulingFunc fwk.PodGroupSchedulingFunc) (*fwk.PodGroupPostFilterResult, *fwk.Status) {
				pm := pf.PreemptionManager()
				victims, _ := pm.GenerateVictims(ctx, pgInfo)
				if len(victims) < 2 {
					return nil, fwk.NewStatus(fwk.Unschedulable)
				}
				// Initial fit check with all victims removed succeeds (assignments1).
				if _, schedStatus := pgSchedulingFunc(ctx); !schedStatus.IsSuccess() {
					return nil, schedStatus
				}
				rf := pm.NewReprieveFilter(ctx, victims)
				// Reprieving victims[1] (v2Victim) fails scheduling check (returns nil, Unschedulable).
				if ok, err := rf.ShouldAttemptReprieval(ctx, victims[1]); err != nil || !ok {
					t.Fatalf("expected ShouldAttemptReprieval for victims[1] to succeed, got %v, %v", ok, err)
				}
				if _, schedStatus := pgSchedulingFunc(ctx); schedStatus.IsSuccess() {
					t.Fatalf("expected second scheduling call to fail")
				}
				// Reprieving victims[0] (v1Victim) succeeds scheduling check (assignments2).
				if ok, err := rf.ShouldAttemptReprieval(ctx, victims[0]); err != nil || !ok {
					t.Fatalf("expected ShouldAttemptReprieval for victims[0] to succeed, got %v, %v", ok, err)
				}
				if _, schedStatus := pgSchedulingFunc(ctx); !schedStatus.IsSuccess() {
					t.Fatalf("expected third scheduling call to succeed, got %v", schedStatus)
				}
				if err := rf.OnVictimReprieved(ctx, victims[0]); err != nil {
					t.Fatalf("unexpected error in OnVictimReprieved: %v", err)
				}

				pm.Executor().ActuatePodGroupPreemption(ctx, nil, pgInfo, "")
				return &fwk.PodGroupPostFilterResult{}, fwk.NewStatus(fwk.Success)
			},
		}
		pf = upstreampreemption.NewPreemptionFramework(mf, nil)

		schedCalls := 0
		baseSchedFn := func(_ context.Context) (*fwk.PodGroupAssignments, *fwk.Status) {
			schedCalls++
			switch schedCalls {
			case 1:
				return assignments1, fwk.NewStatus(fwk.Success)
			case 2:
				return nil, fwk.NewStatus(fwk.Unschedulable)
			case 3:
				return assignments2, fwk.NewStatus(fwk.Success)
			default:
				return nil, fwk.NewStatus(fwk.Unschedulable)
			}
		}

		filter := newFakeFilter(true)
		gotAssignments, gotVictims, gotStatus := EvaluateWorkloadPreemption(
			ctx, pf, framework.NewCycleState(), nil, baseSchedFn,
			WorkloadPreemptionOptions{
				PotentialVictims: slices.Values([]PreemptionVictim{v1Victim, v2Victim}),
				PreemptionFilter: filter,
			},
		)
		if !gotStatus.IsSuccess() {
			t.Fatalf("expected Success, got %v", gotStatus)
		}
		if gotAssignments != assignments2 {
			t.Errorf("expected last successful assignments (assignments2), got %v", gotAssignments)
		}
		if !slices.Equal(gotVictims, []PreemptionVictim{v2Victim}) {
			t.Errorf("expected only non-reprieved [v2Victim], got %v", gotVictims)
		}
		if filter.activeVictimSet[v1Victim] || !filter.activeVictimSet[v2Victim] {
			t.Errorf("unexpected activeVictimSet in filter: %v", filter.activeVictimSet)
		}
	})

	t.Run("RunPodGroupPostFilterPlugins error aborts immediately", func(t *testing.T) {
		expectedErr := errors.New("postfilter plugin error")
		mf := &mockFramework{
			postFilterFn: func(_ context.Context, _ *framework.CycleState, _ fwk.PodGroupInfo, _ fwk.PodGroupSchedulingFunc) (*fwk.PodGroupPostFilterResult, *fwk.Status) {
				return nil, fwk.AsStatus(expectedErr)
			},
		}
		pf := upstreampreemption.NewPreemptionFramework(mf, nil)

		gotAssignments, gotVictims, gotStatus := EvaluateWorkloadPreemption(
			ctx, pf, framework.NewCycleState(), nil, nil,
			WorkloadPreemptionOptions{
				PotentialVictims: slices.Values([]PreemptionVictim{v1Victim, v2Victim}),
			},
		)
		if !gotStatus.IsError() || !errors.Is(gotStatus.AsError(), expectedErr) {
			t.Errorf("expected error status %v, got %v", expectedErr, gotStatus)
		}
		if gotAssignments != nil || gotVictims != nil {
			t.Errorf("expected nil assignments and victims, got %v, %v", gotAssignments, gotVictims)
		}
		if mf.postFilterCalls != 1 {
			t.Errorf("expected 1 RunPodGroupPostFilterPlugins call before aborting, got %d", mf.postFilterCalls)
		}
	})

	t.Run("RunPodGroupPostFilterPlugins success without actuation continues and returns Unschedulable", func(t *testing.T) {
		mf := &mockFramework{
			postFilterFn: func(_ context.Context, _ *framework.CycleState, _ fwk.PodGroupInfo, _ fwk.PodGroupSchedulingFunc) (*fwk.PodGroupPostFilterResult, *fwk.Status) {
				return &fwk.PodGroupPostFilterResult{}, fwk.NewStatus(fwk.Success)
			},
		}
		pf := upstreampreemption.NewPreemptionFramework(mf, nil)

		gotAssignments, gotVictims, gotStatus := EvaluateWorkloadPreemption(
			ctx, pf, framework.NewCycleState(), nil, nil,
			WorkloadPreemptionOptions{
				PotentialVictims: slices.Values([]PreemptionVictim{v1Victim, v2Victim}),
			},
		)
		if gotStatus.Code() != fwk.Unschedulable {
			t.Errorf("expected Unschedulable when preemption is not actuated, got %v", gotStatus)
		}
		if gotAssignments != nil || gotVictims != nil {
			t.Errorf("expected nil assignments and victims, got %v, %v", gotAssignments, gotVictims)
		}
		if mf.postFilterCalls != 2 {
			t.Errorf("expected 2 RunPodGroupPostFilterPlugins calls, got %d", mf.postFilterCalls)
		}
	})

	t.Run("all potential victims insufficient returns Unschedulable", func(t *testing.T) {
		mf := &mockFramework{
			postFilterFn: func(_ context.Context, _ *framework.CycleState, _ fwk.PodGroupInfo, _ fwk.PodGroupSchedulingFunc) (*fwk.PodGroupPostFilterResult, *fwk.Status) {
				return nil, fwk.NewStatus(fwk.Unschedulable)
			},
		}
		pf := upstreampreemption.NewPreemptionFramework(mf, nil)

		gotAssignments, gotVictims, gotStatus := EvaluateWorkloadPreemption(
			ctx, pf, framework.NewCycleState(), nil, nil,
			WorkloadPreemptionOptions{
				PotentialVictims: slices.Values([]PreemptionVictim{v1Victim, v2Victim}),
			},
		)
		if gotStatus.Code() != fwk.Unschedulable {
			t.Errorf("expected Unschedulable, got %v", gotStatus)
		}
		if gotAssignments != nil || gotVictims != nil {
			t.Errorf("expected nil assignments and victims, got %v, %v", gotAssignments, gotVictims)
		}
		if mf.postFilterCalls != 2 {
			t.Errorf("expected 2 RunPodGroupPostFilterPlugins calls, got %d", mf.postFilterCalls)
		}
	})
}
