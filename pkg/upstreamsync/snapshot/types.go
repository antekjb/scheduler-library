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
	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/util"

	"time"

	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// CommonSchedulingOptions contains options shared across different scheduling simulation methods.
type CommonSchedulingOptions struct {
	// DryRun determines if the scheduling attempt should be a dry run.
	// When true, the simulation only tests feasibility and returns the results
	// without updating the cluster snapshot state (any state updates are automatically restored).
	DryRun bool
}

// SchedulePodsOptions are the options of ClusterSnapshot.SchedulePods.
type SchedulePodsOptions struct {
	CommonSchedulingOptions
	// StopOnFailure determines whether the first pod that cannot be scheduled ends the whole
	// call. When false, the remaining pods are still attempted and the returned results hold one
	// entry per attempted pod. Unexpected execution errors always end the call regardless of
	// this option, as they indicate a programming error rather than a scheduling failure.
	StopOnFailure bool
}

// SchedulePodsByTemplateOptions are the options of ClusterSnapshot.SchedulePodsByTemplate.
// The method always stops at the first pod that does not fit, as the pods it schedules are
// identical and the next one would not fit either.
type SchedulePodsByTemplateOptions struct {
	CommonSchedulingOptions
}

// NewSchedulePodsOptions builds the SchedulePodsOptions out of its individual fields.
func NewSchedulePodsOptions(dryRun bool, stopOnFailure bool) SchedulePodsOptions {
	return SchedulePodsOptions{
		CommonSchedulingOptions: CommonSchedulingOptions{DryRun: dryRun},
		StopOnFailure:           stopOnFailure,
	}
}

// NewSchedulePodsByTemplateOptions builds the SchedulePodsByTemplateOptions out of its individual fields.
func NewSchedulePodsByTemplateOptions(dryRun bool) SchedulePodsByTemplateOptions {
	return SchedulePodsByTemplateOptions{
		CommonSchedulingOptions: CommonSchedulingOptions{DryRun: dryRun},
	}
}

// TransactionResult is what the function passed to ClusterSnapshot.Transaction returns to decide
// the fate of the mutations it made.
type TransactionResult int

const (
	// Commit keeps the mutations made within the transaction.
	Commit TransactionResult = iota
	// Revert undoes all the mutations made within the transaction.
	Revert
)

// SchedulingResult is the outcome of a single pod scheduling attempt.
type SchedulingResult struct {
	// Pod is the pod the attempt was made for, carrying the selected node when it was scheduled.
	// For the pods passed to SchedulePods it is the library's own copy; for the pods created from a
	// template it is the generated pod, which is the only way for the caller to learn what was
	// scheduled.
	// On a failed attempt Spec.NodeName is left as it came in, so it is empty unless the caller, or
	// the template, already set one.
	Pod *v1.Pod
	// Status is the outcome of the scheduling cycle: success, or the reason the pod was rejected.
	Status *fwk.Status
	// SelectedNodeName is the node the pod was scheduled on, empty if it was not scheduled.
	SelectedNodeName string
	// CycleState is the state of the scheduling cycle.
	CycleState fwk.CycleState
}

// Unpreemption is the handle returned by ClusterSnapshot.PreemptPods, allowing the preempted pods
// to be put back with ClusterSnapshot.Unpreempt. It is single-use and is tied to the state of the
// snapshot it was taken from: any permanent mutation of that snapshot invalidates it, since the
// pods it holds could no longer be restored to the state they were removed from.
type Unpreemption struct {
	// pods are the pods that were preempted, returned to the caller by Unpreempt.
	pods []*v1.Pod
	// revertFn puts the pods back into the snapshot, registering the undo of each addition.
	revertFn func() error
	// reverted marks the handle as consumed, so that it cannot be applied twice.
	reverted bool
	// validPreemptionVersion is the snapshot's preemption state version at the time of the
	// preemption. Unpreempt refuses to run once the snapshot has moved past it.
	validPreemptionVersion uint64
}

// ScheduleWorkloadOptions contains options for scheduling a workload.
type ScheduleWorkloadOptions struct {
	CommonSchedulingOptions
}

// NewScheduleWorkloadOptions builds the ScheduleWorkloadOptions used by ScheduleWorkload.
func NewScheduleWorkloadOptions(dryRun bool) ScheduleWorkloadOptions {
	return ScheduleWorkloadOptions{
		CommonSchedulingOptions: CommonSchedulingOptions{DryRun: dryRun},
	}
}

// GenericPodGroup is a wrapper around either a PodGroup or a CompositePodGroup API object,
// providing a unified interface for operations on PodGroup objects.
type GenericPodGroup struct {
	// PodGroup is a PodGroup API object.
	PodGroup *schedulingv1beta1.PodGroup
	// CompositePodGroup is a CompositePodGroup API object.
	// It can be set only when CompositePodGroup feature is enabled.
	CompositePodGroup *schedulingv1alpha3.CompositePodGroup
}

// NewGenericPodGroup returns a GenericPodGroup for a PodGroup.
func NewGenericPodGroup(pg *schedulingv1beta1.PodGroup) *GenericPodGroup {
	return &GenericPodGroup{PodGroup: pg}
}

// NewGenericCompositePodGroup returns a GenericPodGroup for a CompositePodGroup.
func NewGenericCompositePodGroup(cpg *schedulingv1alpha3.CompositePodGroup) *GenericPodGroup {
	return &GenericPodGroup{CompositePodGroup: cpg}
}

// GetPodGroup unwraps the underlying PodGroup object. Returns nil if this wraps a CompositePodGroup.
func (gpg *GenericPodGroup) GetPodGroup() *schedulingv1beta1.PodGroup {
	return gpg.PodGroup
}

// GetCompositePodGroup unwraps the underlying CompositePodGroup object. Returns nil if this wraps a PodGroup.
func (gpg *GenericPodGroup) GetCompositePodGroup() *schedulingv1alpha3.CompositePodGroup {
	return gpg.CompositePodGroup
}

// GetObject returns a raw runtime.Object representing the wrapped object.
func (gpg *GenericPodGroup) GetObject() runtime.Object {
	if gpg.PodGroup != nil {
		return gpg.PodGroup
	}
	return gpg.CompositePodGroup
}

// GetUID returns UID of the wrapped object.
func (gpg *GenericPodGroup) GetUID() types.UID {
	if gpg.PodGroup != nil {
		return gpg.PodGroup.UID
	}
	return gpg.CompositePodGroup.UID
}

// GetName returns a name of the wrapped object.
func (gpg *GenericPodGroup) GetName() string {
	if gpg.PodGroup != nil {
		return gpg.PodGroup.Name
	}
	return gpg.CompositePodGroup.Name
}

// GetNamespace returns a namespace of the wrapped object.
func (gpg *GenericPodGroup) GetNamespace() string {
	if gpg.PodGroup != nil {
		return gpg.PodGroup.Namespace
	}
	return gpg.CompositePodGroup.Namespace
}

// GetType returns the type of the wrapped object.
func (gpg *GenericPodGroup) GetType() fwk.EntityKeyType {
	if gpg.PodGroup != nil {
		return fwk.PodGroupKeyType
	}
	return fwk.CompositePodGroupKeyType
}

// GetKey returns a key of the wrapped object.
func (gpg *GenericPodGroup) GetKey() fwk.EntityKey {
	if gpg.PodGroup != nil {
		return fwk.PodGroupKey(gpg.PodGroup.Namespace, gpg.PodGroup.Name)
	}
	return fwk.CompositePodGroupKey(gpg.CompositePodGroup.Namespace, gpg.CompositePodGroup.Name)
}

// GetParentCompositePodGroupName returns the parent composite pod group name of the GenericPodGroup.
// This should be used only when the feature feature gate CompositePodGroup is enabled.
func (gpg *GenericPodGroup) GetParentCompositePodGroupName() *string {
	if gpg.PodGroup != nil {
		return gpg.PodGroup.Spec.ParentCompositePodGroupName
	}
	return gpg.CompositePodGroup.Spec.ParentCompositePodGroupName
}

// HasParent returns true if the GenericPodGroup has a parent.
// This should be used only when the feature feature gate CompositePodGroup is enabled.
func (gpg *GenericPodGroup) HasParent() bool {
	return gpg.GetParentCompositePodGroupName() != nil
}

// GetParentKey returns the parent key of the GenericPodGroup.
// This should be used only when the feature CompositePodGroup feature gate is enabled.
func (gpg *GenericPodGroup) GetParentKey() (fwk.EntityKey, bool) {
	parentName := gpg.GetParentCompositePodGroupName()
	if parentName == nil {
		return fwk.EntityKey{}, false
	}
	return fwk.CompositePodGroupKey(gpg.GetNamespace(), *parentName), true
}

// GetPriority returns the priority of the wrapped object.
func (gpg *GenericPodGroup) GetPriority() int32 {
	if gpg.PodGroup != nil {
		return util.PodGroupPriority(gpg.PodGroup)
	}
	return util.CompositePodGroupPriority(gpg.CompositePodGroup)
}

// GetCreationTimestamp returns the creation timestamp of the wrapped object.
func (gpg *GenericPodGroup) GetCreationTimestamp() time.Time {
	if gpg.PodGroup != nil {
		return gpg.PodGroup.CreationTimestamp.Time
	}
	return gpg.CompositePodGroup.CreationTimestamp.Time
}

// GetPreemptionPolicy returns the PreemptionPolicy set in the inner pod group or composite pod group,
// or the default policy (PreemptLowerPriority) if not set.
// It should be used only when the PodGroupPreemptionPolicy feature gate is enabled.
func (gpg *GenericPodGroup) GetPreemptionPolicy() v1.PreemptionPolicy {
	if pg := gpg.PodGroup; pg != nil && pg.Spec.PreemptionPolicy != nil {
		return v1.PreemptionPolicy(*pg.Spec.PreemptionPolicy)
	}
	if cpg := gpg.CompositePodGroup; cpg != nil && cpg.Spec.PreemptionPolicy != nil {
		return v1.PreemptionPolicy(*cpg.Spec.PreemptionPolicy)
	}
	return v1.PreemptLowerPriority
}

// HasDisruptionModeAll returns true if the wrapped object has disruption mode All.
func (gpg *GenericPodGroup) HasDisruptionModeAll() bool {
	if pg := gpg.PodGroup; pg != nil && pg.Spec.DisruptionMode != nil && pg.Spec.DisruptionMode.All != nil {
		return true
	}
	if cpg := gpg.CompositePodGroup; cpg != nil && cpg.Spec.DisruptionMode != nil && cpg.Spec.DisruptionMode.All != nil {
		return true
	}
	return false
}
