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

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	fwk "k8s.io/kube-scheduler/framework"
)

// candidate represents a nominated node on which the preemptor can be scheduled,
// along with the list of victims that should be evicted for the preemptor to fit the node.
type candidate struct {
	// victims wraps a list of to-be-preempted Pods and the number of PDB violation.
	victims *extenderv1.Victims
	// name returns the target domain(for pod group)/node name where the preemptor gets nominated to run.
	name string
	// numPodGroupDisruptions returns the number of preemption units that affect pod groups.
	// A single preemption unit can be all pods in a pod group (for DisruptionMode=all) or a single pod (for DisruptionMode=single).
	numPodGroupDisruptions int
}

var _ PreemptionCandidate = &candidate{}

// Victims returns s.victims.
func (s *candidate) Victims() *extenderv1.Victims {
	return s.victims
}

// Name returns s.name.
func (s *candidate) Name() string {
	return s.name
}

// NumPodGroupDisruptions returns s.numPodGroupDisruptions.
func (s *candidate) NumPodGroupDisruptions() int {
	return s.numPodGroupDisruptions
}

// NoopPreemptionManager is dummy implementation of PreemptionManager, PreemptionExecutor and ReprieveFilter.
type NoopPreemptionManager struct {
}

var _ PreemptionManager = &NoopPreemptionManager{}
var _ PreemptionExecutor = &NoopPreemptionManager{}
var _ ReprieveFilter = &NoopPreemptionManager{}

// GenerateVictims is dummy implementation of GenerateVictims from PreemptionManager interface.
func (s *NoopPreemptionManager) GenerateVictims(ctx context.Context, pgInfo fwk.PodGroupInfo) ([]PreemptionVictim, *fwk.Status) {
	return nil, nil
}

// Executor returns PreemptionExecutor from PreemptionManager interface.
func (s *NoopPreemptionManager) Executor() PreemptionExecutor {
	return s
}

// NewReprieveFilter is dummy implementation of NewReprieveFilter from PreemptionManager interface.
func (s *NoopPreemptionManager) NewReprieveFilter(_ []PreemptionVictim) ReprieveFilter {
	return s
}

// IsPodRunningPreemption is dummy implementation of IsPodRunningPreemption from PreemptionExecutor interface.
func (s *NoopPreemptionManager) IsPodRunningPreemption(podUID types.UID) bool {
	return false
}

// IsPodGroupRunningPreemption is dummy implementation of IsPodGroupRunningPreemption from PreemptionExecutor interface.
func (s *NoopPreemptionManager) IsPodGroupRunningPreemption(podGroupUID types.UID) bool {
	return false
}

// IsPodGroupWaitingForVictims is dummy implementation of IsPodGroupWaitingForVictims from PreemptionExecutor interface.
func (s *NoopPreemptionManager) IsPodGroupWaitingForVictims(pgInfo fwk.PodGroupInfo) bool {
	return false
}

// ActuatePodPreemption is dummy implementation of ActuatePodPreemption from PreemptionExecutor interface.
func (s *NoopPreemptionManager) ActuatePodPreemption(ctx context.Context, candidate PreemptionCandidate, pod *v1.Pod, pluginName string) *fwk.Status {
	return nil
}

// ActuatePodGroupPreemption is dummy implementation of ActuatePodGroupPreemption from PreemptionExecutor interface.
func (s *NoopPreemptionManager) ActuatePodGroupPreemption(ctx context.Context, candidate PreemptionCandidate, pgInfo fwk.PodGroupInfo, pluginName string) *fwk.Status {
	return nil
}

// CanReprieveVictim is dummy implementation of CanReprieveVictim from ReprieveFilter interface.
func (s *NoopPreemptionManager) CanReprieveVictim(victim PreemptionVictim) bool {
	return false
}

// OnVictimReprieved is dummy implementation of OnVictimReprieved from ReprieveFilter interface.
func (s *NoopPreemptionManager) OnVictimReprieved(victim PreemptionVictim) {
}
