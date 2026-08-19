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

package testing

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
)

// VerifySnapshot asserts which pods the snapshot has on which node.
func VerifySnapshot(t *testing.T, snap *cache.Snapshot, expectedNodePods map[string][]string) {
	t.Helper()
	if snap == nil {
		t.Fatal("Expected snapshot to be non-nil")
	}

	nodeList, err := snap.NodeInfos().List()
	if err != nil {
		t.Fatalf("Failed to list nodes: %v", err)
	}

	gotNodePods := make(map[string][]string)
	for _, n := range nodeList {
		if n.Node() == nil {
			continue
		}
		nodeName := n.Node().Name
		var pods []string
		for _, pInfo := range n.GetPods() {
			if pInfo != nil && pInfo.GetPod() != nil {
				pods = append(pods, pInfo.GetPod().Name)
			}
		}
		gotNodePods[nodeName] = pods
	}

	if diff := cmp.Diff(expectedNodePods, gotNodePods, cmpopts.EquateEmpty(), cmpopts.SortSlices(func(x, y string) bool { return x < y })); diff != "" {
		t.Errorf("Unexpected snapshot nodePods (-want +got):\n%s", diff)
	}
}

// VerifySnapshotPodCounts asserts how many pods the snapshot has on each node, for the cases where
// the pod names are not known upfront.
func VerifySnapshotPodCounts(t *testing.T, snap *cache.Snapshot, expectedCounts map[string]int) {
	t.Helper()
	if snap == nil {
		t.Fatal("Expected snapshot to be non-nil")
	}

	nodeList, err := snap.NodeInfos().List()
	if err != nil {
		t.Fatalf("Failed to list nodes: %v", err)
	}

	gotNodePodCounts := make(map[string]int)
	for _, n := range nodeList {
		if n.Node() == nil {
			continue
		}
		gotNodePodCounts[n.Node().Name] = len(n.GetPods())
	}

	if diff := cmp.Diff(expectedCounts, gotNodePodCounts, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("Unexpected snapshot pod counts (-want +got):\n%s", diff)
	}
}
