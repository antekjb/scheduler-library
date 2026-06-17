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

package state

import (
	"fmt"
	"sync"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
	plugins "k8s.io/kubernetes/pkg/scheduler/framework/plugins"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/profile"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	upstreamsync "sigs.k8s.io/scheduler-library/pkg/upstream_sync"
)

func init() {
	metrics.Register()
}

func TestClusterState_AddPod(t *testing.T) {
	tests := []struct {
		name         string
		existingPods []*v1.Pod
		podToAdd     *v1.Pod
		expectCount  int
	}{
		{
			name:        "add unassigned pod",
			podToAdd:    st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").Obj(),
			expectCount: 1,
		},
		{
			name:        "add assigned pod",
			podToAdd:    st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").Node("node1").Obj(),
			expectCount: 1,
		},
		{
			name: "add duplicate pod",
			existingPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").Obj(),
			},
			podToAdd:    st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").Obj(),
			expectCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			logger := klog.FromContext(ctx)
			state := New(cache.New(ctx, nil, false), newDummyScheduler(), nil)

			for _, p := range tc.existingPods {
				if err := state.Cache.AddPod(logger, p); err != nil {
					t.Fatalf("Failed to add pod: %v", err)
				}
			}

			_ = state.Cache.AddPod(logger, tc.podToAdd)

			count, err := state.Cache.PodCount()
			if err != nil {
				t.Fatalf("Failed to get pod count: %v", err)
			}
			if count != tc.expectCount {
				t.Errorf("Expected pod count %d, got %d", tc.expectCount, count)
			}
		})
	}
}

func TestClusterState_RemovePod(t *testing.T) {
	tests := []struct {
		name         string
		existingPods []*v1.Pod
		podToRemove  *v1.Pod
		expectCount  int
	}{
		{
			name: "remove existing pod",
			existingPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").Obj(),
			},
			podToRemove: st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").Obj(),
			expectCount: 0,
		},
		{
			name: "remove non-existent pod",
			existingPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").Obj(),
			},
			podToRemove: st.MakePod().Name("pod2").Namespace("default").UID("uid-pod2").Obj(),
			expectCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			logger := klog.FromContext(ctx)
			state := New(cache.New(ctx, nil, false), newDummyScheduler(), nil)

			for _, p := range tc.existingPods {
				if err := state.Cache.AddPod(logger, p); err != nil {
					t.Fatalf("Failed to add pod: %v", err)
				}
			}

			_ = state.Cache.RemovePod(logger, tc.podToRemove)

			count, err := state.Cache.PodCount()
			if err != nil {
				t.Fatalf("Failed to get pod count: %v", err)
			}
			if count != tc.expectCount {
				t.Errorf("Expected pod count %d, got %d", tc.expectCount, count)
			}
		})
	}
}

func TestClusterState_AddNode(t *testing.T) {
	tests := []struct {
		name          string
		existingNodes []*v1.Node
		nodeToAdd     *v1.Node
		expectCount   int
	}{
		{
			name: "add valid node",
			nodeToAdd: st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{
				v1.ResourceCPU:    "1",
				v1.ResourceMemory: "1Gi",
				v1.ResourcePods:   "110",
			}).Obj(),
			expectCount: 1,
		},
		{
			name: "add duplicate node",
			existingNodes: []*v1.Node{
				st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{
					v1.ResourceCPU:    "1",
					v1.ResourceMemory: "1Gi",
					v1.ResourcePods:   "110",
				}).Obj(),
			},
			nodeToAdd: st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{
				v1.ResourceCPU:    "1",
				v1.ResourceMemory: "1Gi",
				v1.ResourcePods:   "110",
			}).Obj(),
			expectCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			logger := klog.FromContext(ctx)
			state := New(cache.New(ctx, nil, false), newDummyScheduler(), nil)

			for _, n := range tc.existingNodes {
				state.Cache.AddNode(logger, n)
			}

			state.Cache.AddNode(logger, tc.nodeToAdd)

			count := state.Cache.NodeCount()
			if count != tc.expectCount {
				t.Errorf("Expected node count %d, got %d", tc.expectCount, count)
			}
		})
	}
}

func TestClusterState_RemoveNode(t *testing.T) {
	tests := []struct {
		name          string
		existingNodes []*v1.Node
		nodeToRemove  *v1.Node
		expectCount   int
	}{
		{
			name: "remove existing node",
			existingNodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			nodeToRemove: st.MakeNode().Name("node1").Obj(),
			expectCount:  0,
		},
		{
			name: "remove non-existent node",
			existingNodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			nodeToRemove: st.MakeNode().Name("node2").Obj(),
			expectCount:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			logger := klog.FromContext(ctx)
			state := New(cache.New(ctx, nil, false), newDummyScheduler(), nil)

			for _, n := range tc.existingNodes {
				state.Cache.AddNode(logger, n)
			}

			_ = state.Cache.RemoveNode(logger, tc.nodeToRemove)

			count := state.Cache.NodeCount()
			if count != tc.expectCount {
				t.Errorf("Expected node count %d, got %d", tc.expectCount, count)
			}
		})
	}
}

func TestClusterState_Snapshot(t *testing.T) {
	tests := []struct {
		name          string
		existingNodes []*v1.Node
		existingPods  []*v1.Pod
		hasFramework  bool
	}{
		{
			name: "empty snapshot",
		},
		{
			name: "snapshot with data",
			existingNodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			existingPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").Obj(),
			},
		},
		{
			name: "snapshot in sync with framework snapshot",
			existingNodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			existingPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").Node("node1").Obj(),
			},
			hasFramework: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			var sched *upstreamsync.Scheduler
			sharedSnap := cache.NewEmptySnapshot()
			if tc.hasFramework {
				informerFactory := informers.NewSharedInformerFactory(fake.NewClientset(), 0)
				registry := plugins.NewInTreeRegistry()
				prof := schedulerapi.KubeSchedulerProfile{
					SchedulerName: "default-scheduler",
				}
				fwk, err := frameworkruntime.NewFramework(ctx, registry, &prof,
					frameworkruntime.WithSnapshotSharedLister(sharedSnap),
					frameworkruntime.WithInformerFactory(informerFactory),
				)
				if err != nil {
					t.Fatalf("Failed to create framework: %v", err)
				}
				sched = &upstreamsync.Scheduler{
					Scheduler: &scheduler.Scheduler{
						Profiles: profile.Map{
							"default-scheduler": fwk,
						},
					},
				}
			} else {
				sched = newDummyScheduler()
			}

			state := New(cache.New(ctx, nil, false), sched, sharedSnap)
			logger := klog.FromContext(ctx)

			for _, n := range tc.existingNodes {
				state.Cache.AddNode(logger, n)
			}
			for _, p := range tc.existingPods {
				if err := state.Cache.AddPod(logger, p); err != nil {
					t.Fatalf("Failed to add pod: %v", err)
				}
			}

			snap, err := state.Snapshot(logger)
			if err != nil {
				t.Fatalf("Snapshot() error = %v", err)
			}
			if snap == nil {
				t.Fatal("Expected snapshot to be non-nil")
			}

			if tc.hasFramework {
				for _, n := range tc.existingNodes {
					nodeInfo, err := sharedSnap.NodeInfos().Get(n.Name)
					if err != nil {
						t.Fatalf("Failed to get node %s from shared framework snapshot: %v", n.Name, err)
					}
					if nodeInfo == nil {
						t.Fatalf("Expected nodeInfo for node %s to be non-nil", n.Name)
					}

					expectedPods := make(map[string]bool)
					for _, p := range tc.existingPods {
						if p.Spec.NodeName == n.Name {
							expectedPods[p.Name] = true
						}
					}

					pods := nodeInfo.GetPods()
					if len(pods) != len(expectedPods) {
						t.Fatalf("Expected %d pods on node %s in shared framework snapshot, got %d", len(expectedPods), n.Name, len(pods))
					}

					for _, podInfo := range pods {
						podName := podInfo.GetPod().Name
						if !expectedPods[podName] {
							t.Fatalf("Unexpected pod %s on node %s in shared framework snapshot", podName, n.Name)
						}
					}
				}
			}
		})
	}
}

func TestClusterState_ComplexScenarios(t *testing.T) {
	t.Run("pod assigned to non-existent node", func(t *testing.T) {
		ctx := t.Context()
		logger := klog.FromContext(ctx)
		state := New(cache.New(ctx, nil, false), newDummyScheduler(), nil)

		pod := st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").Node("node1").Obj()

		// Add pod first
		if err := state.Cache.AddPod(logger, pod); err != nil {
			t.Fatalf("Failed to add pod: %v", err)
		}

		// Verify pod count
		pCount, err := state.Cache.PodCount()
		if err != nil {
			t.Fatal(err)
		}
		if pCount != 1 {
			t.Errorf("Expected pod count 1, got %d", pCount)
		}

		// Add node later
		node := st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{
			v1.ResourceCPU:    "1",
			v1.ResourceMemory: "1Gi",
			v1.ResourcePods:   "110",
		}).Obj()
		state.Cache.AddNode(logger, node)

		// Verify node count
		nCount := state.Cache.NodeCount()
		if nCount != 1 {
			t.Errorf("Expected node count 1, got %d", nCount)
		}
	})

	t.Run("node removal with pods", func(t *testing.T) {
		ctx := t.Context()
		logger := klog.FromContext(ctx)
		state := New(cache.New(ctx, nil, false), newDummyScheduler(), nil)

		node := st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{
			v1.ResourceCPU:    "1",
			v1.ResourceMemory: "1Gi",
			v1.ResourcePods:   "110",
		}).Obj()
		pod := st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").Node("node1").Obj()

		state.Cache.AddNode(logger, node)
		if err := state.Cache.AddPod(logger, pod); err != nil {
			t.Fatalf("Failed to add pod: %v", err)
		}

		// Remove node
		if err := state.Cache.RemoveNode(logger, node); err != nil {
			t.Fatalf("Failed to remove node: %v", err)
		}

		// Verify node count is still 1 (ghost node) because pod still exists
		nCount := state.Cache.NodeCount()
		if nCount != 1 {
			t.Errorf("Expected node count 1 (ghost node), got %d", nCount)
		}

		// Verify pod count is still 1
		pCount, err := state.Cache.PodCount()
		if err != nil {
			t.Fatal(err)
		}
		if pCount != 1 {
			t.Errorf("Expected pod count 1, got %d", pCount)
		}

		// Now remove the pod
		if err := state.Cache.RemovePod(logger, pod); err != nil {
			t.Fatalf("Failed to remove pod: %v", err)
		}

		// Verify node count becomes 0 after pod is removed
		nCount = state.Cache.NodeCount()
		if nCount != 0 {
			t.Errorf("Expected node count 0 after pod removal, got %d", nCount)
		}
	})
}

func TestClusterState_ConcurrentAccess(t *testing.T) {
	ctx := t.Context()
	state := New(cache.New(ctx, nil, false), newDummyScheduler(), cache.NewEmptySnapshot())

	const numGoroutines = 20
	const numOperations = 50

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			logger := klog.FromContext(ctx)
			for j := range numOperations {
				pod := &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("pod-%d-%d", id, j),
						Namespace: "default",
						UID:       types.UID(fmt.Sprintf("uid-%d-%d", id, j)),
					},
				}
				node := &v1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("node-%d-%d", id, j)},
				}

				state.Cache.AddNode(logger, node)
				if err := state.Cache.AddPod(logger, pod); err != nil {
					// Concurrent access might cause some pods to already exist if not careful,
					// but here names are unique by goroutine id and iteration j.
					t.Errorf("Failed to add pod: %v", err)
				}
				if _, err := state.Snapshot(logger); err != nil {
					t.Errorf("Snapshot() error = %v", err)
				}
				if err := state.Cache.RemovePod(logger, pod); err != nil {
					t.Errorf("Failed to remove pod: %v", err)
				}
				if err := state.Cache.RemoveNode(logger, node); err != nil {
					t.Errorf("Failed to remove node: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
}

func newDummyScheduler() *upstreamsync.Scheduler {
	return &upstreamsync.Scheduler{
		Scheduler: &scheduler.Scheduler{
			Profiles: make(profile.Map),
		},
	}
}
