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

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"fmt"
	"sort"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

func init() {
	metrics.Register()
}

func TestPreemptionSnapshot(t *testing.T) {
	nodes := []*v1.Node{st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU:    "1",
		v1.ResourceMemory: "1Gi",
		v1.ResourcePods:   "110",
	}).Obj()}
	pod1 := st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").Node("node1").Obj()
	pod2 := st.MakePod().Name("pod2").Namespace("default").UID("uid-pod2").Node("node1").Obj()

	t.Run("Preempt outside transaction", func(t *testing.T) {
		ctx := t.Context()
		cs, _, _ := setupSnapshotTest(t, ctx, nodes, []*v1.Pod{pod1, pod2})
		initialVersion := cs.stateVersion
		u, err := cs.PreemptPods(ctx, nil, []*v1.Pod{pod1})
		if err != nil {
			t.Fatalf("PreemptPods failed: %v", err)
		}
		if u != nil {
			t.Errorf("Expected nil preemption handle outside transaction")
		}
		_, err = cs.Unpreempt(u)
		if err == nil {
			t.Errorf("Expected Unpreempt with nil handle to fail")
		}
		if cs.stateVersion != initialVersion+1 {
			t.Errorf("Expected stateVersion to be incremented, got %d, want %d", cs.stateVersion, initialVersion+1)
		}
		nodeInfo, err := cs.schedulerSnapshot.NodeInfos().Get("node1")
		if err != nil {
			t.Fatalf("failed to get node info: %v", err)
		}
		foundPod1 := false
		for _, pInfo := range nodeInfo.GetPods() {
			if pInfo.GetPod().Name == "pod1" {
				foundPod1 = true
			}
		}
		if foundPod1 {
			t.Errorf("Expected pod1 to be removed from nodeInfo")
		}
	})

	t.Run("Unpreempt in transaction", func(t *testing.T) {
		ctx := t.Context()
		cs, _, _ := setupSnapshotTest(t, ctx, nodes, []*v1.Pod{pod1, pod2})
		err := cs.Transaction(ctx, klog.FromContext(ctx), func() (TransactionResult, error) {
			uA, err := cs.PreemptPods(ctx, nil, []*v1.Pod{pod1})
			if err != nil {
				return Revert, err
			}
			uB, err := cs.PreemptPods(ctx, nil, []*v1.Pod{pod2})
			if err != nil {
				return Revert, err
			}

			// Unpreempt A first (non-LIFO / arbitrary order). Should succeed.
			_, err = cs.Unpreempt(uA)
			if err != nil {
				t.Errorf("Expected Unpreempt A to succeed, got: %v", err)
			}

			// Unpreempt B next. Should succeed.
			_, err = cs.Unpreempt(uB)
			if err != nil {
				t.Errorf("Expected Unpreempt B to succeed, got: %v", err)
			}

			return Commit, nil
		})
		if err != nil {
			t.Fatalf("Transaction failed: %v", err)
		}
	})

	t.Run("Committed transaction handle reuse fails", func(t *testing.T) {
		ctx := t.Context()
		cs, _, _ := setupSnapshotTest(t, ctx, nodes, []*v1.Pod{pod1, pod2})
		var u *Unpreemption
		err := cs.Transaction(ctx, klog.FromContext(ctx), func() (TransactionResult, error) {
			var txErr error
			u, txErr = cs.PreemptPods(ctx, nil, []*v1.Pod{pod1})
			if txErr != nil {
				return Revert, txErr
			}
			return Commit, nil
		})
		if err != nil {
			t.Fatalf("Transaction failed: %v", err)
		}

		// Now try to unpreempt inside a new transaction. It should fail because the handle is from a previous transaction.
		err = cs.Transaction(ctx, klog.FromContext(ctx), func() (TransactionResult, error) {
			_, err = cs.Unpreempt(u)
			if err == nil {
				t.Errorf("Expected Unpreempt to fail using handle from a committed transaction")
			}
			return Commit, nil
		})
		if err != nil {
			t.Fatalf("Transaction failed: %v", err)
		}
	})
}

func TestPreemptionTransactionLifecycle(t *testing.T) {
	ctx := t.Context()
	nodes := []*v1.Node{st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU:    "1",
		v1.ResourceMemory: "1Gi",
		v1.ResourcePods:   "110",
	}).Obj()}
	pod := st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").Node("node1").Obj()

	tests := []struct {
		name         string
		unpreempt    bool
		resultAction TransactionResult
		expectBack   bool
	}{
		{
			name:         "Unpreempt inside transaction restores pod",
			unpreempt:    true,
			resultAction: Commit,
			expectBack:   true,
		},
		{
			name:         "Rollback transaction restores pod",
			unpreempt:    false,
			resultAction: Revert,
			expectBack:   true,
		},
		{
			name:         "Commit transaction without unpreempt keeps pod preempted",
			unpreempt:    false,
			resultAction: Commit,
			expectBack:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cs, snap, _ := setupSnapshotTest(t, ctx, nodes, []*v1.Pod{pod})

			err := cs.Transaction(ctx, klog.FromContext(ctx), func() (TransactionResult, error) {
				// Preempt the pod.
				u, err := cs.PreemptPods(ctx, nil, []*v1.Pod{pod})
				if err != nil {
					return Revert, err
				}

				// Verify pod is gone.
				nodeInfo, err := snap.NodeInfos().Get("node1")
				if err != nil {
					return Revert, err
				}
				if len(nodeInfo.GetPods()) != 0 {
					return Revert, fmt.Errorf("expected 0 pods after preemption, got %d", len(nodeInfo.GetPods()))
				}

				if tc.unpreempt {
					// Restore via Unpreempt.
					if _, err := cs.Unpreempt(u); err != nil {
						return Revert, err
					}
				}

				return tc.resultAction, nil
			})
			if err != nil {
				t.Fatalf("Transaction failed: %v", err)
			}

			// Verify pod state on node after transaction finishes
			nodeInfo, err := snap.NodeInfos().Get("node1")
			if err != nil {
				t.Fatalf("failed to get node info: %v", err)
			}
			podsCount := len(nodeInfo.GetPods())
			if tc.expectBack {
				if podsCount != 1 {
					t.Errorf("expected pod to be restored, but node has %d pods", podsCount)
				}
			} else {
				if podsCount != 0 {
					t.Errorf("expected pod to remain preempted, but node has %d pods", podsCount)
				}
			}
		})
	}
}

func TestTransaction(t *testing.T) {
	tests := []struct {
		name             string
		transactionFn    func(cs *ClusterSnapshot, revertCalled *bool, state map[string]any) (TransactionResult, error)
		expectErr        bool
		expectStackEmpty bool
		checkResult      func(t *testing.T, revertCalled bool, state map[string]any)
	}{
		{
			name: "Commit does not roll back",
			transactionFn: func(cs *ClusterSnapshot, revertCalled *bool, state map[string]any) (TransactionResult, error) {
				cs.txCompensations = append(cs.txCompensations, &txMutation{
					revertFn: func() { *revertCalled = true },
					active:   true,
				})
				return Commit, nil
			},
			expectErr:        false,
			expectStackEmpty: true,
			checkResult: func(t *testing.T, revertCalled bool, state map[string]any) {
				if revertCalled {
					t.Errorf("Expected revert NOT to be called on committed transaction")
				}
			},
		},
		{
			name: "Revert rolls back",
			transactionFn: func(cs *ClusterSnapshot, revertCalled *bool, state map[string]any) (TransactionResult, error) {
				cs.txCompensations = append(cs.txCompensations, &txMutation{
					revertFn: func() { *revertCalled = true },
					active:   true,
				})
				return Revert, nil
			},
			expectErr:        false,
			expectStackEmpty: true,
			checkResult: func(t *testing.T, revertCalled bool, state map[string]any) {
				if !revertCalled {
					t.Errorf("Expected revert to be called on reverted transaction")
				}
			},
		},
		{
			name: "Error rolls back automatically",
			transactionFn: func(cs *ClusterSnapshot, revertCalled *bool, state map[string]any) (TransactionResult, error) {
				cs.txCompensations = append(cs.txCompensations, &txMutation{
					revertFn: func() { *revertCalled = true },
					active:   true,
				})
				return Commit, fmt.Errorf("some error")
			},
			expectErr:        true,
			expectStackEmpty: true,
			checkResult: func(t *testing.T, revertCalled bool, state map[string]any) {
				if !revertCalled {
					t.Errorf("Expected revert to be called on error transaction exit")
				}
			},
		},
		{
			name: "Nested transaction fails",
			transactionFn: func(cs *ClusterSnapshot, revertCalled *bool, state map[string]any) (TransactionResult, error) {
				err := cs.Transaction(context.TODO(), klog.Background(), func() (TransactionResult, error) {
					return Commit, nil
				})
				if err == nil {
					return Revert, fmt.Errorf("expected nested transaction to fail")
				}
				state["nested_err"] = err
				return Commit, nil
			},
			expectErr:        false,
			expectStackEmpty: true,
			checkResult: func(t *testing.T, revertCalled bool, state map[string]any) {
				err, ok := state["nested_err"].(error)
				if !ok {
					t.Fatalf("expected nested transaction error in state")
				}
				expectedMsg := "a transaction is already in progress"
				if err.Error() != expectedMsg {
					t.Errorf("unexpected error message: %q, want %q", err.Error(), expectedMsg)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cs := &ClusterSnapshot{}
			revertCalled := false
			state := make(map[string]any)
			ctx := t.Context()

			err := cs.Transaction(ctx, klog.FromContext(ctx), func() (TransactionResult, error) {
				return tc.transactionFn(cs, &revertCalled, state)
			})

			if (err != nil) != tc.expectErr {
				t.Errorf("Transaction() error = %v, expectErr %v", err, tc.expectErr)
			}

			if tc.expectStackEmpty && cs.txCompensations != nil {
				t.Errorf("Expected txCompensations to be nil, got non-nil")
			}

			if tc.checkResult != nil {
				tc.checkResult(t, revertCalled, state)
			}
		})
	}
}

func TestCanSchedulePod(t *testing.T) {
	tests := []struct {
		name           string
		candidateNodes []string
		schedulerName  string
		podRequestCPU  string
		expectNodes    []string
		expectErr      bool
		expectRejected map[string]string
	}{
		{
			name:           "Success - all nodes eligible",
			candidateNodes: []string{"node1", "node2"},
			expectNodes:    []string{"node1", "node2"},
			expectErr:      false,
		},
		{
			name:           "Error - unknown scheduler name",
			candidateNodes: []string{"node1"},
			schedulerName:  "unknown-scheduler",
			expectErr:      true,
		},
		{
			name:           "Success - empty candidate list returns empty result",
			candidateNodes: []string{},
			expectNodes:    nil,
			expectErr:      false,
		},
		{
			name:           "Rejected - insufficient cpu",
			candidateNodes: []string{"node1"},
			podRequestCPU:  "1",
			expectNodes:    []string{},
			expectErr:      false,
			expectRejected: map[string]string{
				"node1": "Insufficient cpu",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			snapshotNodes := make([]*v1.Node, len(tc.candidateNodes))
			for i, name := range tc.candidateNodes {
				snapshotNodes[i] = st.MakeNode().Name(name).Capacity(map[v1.ResourceName]string{
					v1.ResourceCPU:    "0",
					v1.ResourceMemory: "0",
					v1.ResourcePods:   "110",
				}).Obj()
			}

			cs, _, _ := setupSnapshotTest(t, ctx, snapshotNodes, nil)

			podBuilder := st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").SchedulerName(tc.schedulerName)
			if tc.podRequestCPU != "" {
				podBuilder = podBuilder.Req(map[v1.ResourceName]string{
					v1.ResourceCPU: tc.podRequestCPU,
				})
			}
			pod := SchedulablePod{
				Pod:                podBuilder.Obj(),
				CandidateNodeNames: tc.candidateNodes,
			}

			nodes, diagnosis, err := cs.CanSchedulePod(ctx, klog.FromContext(ctx), pod)
			if (err != nil) != tc.expectErr {
				t.Fatalf("CanSchedulePod() error = %v, expectErr %v", err, tc.expectErr)
			}

			if !tc.expectErr {
				sort.Strings(nodes)
				sort.Strings(tc.expectNodes)
				if len(nodes) != len(tc.expectNodes) {
					t.Errorf("Expected nodes %v, got %v", tc.expectNodes, nodes)
				} else {
					for i, n := range nodes {
						if n != tc.expectNodes[i] {
							t.Errorf("Expected nodes %v, got %v", tc.expectNodes, nodes)
							break
						}
					}
				}

				if len(tc.expectRejected) > 0 {
					if diagnosis == nil {
						t.Errorf("Expected diagnosis, got nil")
					} else {
						for nodeName, expectedMsg := range tc.expectRejected {
							status := diagnosis.NodeToStatus.Get(nodeName)
							if status == nil {
								t.Errorf("Expected status for node %s, got nil", nodeName)
							} else if !strings.Contains(status.Message(), expectedMsg) {
								t.Errorf("Expected status message %q to contain %q for node %s", status.Message(), expectedMsg, nodeName)
							}
						}
					}
				}
			}
		})
	}
}

func TestSchedulePods(t *testing.T) {
	tests := []struct {
		name               string
		pods               []SchedulablePod
		opts               SchedulePodsOptions
		expectResults      int
		expectErr          bool
		expectTxLen        int // expected length of txCompensation for current tx
		inTransaction      bool
		unschedulableNodes []string
	}{
		{
			name: "Success - schedule one pod",
			pods: []SchedulablePod{
				{
					Pod:                st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").Obj(),
					CandidateNodeNames: []string{"node1"},
				},
			},
			opts:          SchedulePodsOptions{},
			expectResults: 1,
			expectErr:     false,
		},
		{
			name: "DryRun - does not persist",
			pods: []SchedulablePod{
				{
					Pod:                st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").Obj(),
					CandidateNodeNames: []string{"node1"},
				},
			},
			opts:          NewSchedulePodsOptions(true, false),
			expectResults: 1,
			expectErr:     false,
		},
		{
			name: "InTransaction - adds to compensation",
			pods: []SchedulablePod{
				{
					Pod:                st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").Obj(),
					CandidateNodeNames: []string{"node1"},
				},
			},
			opts:          SchedulePodsOptions{},
			inTransaction: true,
			expectResults: 1,
			expectErr:     false,
			expectTxLen:   1,
		},
		{
			name: "StopOnFailure - fails on first pod",
			pods: []SchedulablePod{
				{
					Pod:                st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").Obj(),
					CandidateNodeNames: []string{"non-existent-node"}, // will fail filter or node lookup
				},
			},
			opts:          NewSchedulePodsOptions(false, true),
			expectResults: 0,
			expectErr:     true,
		},
		{
			name: "Fails due to node unschedulable",
			pods: []SchedulablePod{
				{
					Pod:                st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").Obj(),
					CandidateNodeNames: []string{"node1"},
				},
			},
			opts:               NewSchedulePodsOptions(false, true),
			expectResults:      0,
			expectErr:          true,
			unschedulableNodes: []string{"node1"},
		},
		{
			name: "StopOnFailure - succeeds on first, fails on second due to node unschedulable",
			pods: []SchedulablePod{
				{
					Pod:                st.MakePod().Name("pod1").Namespace("default").UID("uid-pod1").Obj(),
					CandidateNodeNames: []string{"node1"},
				},
				{
					Pod:                st.MakePod().Name("pod2").Namespace("default").UID("uid-pod2").Obj(),
					CandidateNodeNames: []string{"node2"},
				},
			},
			opts:               NewSchedulePodsOptions(false, true),
			expectResults:      1,    // Pod 1 succeeds on node1
			expectErr:          true, // Pod 2 fails on node2
			unschedulableNodes: []string{"node2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			var nodes []*v1.Node
			nodeMap := make(map[string]*v1.Node)
			unschedSet := make(map[string]struct{})
			for _, n := range tc.unschedulableNodes {
				unschedSet[n] = struct{}{}
			}
			for _, pod := range tc.pods {
				for _, nodeName := range pod.CandidateNodeNames {
					if nodeName != "non-existent-node" && nodeMap[nodeName] == nil {
						node := &v1.Node{
							ObjectMeta: metav1.ObjectMeta{Name: nodeName},
							Status: v1.NodeStatus{
								Allocatable: v1.ResourceList{
									v1.ResourcePods: resource.MustParse("110"),
								},
								Capacity: v1.ResourceList{
									v1.ResourcePods: resource.MustParse("110"),
								},
							},
						}
						if _, ok := unschedSet[nodeName]; ok {
							node.Spec.Unschedulable = true
						}
						nodeMap[nodeName] = node
						nodes = append(nodes, node)
					}
				}
			}

			cs, _, _ := setupSnapshotTest(t, ctx, nodes, nil)

			if tc.inTransaction {
				cs.txCompensations = []*txMutation{}
			}

			results, err := cs.SchedulePods(ctx, klog.FromContext(ctx), tc.pods, tc.opts)
			if (err != nil) != tc.expectErr {
				t.Fatalf("SchedulePods() error = %v, expectErr %v", err, tc.expectErr)
			}

			if len(results) != tc.expectResults {
				t.Errorf("Expected results %d, got %d", tc.expectResults, len(results))
			}

			if tc.inTransaction {
				txLen := len(cs.txCompensations)
				if txLen != tc.expectTxLen {
					t.Errorf("Expected tx compensation len %d, got %d", tc.expectTxLen, txLen)
				}
			}
		})
	}
}

func TestSchedulePodsByTemplate(t *testing.T) {
	tests := []struct {
		name           string
		template       *v1.PodTemplateSpec
		candidateNodes []string
		maxPods        int
		opts           SchedulePodsByTemplateOptions
		expectResults  int
		expectErr      bool
		expectTxLen    int
		inTransaction  bool
	}{
		{
			name:           "Success - schedule maxPods",
			template:       &v1.PodTemplateSpec{Spec: v1.PodSpec{}},
			candidateNodes: []string{"node1", "node2"},
			maxPods:        2,
			opts:           SchedulePodsByTemplateOptions{},
			expectResults:  2,
			expectErr:      false,
		},
		{
			name:           "DryRun - does not persist",
			template:       &v1.PodTemplateSpec{Spec: v1.PodSpec{}},
			candidateNodes: []string{"node1"},
			maxPods:        2,
			opts:           NewSchedulePodsByTemplateOptions(true),
			expectResults:  2,
			expectErr:      false,
		},
		{
			name:           "InTransaction - adds to compensation",
			template:       &v1.PodTemplateSpec{Spec: v1.PodSpec{}},
			candidateNodes: []string{"node1"},
			maxPods:        1,
			opts:           SchedulePodsByTemplateOptions{},
			inTransaction:  true,
			expectResults:  1,
			expectErr:      false,
			expectTxLen:    1,
		},
		{
			name: "Custom Namespace",
			template: &v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Namespace: "custom-ns"},
				Spec:       v1.PodSpec{},
			},
			candidateNodes: []string{"node1"},
			maxPods:        1,
			opts:           SchedulePodsByTemplateOptions{},
			expectResults:  1,
			expectErr:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			var nodes []*v1.Node
			for _, nodeName := range tc.candidateNodes {
				nodes = append(nodes, &v1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: nodeName},
					Status: v1.NodeStatus{
						Allocatable: v1.ResourceList{
							v1.ResourcePods: resource.MustParse("110"),
						},
						Capacity: v1.ResourceList{
							v1.ResourcePods: resource.MustParse("110"),
						},
					},
				})
			}

			cs, _, _ := setupSnapshotTest(t, ctx, nodes, nil)

			if tc.inTransaction {
				cs.txCompensations = []*txMutation{}
			}

			results, err := cs.SchedulePodsByTemplate(ctx, klog.FromContext(ctx), tc.template, tc.candidateNodes, tc.maxPods, tc.opts)
			if (err != nil) != tc.expectErr {
				t.Fatalf("SchedulePodsByTemplate() error = %v, expectErr %v", err, tc.expectErr)
			}

			if len(results) != tc.expectResults {
				t.Errorf("Expected results %d, got %d", tc.expectResults, len(results))
			}

			if tc.inTransaction {
				txLen := len(cs.txCompensations)
				if txLen != tc.expectTxLen {
					t.Errorf("Expected tx compensation len %d, got %d", tc.expectTxLen, txLen)
				}
			}
		})
	}
}
