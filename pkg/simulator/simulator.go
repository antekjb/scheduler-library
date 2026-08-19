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

package simulator

import (
	"context"
	"fmt"

	"sigs.k8s.io/scheduler-library/pkg/framework"
	"sigs.k8s.io/scheduler-library/pkg/state"
	"sigs.k8s.io/scheduler-library/pkg/upstreamsync"
	"sigs.k8s.io/scheduler-library/pkg/upstreamsync/snapshot"

	v1 "k8s.io/api/core/v1"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/scheduler"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
)

// SchedulingSimulator is the entry point of the library: it owns the scheduler configuration and
// the informers, and creates the objects the simulation is run against (see NewClusterState and
// NewClusterSnapshot). It is meant to be created once and reused; every state and snapshot it
// creates gets its own scheduling profiles built from the same configuration.
type SchedulingSimulator struct {
	cfg             *schedulerapi.KubeSchedulerConfiguration
	informerFactory informers.SharedInformerFactory
	client          kubernetes.Interface
}

// NewSchedulingSimulator creates a new SchedulingSimulator.
// The cfg may be nil, in which case the default kube-scheduler profile is used, and so may the
// informerFactory, in which case one is created from the client. The informers are started and
// synced before returning, so the call blocks until the cluster state has been read.
func NewSchedulingSimulator(
	ctx context.Context,
	cfg *schedulerapi.KubeSchedulerConfiguration,
	client ReadonlyClient,
	informerFactory informers.SharedInformerFactory,
) (*SchedulingSimulator, error) {
	if client.client == nil {
		return nil, fmt.Errorf("client needs to be provided, got nil")
	}

	framework.InitMetricsOnce()

	if informerFactory == nil {
		informerFactory = scheduler.NewInformerFactory(client.client, 0)
	}
	_ = informerFactory.Core().V1().Nodes().Informer()
	_ = informerFactory.Core().V1().Pods().Informer()
	informerFactory.StartWithContext(ctx)
	res := informerFactory.WaitForCacheSyncWithContext(ctx)
	if res.Err != nil {
		return nil, res.Err
	}

	return &SchedulingSimulator{
		cfg:             cfg,
		informerFactory: informerFactory,
		client:          client.client,
	}, nil
}

// NewClusterState initializes a new runtime cluster state.
func (s *SchedulingSimulator) NewClusterState(ctx context.Context) (*state.ClusterState, error) {
	snap := cache.NewEmptySnapshot()

	internalCache := cache.New(ctx, nil, false)
	profiles, err := s.buildProfileMap(ctx, snap)
	if err != nil {
		return nil, err
	}

	return state.New(internalCache, profiles, snap), nil
}

// NewClusterSnapshot initializes a new snapshot with the provided pods and nodes.
func (s *SchedulingSimulator) NewClusterSnapshot(ctx context.Context, pods []*v1.Pod, nodes []*v1.Node) (*snapshot.ClusterSnapshot, error) {
	snap := cache.NewSnapshot(pods, nodes)

	profiles, err := s.buildProfileMap(ctx, snap)
	if err != nil {
		return nil, err
	}

	return snapshot.New(snap, profiles), nil
}

func (s *SchedulingSimulator) buildProfileMap(ctx context.Context, snap *cache.Snapshot) (*upstreamsync.ProfileMap, error) {
	profiles, err := framework.NewProfileMap(ctx, s.client, s.informerFactory, snap, s.cfg)
	if err != nil {
		return nil, fmt.Errorf("schedlib: building scheduler: %w", err)
	}
	s.informerFactory.StartWithContext(ctx)
	res := s.informerFactory.WaitForCacheSyncWithContext(ctx)
	if res.Err != nil {
		return nil, res.Err
	}
	return profiles, nil
}
