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

	"sigs.k8s.io/scheduler-library/pkg/snapshot"
	"sigs.k8s.io/scheduler-library/pkg/state"
	upstreamsync "sigs.k8s.io/scheduler-library/pkg/upstream_sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/profile"
)

type SchedulingSimulator struct {
	cfg             *schedulerapi.KubeSchedulerConfiguration
	recorderFactory profile.RecorderFactory
	informerFactory informers.SharedInformerFactory
}

// NewSchedulingSimulator creates a new SchedulingSimulator.
func NewSchedulingSimulator(
	ctx context.Context,
	cfg *schedulerapi.KubeSchedulerConfiguration,
	informerFactory informers.SharedInformerFactory,
) (*SchedulingSimulator, error) {
	recorderFactory := func(name string) events.EventRecorderLogger {
		return &discardEventRecorder{}
	}

	if informerFactory == nil {
		informerFactory = informers.NewSharedInformerFactory(nil, 0)
	}

	return &SchedulingSimulator{
		cfg:             cfg,
		recorderFactory: recorderFactory,
		informerFactory: informerFactory,
	}, nil
}

// NewClusterState initializes a new runtime cluster state.
func (s *SchedulingSimulator) NewClusterState(ctx context.Context) (*state.ClusterState, error) {
	snap := cache.NewEmptySnapshot()

	sched, err := s.buildScheduler(ctx, snap)
	if err != nil {
		return nil, err
	}

	internalCache := cache.New(ctx, nil, false)

	upstreamSched := upstreamsync.NewScheduler(sched, snap)
	return state.New(internalCache, upstreamSched, snap), nil
}

// NewClusterSnapshot initializes a new snapshot with the provided pods and nodes.
func (s *SchedulingSimulator) NewClusterSnapshot(ctx context.Context, pods []*v1.Pod, nodes []*v1.Node) (*snapshot.ClusterSnapshot, error) {
	snap := cache.NewSnapshot(pods, nodes)

	sched, err := s.buildScheduler(ctx, snap)
	if err != nil {
		return nil, err
	}

	upstreamSched := upstreamsync.NewScheduler(sched, snap)
	return snapshot.New(snap, upstreamSched), nil
}

func (s *SchedulingSimulator) buildScheduler(ctx context.Context, snap *cache.Snapshot) (*scheduler.Scheduler, error) {
	sched, err := scheduler.New(ctx, nil, s.informerFactory, nil, s.recorderFactory,
		scheduler.WithProfiles(s.cfg.Profiles...),
		scheduler.WithNodeInfoSnapshot(snap),
		scheduler.WithPercentageOfNodesToScore(ptr.To[int32](100)),
	)
	if err != nil {
		return nil, fmt.Errorf("schedlib: building scheduler: %w", err)
	}
	return sched, nil
}

type discardEventRecorder struct{}

var _ events.EventRecorderLogger = &discardEventRecorder{}

func (d *discardEventRecorder) Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...interface{}) {
}

func (d *discardEventRecorder) WithLogger(logger klog.Logger) events.EventRecorderLogger {
	return d
}
