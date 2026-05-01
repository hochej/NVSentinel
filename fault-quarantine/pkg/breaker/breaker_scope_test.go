// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
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

package breaker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/labels"
)

type memoryScopeClient struct {
	nodes  map[string]labels.Set
	state  State
	cursor CursorMode
}

func newMemoryScopeClient(nodes map[string]labels.Set) *memoryScopeClient {
	return &memoryScopeClient{
		nodes:  nodes,
		state:  StateClosed,
		cursor: CursorModeResume,
	}
}

func (c *memoryScopeClient) GetTotalNodes(ctx context.Context) (int, error) {
	return len(c.nodes), nil
}

func (c *memoryScopeClient) GetCircuitBreakerNodeScope(
	ctx context.Context,
	nodeName string,
	selector labels.Selector,
) (NodeScope, error) {
	if selector == nil {
		selector = labels.Everything()
	}

	scope := NodeScope{}
	for name, nodeLabels := range c.nodes {
		if !selector.Matches(nodeLabels) {
			continue
		}

		scope.ScopedNodeCount++
		if name == nodeName {
			scope.NodeInScope = true
		}
	}

	return scope, nil
}

func (c *memoryScopeClient) EnsureCircuitBreakerConfigMap(ctx context.Context, name, namespace string, initialStatus State) error {
	if c.state == "" {
		c.state = initialStatus
	}

	return nil
}

func (c *memoryScopeClient) ReadCircuitBreakerState(ctx context.Context, name, namespace string) (State, error) {
	return c.state, nil
}

func (c *memoryScopeClient) WriteCircuitBreakerState(ctx context.Context, name, namespace string, status State) error {
	c.state = status
	return nil
}

func (c *memoryScopeClient) ReadCursorMode(ctx context.Context, name, namespace string) (CursorMode, error) {
	return c.cursor, nil
}

func (c *memoryScopeClient) WriteCursorMode(ctx context.Context, name, namespace string, mode CursorMode) error {
	c.cursor = mode
	return nil
}

type flakyScopeClient struct {
	*memoryScopeClient
	remainingFailures int
}

func (c *flakyScopeClient) GetCircuitBreakerNodeScope(
	ctx context.Context,
	nodeName string,
	selector labels.Selector,
) (NodeScope, error) {
	if c.remainingFailures > 0 {
		c.remainingFailures--
		return NodeScope{}, errors.New("transient node scope failure")
	}

	return c.memoryScopeClient.GetCircuitBreakerNodeScope(ctx, nodeName, selector)
}

func newMemoryBreaker(t *testing.T, client K8sClientOperations, selector labels.Selector, percentage float64) CircuitBreaker {
	t.Helper()

	b, err := NewSlidingWindowBreaker(context.Background(), Config{
		Window:             time.Minute,
		TripPercentage:     percentage,
		K8sClient:          client,
		NodeSelector:       selector,
		ConfigMapName:      "test-circuit-breaker",
		ConfigMapNamespace: "default",
		MaxRetries:         1,
		InitialRetryDelay:  time.Millisecond,
		MaxRetryDelay:      time.Millisecond,
	})
	require.NoError(t, err)

	return b
}

func TestCircuitBreakerScopeIgnoresOutOfScopeEventsInMixedCluster(t *testing.T) {
	ctx := context.Background()
	gpuSelector := labels.SelectorFromSet(labels.Set{"nvidia.com/gpu.present": "true"})
	client := newMemoryScopeClient(map[string]labels.Set{
		"gpu-0": {"nvidia.com/gpu.present": "true"},
		"gpu-1": {"nvidia.com/gpu.present": "true"},
		"cpu-0": {},
		"cpu-1": {},
		"cpu-2": {},
		"cpu-3": {},
		"cpu-4": {},
		"cpu-5": {},
		"cpu-6": {},
		"cpu-7": {},
	})
	b := newMemoryBreaker(t, client, gpuSelector, 50)

	for _, nodeName := range []string{"cpu-0", "cpu-1", "cpu-2", "cpu-3", "cpu-4", "cpu-5", "cpu-6", "cpu-7"} {
		result, err := b.CheckCircuitBreakerForNode(ctx, nodeName)
		require.NoError(t, err)
		require.False(t, result.NodeInScope)
		require.Equal(t, 2, result.ScopedNodeCount)
		b.AddCordonEventWithScope(nodeName, result.NodeInScope)
	}

	tripped, err := b.IsTripped(ctx)
	require.NoError(t, err)
	require.False(t, tripped, "out-of-scope CPU events must not trip a GPU-scoped breaker")

	result, err := b.CheckCircuitBreakerForNode(ctx, "gpu-0")
	require.NoError(t, err)
	require.True(t, result.NodeInScope)
	b.AddCordonEventWithScope("gpu-0", result.NodeInScope)

	tripped, err = b.IsTripped(ctx)
	require.NoError(t, err)
	require.True(t, tripped, "one GPU event should trip a 50%% breaker scoped to 2 GPU nodes")
}

func TestCircuitBreakerEmptySelectorMeansAllNodes(t *testing.T) {
	ctx := context.Background()
	client := newMemoryScopeClient(map[string]labels.Set{
		"gpu-0": {"nvidia.com/gpu.present": "true"},
		"cpu-0": {},
	})
	b := newMemoryBreaker(t, client, labels.Everything(), 50)

	result, err := b.CheckCircuitBreakerForNode(ctx, "cpu-0")
	require.NoError(t, err)
	require.True(t, result.NodeInScope)
	require.Equal(t, 2, result.ScopedNodeCount)
}

func TestCircuitBreakerEmptyScopeReturnsSentinelError(t *testing.T) {
	ctx := context.Background()
	gpuSelector := labels.SelectorFromSet(labels.Set{"nvidia.com/gpu.present": "true"})
	client := newMemoryScopeClient(map[string]labels.Set{
		"cpu-0": {},
		"cpu-1": {},
	})
	b := newMemoryBreaker(t, client, gpuSelector, 50)

	tripped, err := b.IsTripped(ctx)
	require.False(t, tripped)
	require.True(t, errors.Is(err, ErrEmptyCircuitBreakerScope))
	require.Equal(t, StateClosed, b.CurrentState())
}

func TestCircuitBreakerEmptyScopeAllowsOutOfScopeNodeEvent(t *testing.T) {
	ctx := context.Background()
	gpuSelector := labels.SelectorFromSet(labels.Set{"nvidia.com/gpu.present": "true"})
	client := newMemoryScopeClient(map[string]labels.Set{
		"cpu-0": {},
	})
	b := newMemoryBreaker(t, client, gpuSelector, 50)

	result, err := b.CheckCircuitBreakerForNode(ctx, "cpu-0")
	require.NoError(t, err)
	require.False(t, result.Tripped)
	require.False(t, result.NodeInScope)
	require.Equal(t, 0, result.ScopedNodeCount)
}

func TestCircuitBreakerRetriesTransientNodeScopeErrors(t *testing.T) {
	ctx := context.Background()
	client := &flakyScopeClient{
		memoryScopeClient: newMemoryScopeClient(map[string]labels.Set{
			"node-0": {},
		}),
		remainingFailures: 1,
	}
	b := newMemoryBreaker(t, client, labels.Everything(), 50)

	result, err := b.CheckCircuitBreakerForNode(ctx, "node-0")
	require.NoError(t, err)
	require.False(t, result.Tripped)
	require.True(t, result.NodeInScope)
	require.Equal(t, 1, result.ScopedNodeCount)
}

func TestCheckCircuitBreakerForNodeSkipsScopeLookupWhenAlreadyTripped(t *testing.T) {
	ctx := context.Background()
	gpuSelector := labels.SelectorFromSet(labels.Set{"nvidia.com/gpu.present": "true"})
	client := newMemoryScopeClient(map[string]labels.Set{
		"gpu-0": {"nvidia.com/gpu.present": "true"},
		"cpu-0": {},
	})
	b := newMemoryBreaker(t, client, gpuSelector, 50)
	require.NoError(t, b.ForceState(ctx, StateTripped))

	result, err := b.CheckCircuitBreakerForNode(ctx, "gpu-0")
	require.NoError(t, err)
	require.True(t, result.Tripped)
	require.False(t, result.NodeInScope)
	require.Equal(t, 0, result.ScopedNodeCount)
}

func TestScopedEventStaysCountedAfterOutOfScopeEventForSameNode(t *testing.T) {
	ctx := context.Background()
	gpuSelector := labels.SelectorFromSet(labels.Set{"nvidia.com/gpu.present": "true"})
	client := newMemoryScopeClient(map[string]labels.Set{
		"gpu-0": {"nvidia.com/gpu.present": "true"},
		"gpu-1": {"nvidia.com/gpu.present": "true"},
	})
	b := newMemoryBreaker(t, client, gpuSelector, 50)

	b.AddCordonEventWithScope("gpu-0", true)
	b.AddCordonEventWithScope("gpu-0", false)

	tripped, err := b.IsTripped(ctx)
	require.NoError(t, err)
	require.True(t, tripped, "out-of-scope events must not remove existing in-scope events before expiry")
}
