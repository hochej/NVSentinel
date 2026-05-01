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

// Package breaker provides types and interfaces for the fault quarantine circuit breaker.
package breaker

import (
	"context"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/labels"
)

type CursorMode string

const (
	CursorModeResume CursorMode = "RESUME"
	CursorModeCreate CursorMode = "CREATE"
)

// K8sClientOperations defines the minimal interface needed by the circuit breaker
type K8sClientOperations interface {
	GetCircuitBreakerNodeScope(ctx context.Context, nodeName string, selector labels.Selector) (NodeScope, error)
	EnsureCircuitBreakerConfigMap(ctx context.Context, name, namespace string, initialStatus State) error
	ReadCircuitBreakerState(ctx context.Context, name, namespace string) (State, error)
	WriteCircuitBreakerState(ctx context.Context, name, namespace string, status State) error
	ReadCursorMode(ctx context.Context, name, namespace string) (CursorMode, error)
	WriteCursorMode(ctx context.Context, name, namespace string, mode CursorMode) error
}

// NodeScope is a snapshot of circuit-breaker node-selector scope.
type NodeScope struct {
	ScopedNodeCount int
	NodeInScope     bool
}

// CheckResult contains the outcome of an event-scoped circuit-breaker check.
type CheckResult struct {
	Tripped         bool
	NodeInScope     bool
	ScopedNodeCount int
}

// CircuitBreakerConfig holds the Kubernetes-specific configuration for the circuit breaker
type CircuitBreakerConfig struct {
	Namespace    string
	Name         string
	Percentage   int
	Duration     time.Duration
	NodeSelector labels.Selector
}

// State represents the current state of the circuit breaker
type State string

const (
	// StateClosed indicates the breaker is allowing operation
	StateClosed State = "CLOSED"
	// StateTripped indicates the breaker is blocking operations
	StateTripped State = "TRIPPED"
	// StateScopeEmpty indicates the configured selector currently matches no nodes.
	// It is exposed via metrics only and is not persisted in the ConfigMap.
	StateScopeEmpty State = "SCOPE_EMPTY"
)

type CircuitBreaker interface {
	// AddCordonEvent records an in-scope node cordoning event in the sliding window
	AddCordonEvent(nodeName string)
	// AddCordonEventWithScope records a node cordoning event with its selector membership.
	AddCordonEventWithScope(nodeName string, inScope bool)
	// CheckCircuitBreakerForNode checks if the breaker should prevent cordoning nodeName
	// and returns nodeName's circuit-breaker scope membership from the same cache snapshot
	// used to compute the scoped denominator.
	CheckCircuitBreakerForNode(ctx context.Context, nodeName string) (CheckResult, error)
	// IsTripped checks if the breaker should prevent further cordoning
	IsTripped(ctx context.Context) (bool, error)
	// ForceState manually sets the breaker state (CLOSED or TRIPPED)
	ForceState(ctx context.Context, s State) error
	// CurrentState returns the current breaker state
	CurrentState() State

	GetCursorMode(ctx context.Context) (CursorMode, error)
	SetCursorMode(ctx context.Context, mode CursorMode) error
}

// Config holds the configuration parameters for the sliding window circuit breaker.
// It defines the time window, trip threshold, and K8s client for state persistence.
type Config struct {
	// Window defines the sliding time window over which cordon events are counted.
	// Default: 5 minutes. Events older than this window are automatically discarded.
	Window time.Duration

	// TripPercentage is the fraction of selected nodes that, if exceeded by recent
	// in-scope cordon events within Window, will trip the breaker (e.g., 50 for 50%).
	// Default: 50 (50% of selected nodes).
	TripPercentage float64

	// K8sClient provides operations for node counts and ConfigMap state persistence
	K8sClient K8sClientOperations

	// NodeSelector selects the nodes that participate in circuit-breaker accounting.
	// Empty/nil means all nodes.
	NodeSelector labels.Selector

	// ConfigMapName is the name of the ConfigMap used for state persistence
	ConfigMapName string

	// ConfigMapNamespace is the namespace of the ConfigMap
	ConfigMapNamespace string

	// MaxRetries is the maximum number of retry attempts when the node selector matches 0 nodes.
	// Default: 10 retries (allows ~30 seconds for cache sync with exponential backoff)
	MaxRetries int

	// InitialRetryDelay is the base delay for the first retry attempt
	// Default: 100ms, exponentially increases with each retry
	InitialRetryDelay time.Duration

	// MaxRetryDelay caps the maximum delay between retry attempts
	// Default: 5 seconds (prevents excessive delays)
	MaxRetryDelay time.Duration
}

// slidingWindowBreaker implements CircuitBreaker using a ring buffer approach.
// It tracks unique nodes that have been cordoned in time-based buckets and automatically
// expires old events as the window slides forward.
type slidingWindowBreaker struct {
	// cfg holds the breaker configuration (window size, trip ratio, etc.)
	cfg Config
	// mu protects all mutable fields from concurrent access
	mu sync.RWMutex

	// Ring buffer implementation for sliding window
	// bucketSize defines the duration represented by each bucket (fixed at 1 second)
	bucketSize time.Duration
	// buckets holds per-bucket counts of recent cordon events forming a ring buffer
	// For a 5-minute window, this contains 300 buckets (5 * 60 seconds)
	buckets []int
	// startTime marks the time corresponding to buckets[0]; used to advance the ring
	// as time progresses, ensuring the window slides correctly
	startTime time.Time

	// Node tracking for unique cordon events within the sliding window
	// nodeToEvent maps node name to the bucket index where it was last cordoned and
	// whether that event was in the configured circuit-breaker node scope.
	nodeToEvent map[string]cordonEvent
	// indexToNodes maps bucket index to the node names recorded in that bucket.
	indexToNodes map[int]map[string]struct{}

	// state is the current breaker state (CLOSED or TRIPPED)
	// Can be manually forced via ForceState() or automatically set by IsTripped()
	state State
}

type cordonEvent struct {
	bucketIndex int
	inScope     bool
}
