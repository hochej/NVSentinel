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

// Package breaker implements a sliding window circuit breaker for fault quarantine.
// It prevents excessive node cordoning that could destabilize Kubernetes clusters
// by tracking cordon events over time and blocking further operations when thresholds are exceeded.
//
// The implementation uses a ring buffer to efficiently track events within a sliding time window,
// providing predictable performance and memory usage regardless of cluster activity levels.
package breaker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"golang.org/x/exp/maps"

	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/metrics"
)

const (
	resultError = "error"
)

var (
	// ErrRetryExhausted signals that node count retry attempts were exhausted.
	// This error should trigger pod restart.
	ErrRetryExhausted = errors.New("circuit breaker: all retry attempts exhausted")

	// ErrEmptyCircuitBreakerScope signals that the configured circuit breaker scope
	// currently has no nodes. Quarantine should fail closed, but this is not a
	// cache-sync failure and should not force a restart loop.
	ErrEmptyCircuitBreakerScope = errors.New("circuit breaker: empty node scope")
)

// NewSlidingWindowBreaker creates a new sliding window circuit breaker for fault quarantine.
// It prevents cordoning more than a specified percentage of nodes within a time window.
// The breaker uses a ring buffer with 1-second granularity to track unique cordoned nodes.
func NewSlidingWindowBreaker(ctx context.Context, cfg Config) (CircuitBreaker, error) {
	numBuckets := int((cfg.Window + time.Second - 1) / time.Second)
	b := &slidingWindowBreaker{
		cfg:          cfg,
		bucketSize:   time.Second,
		buckets:      make([]int, numBuckets),
		startTime:    time.Now(),
		state:        StateClosed,
		nodeToEvent:  make(map[string]cordonEvent),
		indexToNodes: make(map[int]map[string]bool),
	}

	// Initialize indexToNodes for all buckets
	for i := range numBuckets {
		b.indexToNodes[i] = make(map[string]bool)
	}

	err := cfg.K8sClient.EnsureCircuitBreakerConfigMap(ctx, cfg.ConfigMapName, cfg.ConfigMapNamespace, StateClosed)
	if err != nil {
		slog.ErrorContext(ctx, "Error ensuring circuit breaker config map", "error", err)
		return nil, fmt.Errorf("error ensuring circuit breaker config map: %w", err)
	}

	state, err := cfg.K8sClient.ReadCircuitBreakerState(ctx, cfg.ConfigMapName, cfg.ConfigMapNamespace)
	if err == nil {
		if state == StateClosed || state == StateTripped {
			b.state = state
		}
	}

	return b, nil
}

// slideWindowToCurrentTimeLocked advances the ring buffer to the current time by shifting buckets.
// This method must be called with the mutex locked. It calculates elapsed time since
// the last update and shifts the ring buffer accordingly, clearing old buckets and node mappings.
func (b *slidingWindowBreaker) slideWindow(now time.Time) {
	elapsed := now.Sub(b.startTime)
	if elapsed <= 0 {
		return
	}

	steps := int(elapsed / b.bucketSize)
	if steps >= len(b.buckets) {
		// If we've elapsed more than the entire window, clear everything
		for i := range b.buckets {
			b.buckets[i] = 0
		}

		maps.Clear(b.nodeToEvent)

		for i := range b.indexToNodes {
			maps.Clear(b.indexToNodes[i])
		}

		b.startTime = now.Truncate(b.bucketSize)

		return
	}

	for range steps {
		// Clean up node mappings for the bucket being shifted out (bucket 0)
		if expiredNodes, ok := b.indexToNodes[0]; ok {
			for nodeName := range expiredNodes {
				delete(b.nodeToEvent, nodeName)
			}
		}

		// Shift ring buffer by one bucket
		copy(b.buckets, b.buckets[1:])
		b.buckets[len(b.buckets)-1] = 0

		// Shift node mappings
		for i := range len(b.indexToNodes) - 1 {
			b.indexToNodes[i] = b.indexToNodes[i+1]
		}

		b.indexToNodes[len(b.indexToNodes)-1] = make(map[string]bool)

		// Update all node indices (decrement by 1)
		for nodeName, event := range b.nodeToEvent {
			event.bucketIndex--
			b.nodeToEvent[nodeName] = event
		}

		b.startTime = b.startTime.Add(b.bucketSize)
	}
}

// AddCordonEvent records a new node cordoning event in the sliding window.
// It advances the ring buffer to the current time and tracks the node uniquely
// within the sliding window. Scope membership is captured at cordon time so
// later node deletion or label/capacity changes do not remove the event from
// the numerator before it expires. This method is thread-safe.
func (b *slidingWindowBreaker) AddCordonEvent(nodeName string, inCircuitBreakerScope bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	b.slideWindow(now)

	currentBucketIndex := len(b.buckets) - 1

	eventInCircuitBreakerScope := inCircuitBreakerScope

	// Check if this node was already cordoned in the current window
	if oldEvent, exists := b.nodeToEvent[nodeName]; exists {
		// Preserve a scoped cordon until its original event expires. A later
		// out-of-scope observation must not extend the scoped numerator window.
		if oldEvent.inCircuitBreakerScope && !inCircuitBreakerScope {
			return
		}

		// Node was already cordoned in this window, remove from old bucket
		if oldBucketNodes, ok := b.indexToNodes[oldEvent.bucketIndex]; ok {
			delete(oldBucketNodes, nodeName)

			b.buckets[oldEvent.bucketIndex]--
		}
	}

	slog.Debug("Adding node to current bucket",
		"node", nodeName,
		"bucket", currentBucketIndex,
		"inCircuitBreakerScope", eventInCircuitBreakerScope)
	// Add node to current bucket
	b.nodeToEvent[nodeName] = cordonEvent{
		bucketIndex:           currentBucketIndex,
		inCircuitBreakerScope: eventInCircuitBreakerScope,
	}
	b.indexToNodes[currentBucketIndex][nodeName] = true
	b.buckets[currentBucketIndex]++
}

// countRecentScopedCordons counts unique recent cordon events that were in the
// configured circuit breaker scope when they were recorded.
func (b *slidingWindowBreaker) countRecentScopedCordons() int {
	count := 0

	for _, event := range b.nodeToEvent {
		if event.inCircuitBreakerScope {
			count++
		}
	}

	return count
}

// IsTripped checks if the circuit breaker should prevent further node cordoning.
// It returns true if:
// 1. The breaker is already in TRIPPED state, OR
// 2. Recent scoped cordon events exceed the configured threshold (TripPercentage * scoped nodes)
// The method automatically trips the breaker if the threshold is exceeded.
func (b *slidingWindowBreaker) IsTripped(ctx context.Context) (bool, error) {
	b.mu.RLock()

	if b.state == StateTripped {
		b.mu.RUnlock()

		return true, nil
	}

	b.mu.RUnlock()

	circuitBreakerNodeNames, err := b.getCircuitBreakerNodeNamesWithRetry(ctx)
	if err != nil {
		if errors.Is(err, ErrEmptyCircuitBreakerScope) {
			slog.WarnContext(ctx, "Circuit breaker node scope is empty", "error", err)
			metrics.SetFaultQuarantineBreakerState(string(StateClosed))
			metrics.SetFaultQuarantineBreakerUtilization(0)
			return false, err
		}

		slog.ErrorContext(ctx, "Failed to get circuit breaker nodes", "error", err)

		return false, fmt.Errorf("failed to get circuit breaker nodes: %w", err)
	}

	totalNodes := len(circuitBreakerNodeNames)
	if totalNodes == 0 {
		slog.WarnContext(ctx, "Circuit breaker node scope is empty")
		return false, ErrEmptyCircuitBreakerScope
	}

	now := time.Now()

	b.mu.Lock()

	b.slideWindow(now)
	recentCordonedNodes := b.countRecentScopedCordons()
	threshold := int(math.Ceil(float64(totalNodes) * b.cfg.TripPercentage / 100))
	shouldTrip := recentCordonedNodes >= threshold

	b.mu.Unlock()

	slog.DebugContext(ctx, "Recent cordoned nodes status",
		"recentCordonedNodes", recentCordonedNodes,
		"totalNodes", totalNodes,
		"tripPercentage", b.cfg.TripPercentage)

	metrics.SetFaultQuarantineBreakerUtilization(float64(recentCordonedNodes) / float64(totalNodes))

	if shouldTrip {
		err := b.ForceState(ctx, StateTripped)
		if err != nil {
			slog.ErrorContext(ctx, "Error forcing circuit breaker state to TRIPPED", "error", err)
			return true, fmt.Errorf("error forcing circuit breaker state to TRIPPED: %w", err)
		}

		metrics.SetFaultQuarantineBreakerState(string(StateTripped))

		return true, nil
	}

	metrics.SetFaultQuarantineBreakerState(string(StateClosed))

	return false, nil
}

// ForceState manually sets the circuit breaker state to CLOSED or TRIPPED.
// This bypasses the normal threshold checking and directly controls the breaker state.
// If a WriteStateFn is configured, it persists the state change. This method is thread-safe.
func (b *slidingWindowBreaker) ForceState(ctx context.Context, s State) error {
	b.mu.Lock()
	b.state = s
	b.mu.Unlock()

	err := b.cfg.K8sClient.WriteCircuitBreakerState(
		ctx, b.cfg.ConfigMapName, b.cfg.ConfigMapNamespace, s)
	if err != nil {
		slog.ErrorContext(ctx, "Error writing circuit breaker state", "error", err)
		return fmt.Errorf("error writing circuit breaker state: %w", err)
	}

	slog.InfoContext(ctx, "ForceState changed", "state", s)

	return nil
}

// CurrentState returns the current state of the circuit breaker (CLOSED or TRIPPED).
// This method is thread-safe and provides read-only access to the breaker state.
func (b *slidingWindowBreaker) CurrentState() State {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.state
}

func (b *slidingWindowBreaker) GetCursorMode(ctx context.Context) (CursorMode, error) {
	return b.cfg.K8sClient.ReadCursorMode(ctx, b.cfg.ConfigMapName, b.cfg.ConfigMapNamespace)
}

func (b *slidingWindowBreaker) SetCursorMode(ctx context.Context, mode CursorMode) error {
	return b.cfg.K8sClient.WriteCursorMode(ctx, b.cfg.ConfigMapName, b.cfg.ConfigMapNamespace, mode)
}

// getCircuitBreakerNodeNamesWithRetry gets the circuit breaker node set.
// An empty synced scope is a stable fail-closed condition, not a retry-exhaustion restart signal.
func (b *slidingWindowBreaker) getCircuitBreakerNodeNamesWithRetry(ctx context.Context) (map[string]bool, error) {
	startTime := time.Now()

	var result string

	var errorType string

	defer func() {
		duration := time.Since(startTime).Seconds()
		metrics.FaultQuarantineGetTotalNodesDuration.WithLabelValues(result).Observe(duration)

		if errorType != "" {
			metrics.FaultQuarantineGetTotalNodesErrors.WithLabelValues(errorType).Inc()
		}
	}()

	maxRetries, initialDelay, maxDelay := b.getRetryConfig()

	for attempt := 0; attempt <= maxRetries; attempt++ {
		nodeNames, err := b.cfg.K8sClient.GetCircuitBreakerNodeNames(ctx)
		if err != nil {
			result = resultError
			errorType = "api_error"

			wrappedErr := b.handleGetCircuitBreakerNodeNamesError(err, attempt, maxRetries)
			if attempt == maxRetries {
				return nil, fmt.Errorf("%w: GetCircuitBreakerNodeNames failed after %d attempts: %w",
					ErrRetryExhausted, attempt+1, wrappedErr)
			}

			if err := b.performRetryDelay(ctx, attempt, maxRetries, initialDelay, maxDelay); err != nil {
				errorType = "context_cancelled"
				return nil, fmt.Errorf("context cancelled during circuit breaker node retry: %w", err)
			}

			continue
		}

		if len(nodeNames) == 0 {
			result = resultError
			errorType = "empty_scope"

			slog.WarnContext(ctx, "Circuit breaker node scope is empty; blocking quarantine until at least one scoped node is visible")

			return nil, ErrEmptyCircuitBreakerScope
		}

		result = "success"
		errorType = ""
		metrics.FaultQuarantineGetTotalNodesRetryAttempts.Observe(float64(attempt))

		return b.handleSuccessfulCircuitBreakerNodeNames(nodeNames, attempt)
	}

	result = resultError
	return nil, fmt.Errorf("%w: circuit breaker node retry loop exited unexpectedly", ErrRetryExhausted)
}

// getRetryConfig extracts and validates retry configuration with defaults
func (b *slidingWindowBreaker) getRetryConfig() (int, time.Duration, time.Duration) {
	maxRetries := b.cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 10 // Default: 10 retries
	}

	initialDelay := b.cfg.InitialRetryDelay
	if initialDelay <= 0 {
		initialDelay = 100 * time.Millisecond // Default: 100ms
	}

	maxDelay := b.cfg.MaxRetryDelay
	if maxDelay <= 0 {
		maxDelay = 5 * time.Second // Default: 5 seconds
	}

	return maxRetries, initialDelay, maxDelay
}

// handleGetCircuitBreakerNodeNamesError handles API errors from GetCircuitBreakerNodeNames.
func (b *slidingWindowBreaker) handleGetCircuitBreakerNodeNamesError(err error, attempt, maxRetries int) error {
	slog.Error("GetCircuitBreakerNodeNames failed on attempt",
		"attempt", attempt+1,
		"maxAttempts", maxRetries+1,
		"error", err)

	return fmt.Errorf("GetCircuitBreakerNodeNames failed: %w", err)
}

// handleSuccessfulCircuitBreakerNodeNames handles the success case when nodes > 0.
func (b *slidingWindowBreaker) handleSuccessfulCircuitBreakerNodeNames(
	nodeNames map[string]bool,
	attempt int,
) (map[string]bool, error) {
	if attempt > 0 {
		slog.Info("Circuit breaker retry successful",
			"circuitBreakerNodes", len(nodeNames),
			"attempts", attempt+1)
	}

	return nodeNames, nil
}

// performRetryDelay calculates and performs the exponential backoff delay
func (b *slidingWindowBreaker) performRetryDelay(ctx context.Context, attempt, maxRetries int,
	initialDelay, maxDelay time.Duration) error {
	delay := b.calculateBackoffDelay(attempt, initialDelay, maxDelay)

	slog.DebugContext(ctx, "Circuit breaker retry; got 0 circuit breaker nodes, retrying (NodeInformer cache may still be syncing)",
		"attempt", attempt+1,
		"maxRetries", maxRetries,
		"delay", delay)

	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled during retry: %w", ctx.Err())
	case <-time.After(delay):
	}

	return nil
}

// calculateBackoffDelay calculates exponential backoff delay with overflow protection
func (b *slidingWindowBreaker) calculateBackoffDelay(attempt int,
	initialDelay, maxDelay time.Duration) time.Duration {
	if attempt > 30 || attempt < 0 { // Prevent overflow for very large or negative attempts
		return maxDelay
	}

	// Safe conversion: attempt is guaranteed to be [0, 30] at this point
	safeAttempt := uint(attempt)
	multiplier := int64(1 << safeAttempt) // 2^attempt as integer
	delay := time.Duration(int64(initialDelay) * multiplier)

	if delay > maxDelay || delay < 0 { // Check for overflow
		delay = maxDelay
	}

	return delay
}
