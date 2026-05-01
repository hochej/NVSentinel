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
	"k8s.io/apimachinery/pkg/labels"

	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/metrics"
)

const (
	resultError = "error"
)

var (
	// ErrRetryExhausted signals that GetTotalNodes retry attempts were exhausted
	// This error should trigger pod restart
	ErrRetryExhausted = errors.New("circuit breaker: all retry attempts exhausted")
	// ErrEmptyCircuitBreakerScope signals that the configured node selector currently
	// matches no nodes. This is a safe paused state, not a restart condition.
	ErrEmptyCircuitBreakerScope = errors.New("circuit breaker: node selector matches no nodes")
)

// NewSlidingWindowBreaker creates a new sliding window circuit breaker for fault quarantine.
// It prevents cordoning more than a specified percentage of nodes within a time window.
// The breaker uses a ring buffer with 1-second granularity to track unique cordoned nodes.
func NewSlidingWindowBreaker(ctx context.Context, cfg Config) (CircuitBreaker, error) {
	if cfg.NodeSelector == nil {
		cfg.NodeSelector = labels.Everything()
	}

	numBuckets := int((cfg.Window + time.Second - 1) / time.Second)
	b := &slidingWindowBreaker{
		cfg:          cfg,
		bucketSize:   time.Second,
		buckets:      make([]int, numBuckets),
		startTime:    time.Now(),
		state:        StateClosed,
		nodeToEvent:  make(map[string]cordonEvent),
		indexToNodes: make(map[int]map[string]struct{}),
	}

	// Initialize indexToNodes for all buckets
	for i := range numBuckets {
		b.indexToNodes[i] = make(map[string]struct{})
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

		b.indexToNodes[len(b.indexToNodes)-1] = make(map[string]struct{})

		// Update all node indices (decrement by 1)
		for nodeName, event := range b.nodeToEvent {
			event.bucketIndex--
			b.nodeToEvent[nodeName] = event
		}

		b.startTime = b.startTime.Add(b.bucketSize)
	}
}

// AddCordonEvent records a new in-scope node cordoning event in the sliding window.
// It advances the ring buffer to the current time and tracks the node uniquely
// within the sliding window. This method is thread-safe.
func (b *slidingWindowBreaker) AddCordonEvent(nodeName string) {
	b.AddCordonEventWithScope(nodeName, true)
}

// AddCordonEventWithScope records a node cordoning event with the node's circuit-breaker
// scope membership as observed before the quarantine action was applied. Only in-scope
// events contribute to breaker utilization. If a node has an existing in-scope event,
// a later out-of-scope event does not refresh or remove that in-scope event; it expires
// naturally with the original sliding-window bucket.
func (b *slidingWindowBreaker) AddCordonEventWithScope(nodeName string, inScope bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	b.slideWindow(now)

	if oldEvent, exists := b.nodeToEvent[nodeName]; exists {
		if !inScope {
			slog.Debug("Ignoring out-of-scope cordon event for node with existing in-scope breaker event",
				"node", nodeName,
				"bucket", oldEvent.bucketIndex)

			return
		}

		if oldBucketNodes, ok := b.indexToNodes[oldEvent.bucketIndex]; ok {
			delete(oldBucketNodes, nodeName)
		}

		b.buckets[oldEvent.bucketIndex]--
	}

	if !inScope {
		slog.Debug("Skipping out-of-scope cordon event", "node", nodeName)

		return
	}

	currentBucketIndex := len(b.buckets) - 1
	event := cordonEvent{bucketIndex: currentBucketIndex, inScope: true}

	slog.Debug("Adding node to current bucket",
		"node", nodeName,
		"bucket", currentBucketIndex,
		"inScope", inScope)

	b.nodeToEvent[nodeName] = event
	b.indexToNodes[currentBucketIndex][nodeName] = struct{}{}
	b.buckets[currentBucketIndex]++
}

// sumBucketsLocked calculates the total number of cordon events across all buckets
// in the sliding window. This method must be called with the mutex locked.
// Returns the sum of all bucket values representing recent cordon events.
func (b *slidingWindowBreaker) sumBuckets() int {
	sum := 0

	for _, v := range b.buckets {
		sum += v
	}

	return sum
}

// CheckCircuitBreakerForNode checks if the circuit breaker should prevent cordoning nodeName.
// It computes the scoped denominator and nodeName's selector membership from one informer
// cache snapshot, then evaluates the breaker using only in-scope cordon events.
func (b *slidingWindowBreaker) CheckCircuitBreakerForNode(ctx context.Context, nodeName string) (CheckResult, error) {
	return b.checkCircuitBreaker(ctx, nodeName)
}

// IsTripped checks if the circuit breaker should prevent further node cordoning.
// It returns true if:
//  1. The breaker is already in TRIPPED state, OR
//  2. Recent in-scope cordon events exceed the configured threshold
//     (TripPercentage * selected nodes).
//
// The method automatically trips the breaker if the threshold is exceeded.
func (b *slidingWindowBreaker) IsTripped(ctx context.Context) (bool, error) {
	result, err := b.checkCircuitBreaker(ctx, "")
	return result.Tripped, err
}

func (b *slidingWindowBreaker) checkCircuitBreaker(ctx context.Context, nodeName string) (CheckResult, error) {
	b.mu.RLock()
	alreadyTripped := b.state == StateTripped
	b.mu.RUnlock()

	if alreadyTripped {
		return CheckResult{Tripped: true}, nil
	}

	scope, err := b.getNodeScopeWithRetry(ctx, nodeName)
	if err != nil {
		if errors.Is(err, ErrEmptyCircuitBreakerScope) {
			return CheckResult{}, err
		}

		slog.ErrorContext(ctx, "Failed to get circuit breaker node scope after retries", "error", err)

		return CheckResult{}, fmt.Errorf("failed to get circuit breaker node scope after retries: %w", err)
	}

	if scope.ScopedNodeCount == 0 {
		return CheckResult{
			Tripped:         false,
			NodeInScope:     scope.NodeInScope,
			ScopedNodeCount: scope.ScopedNodeCount,
		}, nil
	}

	now := time.Now()

	b.mu.Lock()

	b.slideWindow(now)
	recentCordonedNodes := b.sumBuckets()
	threshold := int(math.Ceil(float64(scope.ScopedNodeCount) * b.cfg.TripPercentage / 100))
	shouldTrip := recentCordonedNodes >= threshold

	b.mu.Unlock()

	slog.DebugContext(ctx, "Recent scoped cordoned nodes status",
		"recentCordonedNodes", recentCordonedNodes,
		"scopedNodeCount", scope.ScopedNodeCount,
		"nodeInScope", scope.NodeInScope,
		"node", nodeName,
		"nodeSelector", b.cfg.NodeSelector.String(),
		"tripPercentage", b.cfg.TripPercentage)

	metrics.SetFaultQuarantineBreakerUtilization(float64(recentCordonedNodes) / float64(scope.ScopedNodeCount))

	result := CheckResult{
		Tripped:         shouldTrip,
		NodeInScope:     scope.NodeInScope,
		ScopedNodeCount: scope.ScopedNodeCount,
	}

	if shouldTrip {
		err := b.ForceState(ctx, StateTripped)
		if err != nil {
			slog.ErrorContext(ctx, "Error forcing circuit breaker state to TRIPPED", "error", err)
			return result, fmt.Errorf("error forcing circuit breaker state to TRIPPED: %w", err)
		}

		metrics.SetFaultQuarantineBreakerState(string(StateTripped))

		return result, nil
	}

	metrics.SetFaultQuarantineBreakerState(string(StateClosed))

	return result, nil
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

// getNodeScopeWithRetry gets the selected node count and optional node membership with
// retry logic and exponential backoff. For global checks (nodeName == ""), a selector
// that matches zero nodes is reported as ErrEmptyCircuitBreakerScope, not ErrRetryExhausted,
// so the reconciler can pause safely. Event-scoped checks for an out-of-scope node are
// allowed through even when the selector currently matches zero nodes.
func (b *slidingWindowBreaker) getNodeScopeWithRetry(ctx context.Context, nodeName string) (NodeScope, error) {
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

	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		scope, err := b.cfg.K8sClient.GetCircuitBreakerNodeScope(ctx, nodeName, b.cfg.NodeSelector)
		if err != nil {
			lastErr = err
			b.logGetNodeScopeError(err, attempt, maxRetries)
		} else if scope.ScopedNodeCount > 0 || nodeName != "" {
			result = "success"

			metrics.FaultQuarantineGetTotalNodesRetryAttempts.Observe(float64(attempt))

			return b.handleSuccessfulNodeScope(scope, attempt), nil
		} else {
			lastErr = nil

			if attempt == 0 {
				slog.InfoContext(ctx, "Circuit breaker starting retries: node selector currently matches 0 nodes",
					"maxRetries", maxRetries,
					"nodeSelector", b.cfg.NodeSelector.String())
			}
		}

		if attempt < maxRetries {
			reason := "node selector matched 0 nodes"
			if err != nil {
				reason = "node scope lookup failed"
			}

			if err := b.performRetryDelay(ctx, attempt, maxRetries, initialDelay, maxDelay, reason); err != nil {
				result = resultError
				errorType = "context_cancelled"

				return NodeScope{}, fmt.Errorf("context cancelled during circuit breaker node scope retry: %w", err)
			}
		}
	}

	result = resultError
	if lastErr != nil {
		errorType = "api_error"
		return NodeScope{}, b.logNodeScopeRetriesExhausted(ctx, maxRetries, initialDelay, maxDelay, lastErr)
	}

	errorType = "empty_scope"

	return NodeScope{}, b.logEmptyScopeAfterRetries(ctx, maxRetries, initialDelay, maxDelay)
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

// logGetNodeScopeError logs errors from GetCircuitBreakerNodeScope.
func (b *slidingWindowBreaker) logGetNodeScopeError(err error, attempt, maxRetries int) {
	slog.Error("GetCircuitBreakerNodeScope failed on attempt",
		"attempt", attempt+1,
		"maxAttempts", maxRetries+1,
		"error", err)
}

// handleSuccessfulNodeScope handles a successful scoped-node lookup.
func (b *slidingWindowBreaker) handleSuccessfulNodeScope(scope NodeScope, attempt int) NodeScope {
	if attempt > 0 {
		slog.Info("Circuit breaker retry successful",
			"scopedNodeCount", scope.ScopedNodeCount,
			"nodeInScope", scope.NodeInScope,
			"attempts", attempt+1)
	}

	return scope
}

// performRetryDelay calculates and performs the exponential backoff delay
func (b *slidingWindowBreaker) performRetryDelay(ctx context.Context, attempt, maxRetries int,
	initialDelay, maxDelay time.Duration, reason string) error {
	delay := b.calculateBackoffDelay(attempt, initialDelay, maxDelay)

	slog.DebugContext(ctx, "Circuit breaker retry",
		"reason", reason,
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

// logNodeScopeRetriesExhausted logs a summary when scope lookups keep failing after all retries.
func (b *slidingWindowBreaker) logNodeScopeRetriesExhausted(ctx context.Context, maxRetries int,
	initialDelay, maxDelay time.Duration, lastErr error) error {
	slog.ErrorContext(ctx, "Circuit breaker node scope lookup failed after all retry attempts",
		"maxRetries", maxRetries,
		"initialDelay", initialDelay,
		"maxDelay", maxDelay,
		"nodeSelector", b.cfg.NodeSelector.String(),
		"error", lastErr)

	return fmt.Errorf("%w: GetCircuitBreakerNodeScope failed after %d retries: %w",
		ErrRetryExhausted, maxRetries, lastErr)
}

// logEmptyScopeAfterRetries logs a summary when the configured selector matches no nodes
// after all retries. This is a paused state rather than a restart condition.
func (b *slidingWindowBreaker) logEmptyScopeAfterRetries(ctx context.Context, maxRetries int,
	initialDelay, maxDelay time.Duration) error {
	slog.WarnContext(ctx, "Circuit breaker node selector matched 0 nodes after all retry attempts; pausing event processing",
		"maxRetries", maxRetries,
		"initialDelay", initialDelay,
		"maxDelay", maxDelay,
		"nodeSelector", b.cfg.NodeSelector.String())

	metrics.SetFaultQuarantineBreakerState(string(StateScopeEmpty))

	return fmt.Errorf("%w: selector %q matched 0 nodes after %d retries",
		ErrEmptyCircuitBreakerScope, b.cfg.NodeSelector.String(), maxRetries)
}
