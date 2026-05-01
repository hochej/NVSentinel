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

package reconciler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/breaker"
)

type recordingCircuitBreaker struct {
	checkCalled bool
}

func (b *recordingCircuitBreaker) AddCordonEvent(nodeName string) {}

func (b *recordingCircuitBreaker) AddCordonEventWithScope(nodeName string, inScope bool) {}

func (b *recordingCircuitBreaker) CheckCircuitBreakerForNode(
	ctx context.Context,
	nodeName string,
) (breaker.CheckResult, error) {
	b.checkCalled = true
	return breaker.CheckResult{}, nil
}

func (b *recordingCircuitBreaker) IsTripped(ctx context.Context) (bool, error) {
	return false, nil
}

func (b *recordingCircuitBreaker) ForceState(ctx context.Context, s breaker.State) error {
	return nil
}

func (b *recordingCircuitBreaker) CurrentState() breaker.State {
	return breaker.StateClosed
}

func (b *recordingCircuitBreaker) GetCursorMode(ctx context.Context) (breaker.CursorMode, error) {
	return breaker.CursorModeResume, nil
}

func (b *recordingCircuitBreaker) SetCursorMode(ctx context.Context, mode breaker.CursorMode) error {
	return nil
}

func TestCheckCircuitBreakerAndHaltBypassesForceQuarantine(t *testing.T) {
	cb := &recordingCircuitBreaker{}
	r := &Reconciler{
		config: ReconcilerConfig{CircuitBreakerEnabled: true},
		cb:     cb,
	}

	nodeInScope, shouldHalt := r.checkCircuitBreakerAndHalt(context.Background(), &protos.HealthEvent{
		NodeName:            "node-0",
		QuarantineOverrides: &protos.BehaviourOverrides{Force: true},
	})

	require.False(t, shouldHalt)
	require.False(t, nodeInScope)
	require.False(t, cb.checkCalled)
}
