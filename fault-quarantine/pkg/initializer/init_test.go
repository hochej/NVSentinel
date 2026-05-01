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

package initializer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/labels"
)

func TestParseCircuitBreakerNodeSelectorEmptyMeansAllNodes(t *testing.T) {
	selector, err := parseCircuitBreakerNodeSelector("")
	require.NoError(t, err)
	require.True(t, selector.Matches(labels.Set{}))
	require.True(t, selector.Matches(labels.Set{"nvidia.com/gpu.present": "true"}))
}

func TestParseCircuitBreakerNodeSelectorUsesKubernetesLabelSelectorSyntax(t *testing.T) {
	selector, err := parseCircuitBreakerNodeSelector("nvidia.com/gpu.present=true")
	require.NoError(t, err)
	require.True(t, selector.Matches(labels.Set{"nvidia.com/gpu.present": "true"}))
	require.False(t, selector.Matches(labels.Set{"nvidia.com/gpu.present": "false"}))
	require.False(t, selector.Matches(labels.Set{}))
}

func TestParseCircuitBreakerNodeSelectorRejectsInvalidSelector(t *testing.T) {
	_, err := parseCircuitBreakerNodeSelector("==!=foo")
	require.Error(t, err)
}
