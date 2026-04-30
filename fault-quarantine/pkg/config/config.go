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

package config

import "strings"

const (
	// CircuitBreakerScopeGPU limits circuit breaker calculations to GPU nodes.
	CircuitBreakerScopeGPU = "gpu"
	// CircuitBreakerScopeAll uses all Kubernetes nodes for circuit breaker calculations.
	CircuitBreakerScopeAll = "all"
)

type Rule struct {
	Kind       string `toml:"kind"`
	Expression string `toml:"expression"`
}

type Taint struct {
	Key    string `toml:"key"`
	Value  string `toml:"value"`
	Effect string `toml:"effect"`
}

type Cordon struct {
	ShouldCordon bool `toml:"shouldCordon"`
}

type CircuitBreaker struct {
	Percentage int    `toml:"percentage"`
	Duration   string `toml:"duration"`
	Scope      string `toml:"scope"`
}

// EffectiveScope returns the configured circuit breaker node scope.
// Empty scope defaults to all nodes, preserving the pre-scope circuit breaker behavior.
func (cb CircuitBreaker) EffectiveScope() string {
	scope := strings.ToLower(strings.TrimSpace(cb.Scope))
	if scope == "" {
		return CircuitBreakerScopeAll
	}

	return scope
}

type Match struct {
	Any []Rule `toml:"any"`
	All []Rule `toml:"all"`
}

type RuleSet struct {
	Enabled  bool   `toml:"enabled"`
	Version  string `toml:"version"`
	Name     string `toml:"name"`
	Priority int    `toml:"priority"`
	Match    Match  `toml:"match"`
	Taint    Taint  `toml:"taint"`
	Cordon   Cordon `toml:"cordon"`
}

type TomlConfig struct {
	LabelPrefix    string         `toml:"label-prefix"`
	CircuitBreaker CircuitBreaker `toml:"circuitBreaker"`
	RuleSets       []RuleSet      `toml:"rule-sets"`
}
