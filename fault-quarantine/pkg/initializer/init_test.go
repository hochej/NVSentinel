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
)

func TestParseCircuitBreakerNodeSelector(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		wantErr  bool
		wantStr  string
		wantNil  bool
		matchKey string // expects the parsed selector to require this label key
	}{
		{
			name:    "empty falls back to default GPU selector",
			raw:     "",
			wantStr: DefaultCircuitBreakerNodeSelector,
		},
		{
			name:    "single label key",
			raw:     "nvidia.com/gpu.present",
			wantStr: "nvidia.com/gpu.present",
		},
		{
			name:    "kubectl style equality + set expression",
			raw:     "nvidia.com/gpu.present,!cpu-only",
			wantStr: "!cpu-only,nvidia.com/gpu.present",
		},
		{
			name:    "invalid selector returns error",
			raw:     "==nope==",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCircuitBreakerNodeSelector(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (selector=%q)", got)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got == nil {
				t.Fatalf("expected non-nil selector")
			}

			if got.String() != tc.wantStr {
				t.Fatalf("selector string mismatch: got %q, want %q", got.String(), tc.wantStr)
			}
		})
	}
}
