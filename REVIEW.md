# Review: `hochej/NVSentinel@circuit-breaker-node-selector-scoped`

## Strengths (calibration, not findings)

This branch is the most coherent of the three. It combines the right pieces from the other two:

- **Free-form `labels.Selector`** (like the local branch) — usable on non-Nvidia clusters and customer-specific labels, no closed enum.
- **Scope captured at cordon time** via `cordonEvent.inScope` (like branch 2) — the numerator/denominator asymmetry the local branch suffers from is fixed.
- **`CheckCircuitBreakerForNode(ctx, nodeName)` returns `{Tripped, NodeInScope, ScopedNodeCount}` from a single cache snapshot.** That value is then threaded through `handleEvent → applyQuarantine → recordCordonEventInCircuitBreaker(event, nodeInCircuitBreakerScope)`. This is exactly what branch 2 was missing: the membership decision used to authorize the cordon is the same one recorded for the numerator. The TOCTOU and "halt-on-second-lookup" hazards I called out in branch 2 are gone.
- **`ErrEmptyCircuitBreakerScope` + reconciler retry-with-30s-poll** instead of the indefinite halt or the pod restart loop the other two pick.
- **`StateScopeEmpty` metric** so dashboards can distinguish "paused, no eligible nodes" from "CLOSED, healthy".
- **Empty selector → `labels.Everything()`** as documented in `values.yaml` ("Set to `""` to include all nodes"). The doc-vs-code mismatch from the local branch is gone.
- **Tests cover the regression scenarios:** mixed cluster CPU events not tripping a GPU breaker; in-scope event preserved across a later out-of-scope event; empty-selector → sentinel error.

That said, several issues remain.

## Findings

### [P1] When the breaker is in `StateTripped`, `CheckCircuitBreakerForNode` returns a zero-valued `NodeInScope` that the reconciler will record on the cordon-event path
**File:** `fault-quarantine/pkg/breaker/breaker.go:240-244` and `pkg/reconciler/reconciler.go:519-526`

```go
if b.state == StateTripped {
    b.mu.RUnlock()
    return CheckResult{Tripped: true}, nil   // NodeInScope=false, ScopedNodeCount=0
}
```

The reconciler then does:

```go
if result.Tripped {
    ...
    <-ctx.Done()
    return result.NodeInScope, true
}
```

In the trip path the caller blocks on `ctx.Done()` and never reaches `recordCordonEventInCircuitBreaker`, so today this is benign — but the API contract "returns `NodeInScope` from the same snapshot" is silently violated for tripped state. Any future caller that uses `result.NodeInScope` without first checking `result.Tripped` will see a wrong value. Either compute scope membership before the tripped short-circuit, or document the field as undefined when `Tripped == true` and have the test enforce it.

### [P1] `if scope.ScopedNodeCount == 0` branch in `checkCircuitBreaker` is dead code; the only signal of empty-scope is the wrapped sentinel from `getNodeScopeWithRetry`
**File:** `fault-quarantine/pkg/breaker/breaker.go:257-260`

```go
scope, err := b.getNodeScopeWithRetry(ctx, nodeName)
if err != nil { ... }

if scope.ScopedNodeCount == 0 {
    metrics.SetFaultQuarantineBreakerState(string(StateScopeEmpty))
    return CheckResult{}, ErrEmptyCircuitBreakerScope
}
```

`getNodeScopeWithRetry` only returns `(NodeScope{}, nil)` if `scope.ScopedNodeCount > 0` (the success branch). When empty after retries it returns a wrapped `ErrEmptyCircuitBreakerScope` and is caught by the `if err != nil` block above. The `scope.ScopedNodeCount == 0` block is therefore unreachable. Either remove it, or replace the unreachable check with an invariant assertion. As-is it gives a false sense that there are two empty-scope handling paths.

### [P1] `cordonEvent` value is stored in `indexToNodes` but the embedded `bucketIndex` goes stale after `slideWindow`
**File:** `fault-quarantine/pkg/breaker/breaker.go:130-139,196-198` and `pkg/breaker/types.go:151-153`

`indexToNodes` was changed from `map[int]map[string]bool` to `map[int]map[string]cordonEvent`. The value's `bucketIndex` is updated in `slideWindow` only on `nodeToEvent`:

```go
for nodeName, event := range b.nodeToEvent {
    event.bucketIndex--
    b.nodeToEvent[nodeName] = event
}
```

`indexToNodes[i][nodeName].bucketIndex` is never updated, so it's permanently stale after the first slide. Today no code reads that field from `indexToNodes` (only the keys are used), so the bug is latent — but the type change costs memory for no benefit and is a clear footgun for the next maintainer who naively reads `event.bucketIndex` from `indexToNodes`. Revert to `map[int]map[string]struct{}` (or `bool`) and only carry the rich `cordonEvent` in `nodeToEvent`.

### [P2] Helm chart no longer wraps with `| default ""`; non-string values from operators render as Go-printed scalars into TOML
**File:** `distros/kubernetes/nvsentinel/charts/fault-quarantine/templates/configmap.yaml:29`

```yaml
nodeSelector = {{ .Values.circuitBreaker.nodeSelector | quote }}
```

The previous version had `| default "" | quote`. If an operator sets `nodeSelector: null` (or YAML mis-types it as a map / sequence), `quote` of nil renders `""` but `quote` of a map will render `"map[…]"` and a list will render `"[…]"`, producing a TOML string the Go code will then fail to parse with a confusing error pointing at the runtime side rather than the Helm input. Restore `| default "" |` and consider a `fail` if non-string types are detected to surface the misconfiguration at template time.

### [P2] `K8sClientOperations` interface keeps `GetTotalNodes` though production code no longer calls it
**File:** `fault-quarantine/pkg/breaker/types.go:35`

The breaker's only node-counting path is now `GetCircuitBreakerNodeScope`. `GetTotalNodes` survives on the interface and forces every mock (production and tests) to implement it. Either drop it from the interface (clean) or convert it to a backwards-compat alias. While here, the metric names `FaultQuarantineGetTotalNodes{Duration,Errors,RetryAttempts}` are now misleading — they cover `GetCircuitBreakerNodeScope` calls. Either rename them, or add new metric names and keep the old ones with `0` so existing dashboards/alerts don't go quiet.

### [P2] `checkCircuitBreakerAndHalt` returns `(true, false)` when the breaker is disabled — magic placeholder
**File:** `fault-quarantine/pkg/reconciler/reconciler.go:493-495`

```go
if !r.config.CircuitBreakerEnabled {
    return true, false
}
```

`nodeInCircuitBreakerScope=true` is a sentinel meaning "doesn't matter, breaker's off". Downstream `recordCordonEventInCircuitBreaker` checks `r.config.CircuitBreakerEnabled` again so the value is never observed, but the function's return shape now has an undocumented invariant. At minimum add a comment; better, return a third-value sentinel (e.g., `(scope: bool, halt: bool, ok: bool)` or change to a struct) so future refactors can't read the placeholder by mistake.

### [P2] Default `nodeSelector: "nvidia.com/gpu.present=true"` is a behavioural change for existing installs
**File:** `distros/kubernetes/nvsentinel/charts/fault-quarantine/values.yaml:66`

Operators currently relying on the breaker covering all nodes (including CPU/management nodes that may also be cordoned by FQ) will silently see those events drop out of the breaker calculation after upgrade. Unlike the local branch this no longer causes a CrashLoopBackOff (the breaker pauses gracefully), but it does silently weaken safety on the non-GPU population. Worth a release-note callout, and the chart comment should make explicit that quarantine actions on non-matching nodes are still allowed but no longer count toward the breaker.

### [P2] `getNodeScopeWithRetry` does not retry on transient API errors
**File:** `fault-quarantine/pkg/breaker/breaker.go:365-371`

```go
scope, err := b.cfg.K8sClient.GetCircuitBreakerNodeScope(ctx, nodeName, b.cfg.NodeSelector)
if err != nil {
    result = resultError
    errorType = "api_error"
    return NodeScope{}, b.handleGetNodeScopeError(err, attempt, maxRetries)
}
```

API errors return immediately with no retry, while empty-scope retries up to 10 times. Since `GetCircuitBreakerNodeScope` reads from the lister (no live API call), the only realistic error path here is `node informer cache not synced yet`, which is exactly the case that benefits from retry. Either retry transient errors with the same exponential backoff, or document why API errors are non-recoverable (and probably elevate them to `ErrRetryExhausted` so the pod restarts).

### [P2] `recordCordonEventInCircuitBreaker` respects `event.HealthEvent.QuarantineOverrides.Force` but the matching trip-check does not
**File:** `fault-quarantine/pkg/reconciler/reconciler.go:1023-1027`

```go
if r.config.CircuitBreakerEnabled &&
    (event.HealthEvent.QuarantineOverrides == nil || !event.HealthEvent.QuarantineOverrides.Force) {
    r.cb.AddCordonEventWithScope(...)
}
```

A force-quarantined event skips the breaker's numerator. But `checkCircuitBreakerAndHalt` runs unconditionally on every event, including forced ones — so a forced event can be blocked by a tripped breaker. Either the force-bypass should also skip the trip check (so "force" really means force), or both paths should treat the event the same way. Today the asymmetry means a forced event can be denied at the gate but, if it gets through, won't even count.

### [P3] Per-event O(N) cache walk in `GetNodeScope`
**File:** `fault-quarantine/pkg/informer/node_informer.go:178-208`

`GetNodeScope` walks every cached node and applies `selector.Matches(labels.Set(node.Labels))` once per `ProcessEvent` invocation. For 10k-node clusters and high event rates that's 10k label comparisons per event. The lister has no label index so this is roughly inherent, but the branch could short-circuit `nodeInScope` once found (`if name == nodeName { nodeInScope = true; if alreadyHaveCount { break } }`) once the count is known. Lower priority — the lister List+filter is what client-go does internally too.

### [P3] `parseCircuitBreakerNodeSelector` test for "invalid" uses `"not in"` which happens to be a parse-able token prefix in Kubernetes selector syntax
**File:** `fault-quarantine/pkg/initializer/init_test.go:39-42`

`"not in"` parses as "key 'not' must be `in` (...)" and fails on missing values, which is the intent — but selector grammar evolution (real example: `kubernetes/apimachinery` historically tightened/loosened a few corners) could change which inputs error out. Pin to something unambiguously malformed like `"==!=foo"` so the test stays meaningful across api-machinery upgrades.

## Verdict

**correct (with caveats)** — for the first time across the three attempts, the design is internally consistent: scope is computed once per event, threaded through both the gate and the recording, and the empty-scope condition is a first-class paused state rather than a restart trigger. The remaining findings are quality issues (P1 dead-code/stale-data on cold paths, P2 chart/interface tidy-ups, P2 force-override asymmetry) rather than the operational hazards I flagged on the other two branches.

If this is the candidate you intend to ship, the must-fix list is small:
1. Either document or eliminate the `NodeInScope=false` value when `Tripped=true` (P1).
2. Drop the `cordonEvent` value from `indexToNodes` to remove the latent stale-`bucketIndex` footgun (P1).
3. Decide and align the `Force` override behaviour for the trip-check (P2).
4. Restore the `| default ""` chart guard or add a Helm-time `fail` for non-string inputs (P2).

## Cross-cutting comparison (now with all three branches)

| Concern | Local branch | GitHub branch 2 | This branch |
|---|---|---|---|
| Numerator/denominator symmetry | Broken | Correct | Correct |
| Empty-scope handling | Pod CrashLoop | Reconciler halt with poll | Reconciler pause with poll, distinct metric state |
| Race between trip-check and event recording | N/A (no per-event scope) | TOCTOU (two snapshots) | Single snapshot, threaded through |
| Hot-path cost per event | O(1) (count only) | 2× O(N) (one per check, one per record) | 1× O(N) (one walk per check) |
| Selector flexibility (non-Nvidia clusters) | Free-form selector | Closed enum | Free-form selector |
| Default behaviour change | Silently filters denominator only (asymmetric) | None (`scope: all` default) | Filters both num/denom (correct) |
| Public-field thread-safety | Unprotected | Unprotected | Selector passed through Config; not mutated post-init |
| Dedicated metric for paused/empty-scope | No | No | Yes (`StateScopeEmpty`) |
| Test coverage of new behaviour | Selector parsing only | Scope flag in breaker | Scope flag + mixed-cluster end-to-end + empty-scope sentinel |

Of the three, this is the one I would advance to merge after addressing the P1 items above. Branch 2 needs significant rework of `recordCordonEventInCircuitBreaker` and the `<-ctx.Done()` halt; the local branch needs the entire scoped-numerator design grafted in, which is most of what this branch already does.

## Human Reviewer Callouts (Non-Blocking)

- **This change introduces backwards-incompatible public schema/API/contract changes:** `breaker.CircuitBreaker` interface gains `AddCordonEventWithScope` and `CheckCircuitBreakerForNode`; `K8sClientOperations` swaps `GetCircuitBreakerNodeNames` for `GetCircuitBreakerNodeScope` (and keeps an unused `GetTotalNodes`). Any out-of-tree implementations / mocks must be updated.
- **This change modifies auth/permission behavior:** Indirect — existing chart users get their breaker scoped to GPU nodes by default after upgrade; quarantines on non-GPU nodes still happen but no longer count toward the trip threshold. Recommend a release note.
