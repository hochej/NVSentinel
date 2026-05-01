# Review: `hochej/NVSentinel@circuit-breaker-node-selector-scoped`

## Strengths (calibration)

This branch reads as the most coherent of the three. It combines what worked in each of the previous attempts:

- Free-form `labels.Selector` (from the local branch) — flexible across vendors and label schemes, no hard-coded `isGPUNode` heuristic.
- Scope captured at cordon time via `cordonEvent.inScope` (from branch 2) — keeps numerator and denominator symmetric.
- `ErrEmptyCircuitBreakerScope` distinct from `ErrRetryExhausted` — empty selector no longer crash-loops the pod.
- New `CheckCircuitBreakerForNode` returns `(Tripped, NodeInScope, ScopedNodeCount)` from a **single** informer snapshot, which the reconciler then threads through to `AddCordonEventWithScope`. This eliminates the TOCTOU window that branch 2 had between `IsTripped` and `recordCordonEventInCircuitBreaker`.
- `StateScopeEmpty` exposed as a metric value (not persisted), so dashboards can distinguish "paused on empty scope" from "CLOSED, healthy".
- `applyQuarantine` no longer makes a K8s call before doing the cordon (branch 2 regression is fixed) — the scope decision is computed once up front in `ProcessEvent`.
- Empty Helm value `nodeSelector: ""` correctly maps to `labels.Everything()` (the local branch's docs/code mismatch is fixed here), and the Helm template type-checks the value with `kindIs`.
- Test coverage explicitly exercises mixed-cluster, empty-scope, transient API errors, already-tripped paths, and the in-scope-preservation rule.

That said, several real defects remain.

## Findings

### [P0] Empty selector blocks events for nodes that are not in the breaker's scope
**File:** `fault-quarantine/pkg/breaker/breaker.go:248-255` and `pkg/reconciler/reconciler.go:504-520`

`getNodeScopeWithRetry` returns `ErrEmptyCircuitBreakerScope` whenever `ScopedNodeCount == 0`, regardless of whether the node currently being processed is even in scope. The reconciler responds by entering an indefinite 30s-poll halt for that event. Concrete failure: GPU pool is empty (drained for replacement, NFD not running yet, custom labels), but a CPU-only node fires a fault event — that event is not subject to the breaker by design, yet `CheckCircuitBreakerForNode("cpu-x")` fails and the reconciler stops processing it. Since `recordCordonEventInCircuitBreaker` already correctly no-ops for out-of-scope nodes, the right behavior is: when the called-for node is out of scope, return `(Tripped:false, NodeInScope:false, ScopedNodeCount:0)` and let processing proceed. Reserve the empty-scope halt for in-scope events.

### [P1] Inconsistent recovery strategy between startup and runtime for the same error
**File:** `fault-quarantine/pkg/reconciler/reconciler.go:362-394` (startup) vs `:488-541` (runtime)

`checkCircuitBreakerAtStartup` returns `ErrRetryExhausted` upward, which causes pod restart. `checkCircuitBreakerAndHalt` at runtime catches the same wrapped error in its generic `else` branch and just retries every 30s forever. A persistent informer/API failure therefore produces opposite operational outcomes depending on whether the failure straddles startup. Pick a single policy (either both restart, or both retry-with-bounded-attempts-then-restart) and apply it in both places.

### [P1] `waitForCircuitBreakerRetry` uses a flat 30s delay for every error class
**File:** `fault-quarantine/pkg/reconciler/reconciler.go:54,544-549`

```go
circuitBreakerScopeRetryDelay = 30 * time.Second
```

The breaker's own retry loop (`getNodeScopeWithRetry`) already burned ~30s of exponential backoff before returning. The reconciler's outer loop then waits another 30s on every iteration — for empty-scope and for api-error alike. A 1-second informer hiccup recovers in 30+30=60s minimum. Two improvements: shorter delay for non-empty-scope errors (api/cache errors should retry in seconds, since they are transient by hypothesis), and an event-driven wakeup (e.g., subscribe to NodeInformer add/update events) for the empty-scope wait so a newly-labeled node unblocks processing immediately.

### [P1] Per-event O(N) walk of the entire node cache
**File:** `fault-quarantine/pkg/informer/node_informer.go:178-208`

```go
allObjs := ni.informer.GetIndexer().List()
for _, obj := range allObjs {
    ...
    if !selector.Matches(labels.Set(node.Labels)) { continue }
    selectedNodes++
    if node.Name == nodeName { nodeInScope = true }
}
```

Every health event triggers a full cache walk plus a `selector.Matches` per node. For thousands of nodes and burst event rates, this is significant CPU and (because the lister's underlying store is iterated and labels-set construction is per-node) garbage. The two outputs can be derived more cheaply: maintain a running `selectedNodes` counter via the informer's add/update/delete handlers, and use `lister.Get(nodeName)` + one `selector.Matches` call for membership (O(1)). The selector is fixed at init, so cache invalidation is straightforward.

### [P2] `CheckResult.NodeInScope` is computed even when the breaker is already tripped, but the caller cannot use it
**File:** `fault-quarantine/pkg/breaker/breaker.go:227-242` and reconciler `:526-538`

When the breaker is tripped, `checkCircuitBreaker` does an extra `GetCircuitBreakerNodeScope` call just to populate `NodeInScope` and `ScopedNodeCount`. The reconciler's tripped path is `<-ctx.Done()` then `return result.NodeInScope, true` — `shouldHalt=true` is checked first by `ProcessEvent`, so the `NodeInScope` value is never consumed. Skip the lookup when `alreadyTripped && nodeName != ""`; just return `Tripped:true` with zero-valued scope.

### [P2] Helm default selector silently changes behavior for existing installs
**File:** `distros/kubernetes/nvsentinel/charts/fault-quarantine/values.yaml:67`

`nodeSelector: "nvidia.com/gpu.present=true"` flips upgraded deployments from "all nodes" to "GPU nodes only". Combined with the empty-scope-blocks-events defect (P0 above), an upgrade on a cluster where the label has not yet been applied (NFD/GPU operator timing, custom labelling, dev/kind clusters) freezes event processing on every event for a long time. Less destructive than branch 1's crash-loop, but the default still warrants a release note and arguably should remain `""` (all nodes) until the operator opts in.

### [P2] Generic-error retry path masks `ErrRetryExhausted` from operators
**File:** `fault-quarantine/pkg/reconciler/reconciler.go:511-518`

```go
} else {
    slog.ErrorContext(ctx, "Error checking if circuit breaker is tripped", "error", err)
    ...
}
if !waitForCircuitBreakerRetry(ctx) {
    return false, true
}
continue
```

The else branch logs an error and re-enters the retry loop without distinguishing `ErrRetryExhausted` (the breaker has already exhausted retries — additional outer retries are unlikely to help) from a one-shot context-cancellation. Operators see a steady stream of "Error checking if circuit breaker is tripped" with no indication that the breaker itself has given up. Either special-case `ErrRetryExhausted` (escalate / restart) or annotate the log with the underlying error class.

### [P3] Dead defensive branch in `AddCordonEventWithScope`
**File:** `fault-quarantine/pkg/breaker/breaker.go:182-187`

Out-of-scope events are never stored in `nodeToEvent` (the early return at `:172-179` handles the existing-in-scope case, and the `!inScope` exit at `:182-186` returns before insertion). Therefore `oldEvent.inScope` is always `true` when `exists` is true, and `if oldEvent.inScope { b.buckets[...]-- }` is unconditionally taken. Likewise, `delete(b.nodeToEvent, nodeName)` inside the `!inScope` exit can never delete anything (no entry exists, since the existing-entry+`!inScope`+`oldEvent.inScope` case already returned at `:172`). Either remove the dead checks or convert them to `panic`/assertion to make the invariant explicit.

### [P3] `K8sClientOperations.GetTotalNodes` retained but unused by the breaker
**File:** `fault-quarantine/pkg/informer/k8s_client.go:122-130`

`GetTotalNodes` is no longer called from `breaker.go` — `GetCircuitBreakerNodeScope` superseded it. The method survives on `FaultQuarantineClient` but is no longer in the `K8sClientOperations` interface. Either delete it (and any callers in tests/metrics) or document why it is still public.

## Verdict

**needs attention** — mostly for P0 (out-of-scope nodes blocked by empty scope) and P1s. The architecture is sound and clearly the strongest of the three branches; the remaining defects are localized fixes rather than design changes.

## Cross-cutting comparison (now with branch 3)

| Concern | Branch 1 (`fix/...selector`) | Branch 2 (`...node-scope`) | Branch 3 (`...selector-scoped`) |
|---|---|---|---|
| Numerator/denominator symmetry | broken | correct | correct |
| Empty scope behavior | crash loop | reconciler wedge inside `applyQuarantine` | per-event halt with 30s poll (over-blocks out-of-scope nodes) |
| TOCTOU between check and record | n/a (no scope at record time) | yes (two separate K8s calls) | none (single snapshot threaded through) |
| Vendor/label flexibility | label-selector ✓ | hard-coded `isGPUNode` | label-selector ✓ |
| Hot-path cost per event | 1× count | 2× O(N) list + map alloc | 1× O(N) list (no map alloc, returns counts only) |
| API surface impact | small | wide (interface adds + signature changes) | wide (similar to branch 2, plus new `CheckCircuitBreakerForNode`) |
| Test coverage of new logic | parser + selector filter | scope-flag + tripped flow | scope-flag + tripped flow + empty scope + flaky API |
| Operational risk on upgrade | high (crash loop on no-GPU clusters) | medium (reconciler halt) | medium (event halt; recoverable when label appears) |

Branch 3 is the right base to ship from. The minimal changes I would block on are P0 (let out-of-scope events through when scope is empty) and P1 (consistent error policy + bounded recovery latency). The O(N)-per-event walk (P1 informer) is a follow-up but should be tracked.

## Human Reviewer Callouts (Non-Blocking)

- **This change introduces backwards-incompatible public schema/API/contract changes:** `breaker.K8sClientOperations` removes `GetTotalNodes`/`GetCircuitBreakerNodeNames` and adds `GetCircuitBreakerNodeScope`; `breaker.CircuitBreaker` adds `AddCordonEventWithScope` and `CheckCircuitBreakerForNode`; `breaker.Config` adds `NodeSelector`; the `circuitBreaker.nodeSelector` Helm value defaults to `nvidia.com/gpu.present=true`, which changes the trip threshold for any existing installation on upgrade.
- **This change modifies auth/permission behavior:** indirectly — the breaker still relies on the existing node `list/watch` RBAC; no new verbs required, but per-event cache walks increase informer CPU/memory pressure on large clusters.
