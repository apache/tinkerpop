# Playbook: Driver / Server / Serialization

## Context
Changes to gremlin-driver, gremlin-server, or gremlin-util. These modules
handle connections, protocol, serialization, and request lifecycle. Changes
here affect all users and all GLV drivers. Correctness under concurrency
and backwards compatibility are critical concerns.

## Enrich
- `linkDoc` — if the change adds or removes a serializer type code, record the
  documentation that covers that format (`--entity File --name <doc>`).
- `linkDiscussion` — record a referenced proposal or JIRA.

## Inspect
Context: layered — identify the layer (connection lifecycle / protocol /
serialization / server init / auth) first, then check the matching group.

**Connection management:**
- HttpClient/WebSocket instances shared or created per-connection? (shared is
  correct for pooling; per-connection defeats the pool)
- Could pool size = 1 cause deadlock?
- Connection-tracking data structures: `CopyOnWriteArraySet` has write overhead;
  `ConcurrentLinkedQueue` is better for frequent insert/remove.
- Settings kept as a cohesive object, or extracted into individual fields that
  drift from the source of truth? (prefer the object)

**Serialization:**
- De-bulking happens lazily at traversal iteration, not eagerly on response
  arrival (eager puts all objects in memory at once).
- Removed type IDs leave a comment documenting what they were ("122 was Bytecode
  until removed in 4.x").
- Error-response fallback: GraphBinary errors may return as JSON when the server
  can't serialize the error in binary.
- Numeric types: longs need an `L` suffix in GremlinLang text format.

**API migration:**
- New code uses GremlinLang, not Bytecode (removed in 4.x).
- New code uses `with_()`, not `withRemote` (deprecated).
- Server configuration uses gremlin-lang expressions, not Groovy scripts.
- No commented-out old code left behind — remove it cleanly.

## Interpret
- `checks.blastRadius` — inherently high (shared infrastructure); don't flag the
  reach itself, name the specific callers most affected.
- `checks.centrality` with `listExternalRefs` — separate project coupling
  (`origin: project`/`unresolved`) from library noise (`origin: library`);
  centrality already drops library calls, so a function ranking high is
  genuinely central.
- `checks.coverageGaps` / `checks.orphans` in connection-lifecycle or
  concurrency code — blocking; these are the hardest bugs to reproduce and the
  most impactful in production.
- Serialization type-code change with no `documents` edge from Enrich — high;
  flag the missing IO/upgrade doc.
- "Good enough for now" patterns (strategy handling, migration scaffolding) —
  acceptable if clearly marked temporary; flag if they look like permanent debt.

## Escape
- if connection pool logic modified without concurrency tests — "Pool changes need concurrency testing — flag for manual review of deadlock/race conditions"
- if serialization type codes added/removed without IO doc update — "Serialization changes need IO documentation updates"
