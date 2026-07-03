# Playbook: Driver / Server / Serialization

## Context
Changes to gremlin-driver, gremlin-server, or gremlin-util. These modules
handle connections, protocol, serialization, and request lifecycle. Changes
here affect all users and all GLV drivers. Correctness under concurrency
and backwards compatibility are critical concerns.

## Enrich
Identify what layer of the driver/server stack is being modified:
- Connection lifecycle (pooling, creation, cleanup)
- Protocol handling (HTTP, WebSocket, request/response framing)
- Serialization (GraphBinary, GraphSON, type registration)
- Server initialization (configuration, script engines)
- Authentication/authorization

For each layer, look for:

**Connection management:**
- Are HttpClient/WebSocket instances shared or created per-connection?
  (Shared is correct for pooling; per-connection defeats the pool)
- Could pool size = 1 cause deadlock?
- Data structure choices for connection tracking: CopyOnWriteArraySet has
  write overhead, ConcurrentLinkedQueue is better for frequent insert/remove
- Are settings kept as a cohesive object or extracted into individual fields?
  (Prefer the object — individual fields drift from the source of truth)

**Serialization:**
- De-bulking: should happen lazily at traversal iteration, not eagerly when
  responses arrive (eager de-bulking puts all objects in memory at once)
- Removed type IDs: if a serializer type code is removed, leave a comment
  documenting what it was ("122 was Bytecode until removed in 4.x")
- Error response fallback: GraphBinary errors may come back as JSON if the
  server can't serialize the error in binary
- Numeric types: longs need 'L' suffix in GremlinLang text format

**API migration:**
- New code must use GremlinLang, not Bytecode (removed in 4.x)
- New code must use with_(), not withRemote (deprecated)
- Server configuration: gremlin-lang expressions, not Groovy scripts
- Don't leave commented-out old code — remove it cleanly

## Checks
- blast_radius(pr.modified(), 3)
- high_centrality(pr.modified())
- coverage_gaps(pr.tests(), pr.modified())
- orphans("Function", "tests", { changedOnly: true })

## Interpret
Driver/server changes have inherently high blast radius — they're shared
infrastructure. Don't flag blast radius as surprising, but DO highlight
which specific callers are most affected. Use `listExternalRefs` to separate
real project coupling (`origin: project`/`unresolved`) from library noise
(`origin: library`) when judging a changed function's reach — centrality already
drops the library calls, so a function still ranking high is genuinely central.

Connection pooling and concurrency code should have explicit test coverage.
If coverage gaps exist in connection lifecycle code, flag prominently —
these are the hardest bugs to reproduce and the most impactful in production.

Serialization changes that add/remove type codes need upgrade documentation.
Check if the PR includes corresponding upgrade doc entries.

For "good enough for now" patterns (strategy handling, migration scaffolding),
note them as acceptable if they're clearly marked as temporary, but flag if
they look like they'll become permanent debt.

## Escape
- if connection pool logic modified without concurrency tests — "Pool changes need concurrency testing — flag for manual review of deadlock/race conditions"
- if serialization type codes added/removed without IO doc update — "Serialization changes need IO documentation updates"
