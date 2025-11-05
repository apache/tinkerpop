Title: DevOps CMDB and Incident Knowledge Graph — Language-Agnostic Specification

Summary
A vendor/framework/language-neutral specification for a graph-based Configuration Management Database (CMDB) and Incident Knowledge Graph suitable for building example applications and upgrade exercises on Apache TinkerPop. This document defines the domain model (vertices, edges, and properties), recommended identifiers and indexing guidelines, data quality constraints, example workloads and traversals, and non-functional requirements. The goal is to make it easy for an LLM (or a developer) to generate an implementation in any language, with any web framework, on any supported TinkerPop/graph provider. Assumption: a Gremlin Server–compliant system (or equivalent protocol-compatible service) is available for remote traversal submission.

Goals
- Provide a realistic, moderately complex domain to exercise graph modeling, traversals, and API design.
- Encourage use of rich edge properties (temporal, metrics, provenance) to demonstrate graph advantages over relational models.
- Keep the spec portable across programming languages, application frameworks, and TinkerPop versions/providers.
- Support common operational queries: blast radius, dependency discovery, root-cause hints, deployment/incident correlation, ownership, and timelines.

Non-Goals
- Prescribe a specific programming language, framework, or persistence backend.
- Require a particular TinkerPop version or feature outside the broadly available core.
- Provide exhaustive production hardening (security/compliance) beyond reasonable examples and guidance.

Conceptual Overview
The graph models services, applications, infrastructure, deployments, dependencies, teams, alerts, and incidents. It enables:
- Traversal across service/application/runtime dependencies.
- Temporal reasoning about deployments, alerts, and incidents.
- Impact analysis (blast radius) and ownership discovery.
- Correlation between runtime signals and change events.

Canonical Labels and Properties
Notes
- Use explicit property names; no import wildcards in generated code.
- Prefer stable string identifiers (natural keys) over auto-increment integers.
- Property types are suggestions; adapt to your graph provider’s supported types.
- Cardinality defaults to single unless specified as set or list.

Vertex Labels
1) service
- serviceId: String (unique, stable) — e.g., svc:payments
- name: String
- tier: String (e.g., critical, high, medium, low)
- ownerTeamId: String (FK to team.teamId)
- sla: String (e.g., "99.9%/30d")
- env: String (e.g., prod, staging, dev)
- tags: Set<String>
- criticality: Int (1..5)
- createdAt: Instant
- updatedAt: Instant

2) application
- appId: String (unique) — e.g., app:payments-api
- name: String
- language: String (free-form; do not assume a particular runtime)
- repo: String (URL or VCS path)
- version: String (semantic version or build hash)
- runtime: String (e.g., jvm, dotnet, nodejs, python)
- env: String
- createdAt: Instant
- updatedAt: Instant

3) host
- hostId: String (unique) — hostname, node id, or cloud instance id
- hostname: String
- provider: String (e.g., aws, gcp, onprem)
- region: String
- availabilityZone: String
- env: String
- os: String
- instanceType: String
- createdAt: Instant
- updatedAt: Instant

4) deployment
- deployId: String (unique)
- version: String
- initiatedBy: String (user/bot)
- deployedAt: Instant
- status: String (e.g., success, failed, partial, inProgress)
- changeSet: List<String> (commit ids/PRs/tickets)
- env: String

5) dependency (external resource)
- depId: String (unique)
- type: String (db, cache, queue, api, storage, dns, secret, other)
- name: String
- vendor: String
- criticality: Int (1..5)
- endpoint: String (URL, ARN, etc.)
- env: String
- createdAt: Instant
- updatedAt: Instant

6) team
- teamId: String (unique)
- name: String
- onCall: Boolean
- pager: String (optional)
- slack: String (optional)
- createdAt: Instant
- updatedAt: Instant

7) incident
- incidentId: String (unique)
- severity: String (sev0, sev1, sev2, sev3)
- state: String (open, investigating, mitigated, closed)
- openedAt: Instant
- closedAt: Instant? (nullable)
- summary: String
- description: String
- env: String
- createdBy: String
- updatedAt: Instant

8) alert
- alertId: String (unique)
- source: String (monitoring system)
- metric: String (e.g., errorRate, latency)
- firedAt: Instant
- clearedAt: Instant? (nullable)
- severity: String
- env: String
- fingerprint: String (grouping key)
- createdAt: Instant
- updatedAt: Instant

Edge Labels (camelCase) with Rich Properties
Note: Edge properties are central to this model; use them to encode time, confidence, quantitative metrics, and provenance.

A) runsOn (application → host)
- since: Instant — when the app started running on the host
- status: String (active, draining, retired)
- instanceId: String (container/pod id, process id)
- lastSeen: Instant — last heartbeat
- cpuPct: Double
- memPct: Double
- confidence: Double (0..1) — if relationship inferred

B) deployedAs (service → application)
- since: Instant — when the service started being represented by the app
- strategy: String (blueGreen, canary, rolling, recreate)
- trafficPct: Double — percent traffic routed
- status: String (active, deprecated)
- notes: String

C) deploymentOf (deployment → application)
- relationCreatedAt: Instant — record creation time
- targetVersion: String — duplicate for quick reads
- result: String (success, failed, partial)
- durationMs: Long (elapsed time)
- changeTicket: String (optional external id)

D) deployedTo (deployment → host)
- at: Instant — when the deployment reached this host
- result: String (success, failed, skipped)
- attempts: Int
- durationMs: Long
- rollback: Boolean

E) dependsOn (service → dependency)
- since: Instant — when the dependency was added
- required: Boolean — hard vs soft dependency
- direction: String (read, write, both)
- latencyP50Ms: Long (observed median)
- errorRatePct: Double
- weight: Double (relative importance, 0..1)
- lastVerifiedAt: Instant

F) ownedBy (service → team)
- since: Instant
- role: String (primary, secondary, consulting)
- escalationPolicy: String

G) triggered (alert → incident)
- at: Instant — correlation time
- correlationScore: Double (0..1)
- reason: String (short explanation or rule id)
- evidenceId: String (pointer to logs/trace)

H) affects (incident → service)
- at: Instant — first known impact
- impactType: String (degradation, outage, dataLoss, security)
- scope: String (percent traffic affected, region subset)
- slaImpactPct: Double
- confidence: Double (0..1)

I) relatedTo (incident ↔ incident) [undirected semantics; model with two directed edges if needed]
- correlationScore: Double
- windowStart: Instant
- windowEnd: Instant
- relationType: String (duplicateCause, followOn, sharedDependency, coOccurrence)

J) calls (application → application)
- since: Instant
- protocol: String (http, grpc, mq, rpc)
- method: String (GET/POST or operation name)
- avgLatencyMs: Long
- errorRatePct: Double
- sampleRatePct: Double
- lastSeen: Instant

Identifiers and Cardinality
- Vertex IDs: Use stable string keys (e.g., serviceId) as custom IDs where supported; otherwise store as a dedicated property and map internal IDs as needed.
- Edge multiplicity: Allow multiple edges between the same endpoints if they differ by context/time (e.g., calls over protocols). Consider a uniqueness policy per (outV, inV, label, keyPropertiesTuple).
- Timestamps: Use Instant (UTC). Where Instant unsupported, use epochMillis (Long).
- Sets vs Lists: tags are Set; changeSet is List to preserve order.

Indexing Guidance (Provider-Agnostic)
- Equality/lookup indices:
  - Vertex: service.serviceId, application.appId, host.hostId, deployment.deployId, dependency.depId, team.teamId, incident.incidentId, alert.alertId.
  - Common filters: env, name, severity, state, version.
- Edge-centric indices (where supported):
  - dependsOn.since, dependsOn.required
  - calls.protocol, calls.method
  - affected edges by at, impactType
- Full-text (optional): incident.summary/description for search.
- Composite vs Mixed: choose based on provider; ensure exact match queries benefit.

Data Quality Constraints (Logical)
- Uniqueness: All *Id properties are globally unique per label.
- Referential integrity: Edge endpoints must exist; deletion should consider cascading or soft-delete.
- Enum validation: severity, state, protocol, strategy, impactType, result should match allowed sets.
- Temporal consistency: openedAt ≤ closedAt; since ≤ lastSeen; windowStart ≤ windowEnd.
- Env consistency: Cross-env edges generally discouraged except where intended (documented exceptions only).
- Confidence bounds: 0 ≤ confidence ≤ 1.0; 0 ≤ errorRatePct ≤ 100; 0 ≤ trafficPct ≤ 100.

Minimum Viable Dataset (for examples/tests)
A deterministic sample dataset is provided as a Gremlin script at docs/upgrade/spec/cmdb-data.gremlin. Each line is a single mutation (one vertex or one edge) terminated with iterate() for stable, idempotent loading via a Gremlin Server–compliant system.

Example Traversal Patterns (Gremlin-like Pseudocode)
Note: The following are conceptual and avoid version-specific constructs. Adapt to your provider’s traversal DSL. Variable names are illustrative.

1) Blast radius from an incident (impacted services and upstream relations)
- Input: incidentId
- Idea: From an incident, traverse affects to services, then explore transitive dependencies and deployments.
  g.V().has('incident','incidentId', iid)
    .out('affects')
    .emit()
    .repeat(out('dependsOn','deployedAs').in('deploymentOf'))
    .path()

2) Root-cause candidates (upstream first failing dependency)
- Input: incidentId
  g.V().has('incident','incidentId', iid)
    .out('affects')
    .repeat(out('dependsOn')).emit()
    .where(outE('dependsOn').values('errorRatePct').is(gt(5.0)))
    .limit(50)

3) Deployment → incident correlation
- Input: deployId
  g.V().has('deployment','deployId', did)
    .out('deployedTo').in('runsOn').in('deployedAs').dedup()
    .in('affects').hasLabel('incident')
    .order().by('openedAt', decr).limit(20)

4) Ownership and on-call for an impacted service
- Input: incidentId
  g.V().has('incident','incidentId', iid)
    .out('affects')
    .out('ownedBy')
    .valueMap(true)

5) Hot paths between applications (runtime call graph)
- Input: appId
  g.V().has('application','appId', aid)
    .repeat(outE('calls').order().by('avgLatencyMs', decr).inV()).times(3)
    .path()

6) Services at risk due to a failing dependency
- Input: depId
  g.V().has('dependency','depId', did)
    .inE('dependsOn').has('required', true).outV()
    .dedup()

7) Time-bounded alert to incident correlation
- Inputs: fingerprint, windowStart, windowEnd
  g.V().has('alert','fingerprint', fp)
    .has('firedAt', between(windowStart, windowEnd))
    .outE('triggered')
    .has('correlationScore', gt(0.6))
    .inV()

Suggested API Surfaces (Language-Neutral)
These are example resources for a REST/RPC/GraphQL interface. They are intentionally generic.
- GET /incidents/{id}/blastRadius — returns affected services and transitive dependencies with paths.
- GET /services/{id}/dependencies?depth=N — returns direct and transitive dependencies.
- GET /deployments/{id}/timeline — returns path from deployment to impacted services/incidents.
- GET /services/{id}/owners — returns team ownership and escalation info.
- POST /alerts — upsert alert vertices and (optionally) triggered edges.
- POST /incidents — create/modify incident vertices and affects edges.
- POST /deployments — create deployment vertices and link to application/host.

Operational Concerns (General)
- Transactions: Group related vertex/edge mutations atomically when supported; otherwise implement idempotency keys on the write path.
- Idempotency: Upserts keyed by natural IDs; avoid accidental duplication.
- Pagination: Use limit/offset or cursor; cap server-side to protect the graph.
- Validation: Enforce enum bounds, temporal checks, and numeric ranges before writes.
- Timestamps: Use UTC; prefer Instant or epochMillis consistently.
- Observability: Consider storing minimal audit metadata on edges (createdBy, updatedAt) if provider supports meta-properties.

Testing Guidance
This section specifies exact black-box tests to run against an implementation that follows this spec, assuming the deterministic dataset at docs/upgrade/spec/cmdb-data.gremlin has been loaded into a Gremlin Server–compliant system (traversal source g). Tests are language- and framework-agnostic. For REST, substitute your base URL; for other transports the inputs/outputs are equivalent.

General setup
- Precondition: Seed the dataset by submitting each non-empty, non-comment line from docs/upgrade/spec/cmdb-data.gremlin. Ensure a clean graph (e.g., drop all vertices first) so counts match exactly.
- Conventions: All timestamps are epochMillis; string IDs are exact and case sensitive.

Smoke and readiness
- Call: GET /health/ready
- Expect: HTTP 200 with body containing "ready" (or equivalent readiness signal).

Service dependencies (GET /services/{id}/dependencies?depth=N)
1) svc:orders, depth=2
- Call: GET /services/svc:orders/dependencies?depth=2
- Expect: HTTP 200; an ordered or unordered list of vertex value maps including exactly these two vertices:
  - service svc:orders (properties include serviceId: "svc:orders")
  - dependency dep:orders-db (properties include depId: "dep:orders-db")
- Count: 2 total elements. No other services or dependencies should appear for this service in the provided dataset.

2) svc:payments, depth=1
- Call: GET /services/svc:payments/dependencies?depth=1
- Expect: HTTP 200; list of value maps for:
  - service svc:payments
  - dependency dep:payments-db
  - dependency dep:payments-queue
- Count: 3 total elements.

3) svc:inventory, depth=3
- Call: GET /services/svc:inventory/dependencies?depth=3
- Expect: HTTP 200; list includes:
  - service svc:inventory
  - dependency dep:inventory-cache
- Count: 2 total elements (no additional transitive dependencies in the sample data).

Service owners (GET /services/{id}/owners)
4) svc:payments
- Call: GET /services/svc:payments/owners
- Expect: HTTP 200; list includes exactly one team vertex value map with:
  - teamId: "team:payments"
  - name: "Payments"
  - onCall: true
- Count: 1 total element.

5) svc:api-gateway
- Call: GET /services/svc:api-gateway/owners
- Expect: HTTP 200; list includes exactly one team with teamId: "team:platform" and name: "Platform Engineering"
- Count: 1 total element.

Deployment timeline (GET /deployments/{id}/timeline)
6) depoy:payments-20231113.2 (partial deployment that failed on at least one host)
- Call: GET /deployments/depoy:payments-20231113.2/timeline
- Expect: HTTP 200; list of incident vertex value maps related via the traversal (deployedTo -> runsOn -> deployedAs -> affects). Must include exactly:
  - incidentId: "inc:sev1-payments-001" (severity sev1)
- Count: 1 total element.

7) depoy:payments-20231114.1 (successful follow-up)
- Call: GET /deployments/depoy:payments-20231114.1/timeline
- Expect: HTTP 200; list includes the same incident as above:
  - incidentId: "inc:sev1-payments-001"
- Count: 1 total element.

8) depoy:api-gateway-20231114.1
- Call: GET /deployments/depoy:api-gateway-20231114.1/timeline
- Expect: HTTP 200; list includes:
  - incidentId: "inc:sev1-api-001"
- Count: 1 total element.

Incident blast radius paths (GET /incidents/{id}/blastRadius?depth=N)
Notes: Responses are typically represented as a collection of paths where each object is a valueMap(true) of the vertex at that step. Order within a path follows traversal order. Exact path ordering across the response may vary by provider, but membership assertions below must hold.

9) inc:sev1-payments-001, depth=1
- Call: GET /incidents/inc:sev1-payments-001/blastRadius?depth=1
- Expect: HTTP 200; at least two paths starting with the incident then the affected service svc:payments and then one hop of the repeat step. The set of terminal vertices across all returned paths must contain:
  - dependency dep:payments-db
  - dependency dep:payments-queue
  - deployment vertices reachable via deployedAs/deploymentOf for the payments service: depoy:payments-20231113.2 and depoy:payments-20231114.1
- Minimal membership checks:
  - There exists a path whose last vertex has depId = "dep:payments-db"
  - There exists a path whose last vertex has depId = "dep:payments-queue"
  - There exists a path whose last vertex has deployId = "depoy:payments-20231113.2"
  - There exists a path whose last vertex has deployId = "depoy:payments-20231114.1"

10) inc:sev1-api-001, depth=1
- Call: GET /incidents/inc:sev1-api-001/blastRadius?depth=1
- Expect: HTTP 200; paths must begin with incident inc:sev1-api-001 then service svc:api-gateway. The set of terminal vertices across all returned paths must include:
  - deployment depoy:api-gateway-20231113.1
  - deployment depoy:api-gateway-20231114.1
- Minimal membership checks (as above) for both deployIds.

Negative and edge cases
11) Unknown serviceId
- Call: GET /services/svc:does-not-exist/dependencies?depth=2
- Expect: HTTP 200 with an empty list (no results).

12) Service with optional dependency (required=false)
- Call: GET /services/svc:inventory/dependencies?depth=1
- Expect: HTTP 200 with exactly two elements: svc:inventory and dep:inventory-cache. Verify that dependency existence is returned regardless of the required flag (edge property), as this endpoint enumerates topology not policy.

13) Readiness under graph reset
- Procedure: Drop all vertices, reload the dataset, then call GET /health/ready
- Expect: HTTP 200 and subsequent endpoint tests pass identically to the above, demonstrating determinism.

Determinism requirements
- Use the exact IDs in this section. Counts and membership must match precisely for a conforming implementation using the provided dataset.
- If your transport or framework changes ordering, assert on membership and counts rather than list order.

Data Loading and Fixtures
- Provide a deterministic data generator creating the Minimum Viable Dataset with:
  - Multiple envs (prod, staging) and regions to exercise filters.
  - Multi-hop dependsOn and calls chains (depth ≥ 3).
  - Mixed success/failed deployments around incident windows for correlation.
- Loading order (suggested): teams → services → applications → hosts → dependencies → deployments → incidents → alerts → edges linking them.

Upgrade/Portability Considerations
- Exercise a broad set of Gremlin steps and traversal patterns (e.g., aggregate, project, path, repeat/emit, barrier, dedup, select/sack, coalesce, optional, groupCount, order(local), by(), math, where with predicates, subgraph, mergeV/E, profile) to intentionally surface potential upgrade differences; document provider/version nuances and, where practical, supply optional fallbacks.
- Avoid provider-specific features unless guarded with fallbacks.
- Keep label and property names stable; version and evolve via additive changes.
- When deprecating: retain old properties/edges, populate new ones in parallel, and document migration traversals.

Glossary
- CMDB: Configuration Management Database
- Blast Radius: The set of entities affected by an incident/change
- Root Cause Candidates: Upstream dependencies most likely responsible for impact
- Provenance: Metadata indicating how/when a relation was inferred or observed

Appendix: Quick Reference of Labels
Vertices: service, application, host, deployment, dependency, team, incident, alert
Edges (camelCase): runsOn, deployedAs, deploymentOf, deployedTo, dependsOn, ownedBy, triggered, affects, relatedTo, calls

This specification is intentionally framework-agnostic and should be sufficient for an LLM to generate a complete example project in any language and on any version/provider compatible with Apache TinkerPop.