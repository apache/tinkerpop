<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Apache TinkerPop — Threat Model (v0 draft)

## §1 Header

- **Project:** Apache TinkerPop (`apache/tinkerpop`) — a graph computing framework: the Gremlin query
  language, the traversal machine, Gremlin Server (remote query execution), the GraphSON and GraphBinary
  wire serializers and the Gryo IO format, and the Gremlin Language Variants (Java, Python, .NET, Go, JS).
  *(documented — `README.md`, repo layout)*
- **Scope of this model:** the **`apache/tinkerpop` monorepo**, active branch `3.7-dev`. The model focuses on
  the network-facing and deserialization surfaces (see §2). Provider graph databases that make use of
  reference code are out of scope (§3).
- **Date:** 2026-06-05. **Status:** DRAFT v0, a first pass by the ASF Security team via the
  threat-model-producer rubric, for the TinkerPop PMC to react to. **Author:** ASF Security team, for PMC
  ratification.
- **Version binding:** versioned with the project. A report against release *N* is triaged against the
  model as it stood at *N*, not against `HEAD`.
- **Reporting cross-reference:** §8-property violations → report privately per the ASF process
  (`security@apache.org`). §3 / §9 findings are closed citing this document.
- **Provenance legend:** *(documented)* = stated in TinkerPop's own docs / reference / source.
  *(maintainer)* = confirmed by a TinkerPop PMC member through this process. *(inferred)* = reasoned from
  the reference docs, architecture, and graph-server domain knowledge, **not yet confirmed**. Every
  *(inferred)* claim has a matching §14 open question.
- **Draft confidence:** a v0 with **no PMC confirmation folded in yet**. The deployment posture, the
  default auth/TLS stance, the script-execution sandboxing story, and the serialization disposition are the
  highest-value items for the PMC to confirm in §14.
- **What TinkerPop is:** a graph computing framework for graph databases (OLTP) and graph analytic systems
  (OLAP). It **defines** a common interface/language (Gremlin) plus protocols/APIs, and ships
  production-ready reference implementations. This includes a reference in-memory graph (TinkerGraph), Gremlin
  Server, and the language variants (GLVs). Gremlin traversals run in one of two environments. In
  **embedded**, the caller runs traversals directly in their application via `gremlin-core`. In **remote**,
  Gremlin is sent over the wire (as **bytecode** or a **string script**) to a server that executes it.
  *(documented)*

## §2 Scope and intended use

- **Primary use:** TinkerPop runs in two environments (§1). In **embedded**, the caller runs traversals in
  its own JVM via `gremlin-core`, so there is no network trust boundary and the caller owns the whole
  surface. In **remote**, Gremlin is sent over the wire to a server. This model focuses on the **remote**
  case, where **Gremlin Server** is the main network-facing trust boundary. Gremlin Server **is intended to
  be deployable on the public internet**, but the shipped `gremlin-server.yaml` is a getting-started/testing
  config that **must not be deployed as-is**. A public or otherwise untrusted-facing deployment is expected
  to layer on TLS and an authenticator. *(maintainer)*
- **Embedded surface.** Embedded has **no network, transport, auth, or protocol/frame surface** (those exist
  only with a server). It exposes the same input-consuming primitives as remote, and only where the caller
  feeds them untrusted data: the **`io()` readers** (GraphSON, Gryo, GraphML), the **`gremlin-language`
  parser** if untrusted strings are parsed, and **expensive steps** (e.g. `regex`). The responsibility rules
  are **identical to remote** because they attach to the primitive, not the environment. *(maintainer)*
- **Caller roles** (Gremlin Server is a network service):
  - **remote client** — connects over the WebSocket sub-protocol or HTTP, submits Gremlin (script or
    bytecode). Trust level depends entirely on whether the operator enabled authentication, authorization,
    and script restrictions, **all off in the shipped default** (`gremlin-server.yaml` has no
    `authentication` block, and the default is `AllowAllAuthenticator`, per `gremlin-applications.asciidoc`
    "Security"). *(documented)*
  - **application / provider** — runs `gremlin-core` directly, whether embedded in its own process or as
    Gremlin Server. A provider may also supply the `Authenticator`/`Authorizer` implementations the server
    runs. Trusted within the compile-time + configuration boundary. *(maintainer)*
  - **operator** — runs Gremlin Server, controls `gremlin-server.yaml`, serializers, auth, TLS, the script
    engine configuration, and the host. Trusted for the instance. *(maintainer)*

**Component-family table** *(in/out = in/out of this model)*:

| Family | Entry point | Touches outside the process | In model? |
| --- | --- | --- | --- |
| **Gremlin Server** (`gremlin-server`) | WebSocket sub-protocol (`RequestMessage`/`ResponseMessage` frames, GraphBinary/GraphSON), the default, plus a non-default HTTP request endpoint (script **and** bytecode) | network (listens), invokes the script engine + traversal machine | **In**, both transports *(documented: remote execution endpoint, channelizers)* |
| **Script engine** (`gremlin-groovy`) | `GremlinGroovyScriptEngine` evaluating string scripts | runs supplied Groovy in-process | **In**, the central code-execution surface (see §9) *(documented: scripts are evaluated)* |
| **Gremlin language parser** (`gremlin-language`) | ANTLR grammar for string Gremlin (script-engine-free) | — | **In**, parser must not crash/hang/OOM on malformed input, nor let a crafted string break out of a literal to inject steps *(maintainer)* |
| **Core traversal machine + structure API** (`gremlin-core`) | `GraphTraversal`, strategies, `Vertex`/`Edge` | filesystem (IO formats) | **In** *(documented)* |
| **Serialization** (`gremlin-core`/`gremlin-util`/`gremlin-shaded`) | GraphSON + GraphBinary wire readers. Gryo (Kryo) is IO-format-only, not on the wire | deserializes untrusted wire bytes. Gryo only reads IO/file bytes | **In** (see §9) *(documented — `Serializers` enum: GraphSON + GraphBinary)* |
| **Gremlin Language Variants** (Java `gremlin-driver`, `gremlin-python`, `-dotnet`, `-go`, `-js`) | deserialize server responses | network (connect) | **In**, response-deserialization robustness + TLS cert validation. Java shares the JVM serializers. The others have own per-language deserializers not covered by hardening the JVM server *(documented — each GLV ships its own GraphSON/GraphBinary code)* |
| **Reference graph** (`tinkergraph-gremlin`) | in-memory graph, the **shipped default graph**, with optional file persistence via the `io()` readers | filesystem (persistence) | **In**, TinkerPop reference code on the default reachable path, so a defect in unmodified TinkerGraph is `VALID` (§3). Loading a persisted graph uses the same GraphSON/Gryo/GraphML readers (§6 IO surface). A custom `graphFormat` reader is provider code (§3) *(documented — `gremlin-server.yaml` → `tinkergraph-empty.properties`, `AbstractTinkerGraph.loadGraph()`)* |
| **OLAP** (`gremlin-core`/`tinkergraph` computer, `hadoop-gremlin`, `spark-gremlin`) | `GraphComputer`, remote-reachable via `withComputer()` bytecode | network, filesystem, cluster | **In**, reference code (incl. Hadoop/Spark modules). The third-party Hadoop/Spark runtime + cluster config are out (§3) *(documented — `withComputer` is a bytecode source instruction; in-tree modules)* |
| **`gremlin-console`** | local Groovy REPL. `:remote console` submits **arbitrary Groovy scripts** to a server and deserializes responses. `:install` loads plugins (credentials/hadoop/spark) via Grape | network (connects) | Local REPL **Out** (operator-trusted). As a remote client, submitting arbitrary Groovy = server-side RCE **by-design** (§9), subject to server auth. Response-deserialization robustness + TLS cert validation **In** (like a GLV). Plugins are operator-installed code in the operator's own JVM, so the install decision, the third-party download channel (Grape), and plugin behavior are **Out** (trusted-input, §3) *(documented — `DriverRemoteAcceptor`, `InstallCommand`/Grape; `gremlin-applications.asciidoc`)* |
| Test-only / example / build modules (e.g. `gremlin-test`, `gremlin-examples`, `gremlin-tools`, `gremlin-annotations`) | test/build/example code | — | **Out** *(see §3)* |

## §3 Out of scope (explicit non-goals)

- **Provider code and provider modifications to the reference code.** TinkerPop ships production-ready
  **reference code** (`gremlin-core`, `gremlin-server`, `tinkergraph-gremlin`, etc.) that providers
  are free to use in whole, in part, or not at all. The boundary is drawn by **what code the defect
  reproduces in**, not by where that code runs (provider code typically runs in the same process as TinkerPop
  code):
  - **In-model:** a defect reproducible in **unmodified TinkerPop reference code**, even when it is running
    as part of a provider's product.
  - **Out-of-model → route to the provider:** a defect that depends on the **provider's own code or their
    modifications** to the reference code, such as a modified `Channelizer`/`OpProcessor`/`GraphManager`, an
    added Netty handler, their `Graph`/`GraphComputer` implementation, a **`call()` service they register**
    (`ServiceRegistry` is empty by default), or their `Authenticator`/`Authorizer` decision logic.
  - **Hard case (→ `MODEL-GAP`, §12/§13):** a defect that reproduces only under a **mix** of reference and
    provider code and cannot be cleanly attributed to either. This is not silently dropped to the provider.
    It triggers a joint determination and a §12 model revision.

  Triage test: *is the defective code TinkerPop's?* A defect in unmodified TinkerPop reference code is
  in-model, whatever product or graph it runs inside. A defect that depends on the provider's own code or
  modifications is the provider's. If it genuinely can't be told, `MODEL-GAP`. *(maintainer)*
- **The calling application's own authentication / authorization of its end users.** TinkerPop has no
  concept of the calling application's end users. *(maintainer)*
- **Attackers who already control the host, the Gremlin Server process, `gremlin-server.yaml`, or the
  graph data directory.** They have the operator's authority by definition. *(maintainer)*
- **Operator-configured code-execution/side-effect surfaces:** the Script I/O Format (`ScriptInputFormat`/
  `ScriptOutputFormat` run operator-supplied Groovy from HDFS) and `EventStrategy` `MutationListener`
  callbacks. The code comes from operator/provider config, not a remote request (though a remote mutating
  traversal can *trigger* an already-registered listener). Trusted-input. *(documented — Script I/O Format;
  `EventStrategy`)*
- **Any test-only module, any example-only module, and build/distribution tooling** as a production trust
  surface (e.g. `gremlin-test`, `gremlin-examples`, `gremlin-tools`). *(maintainer)*
- **Confidentiality of data in transit when the operator has not enabled TLS** (see §10). TLS is **off by
  default** (`ssl.enabled: false`), so the TLS posture is the operator's deployment responsibility.
  *(documented — `gremlin-server.yaml`, `gremlin-applications.asciidoc` config table)*
- **`neo4j-gremlin`** — deprecated (not compatible past Neo4j 3.4, EOL 2020). *(documented — reference docs)*
- **`sparql-gremlin`** — a standalone SPARQL→Gremlin translator the user invokes in their own application
  code. The server does not use it, and it parses SPARQL via third-party Apache Jena, so malformed SPARQL is
  the caller's responsibility. *(documented — `gremlin-server` has no `sparql-gremlin` dependency; Jena parser)*
- **`gremlint` (incl. hosted gremlint)** — a client-side Gremlin formatter (parse-and-reprint, no query
  execution). When hosted, input stays in the user's own browser (no URL-shared state, no XSS sink, no
  backend), so a user is responsible for what they input, and a pathological input only affects their own
  browser tab. *(documented — `gremlint/src`, `docs/gremlint`)*

## §4 Trust boundaries and data flow

Data flow for a remote request. The `‖` marks the trust boundary, and everything right of it runs on the server.

```
                                          TRUST BOUNDARY
 calling app → GLV (build bytecode /            ‖
   opaque script) → transport ────────────────▶ ‖ transport decode (HTTP / WS frame)
                                                 ‖   → deserialize (GraphSON/GraphBinary) → authenticate
                                                 ‖   → op-select → authorize → execute (script engine /
                                                 ‖       traversal machine) → graph + storage / io() files
 GLV ◀──────── deserialize response ──────────── ‖   → iterate results → encode response
```

- Untrusted input crosses left-to-right at the boundary (request bytes). The response path crosses
  right-to-left back to the GLV (server-response bytes are untrusted from the client's standpoint, §8).
- The **embedded** environment has no boundary, since the caller runs `gremlin-core` in-process (§2).
- The **`io()` / file surface** feeds the graph/storage box from disk, untrusted only if the caller loads
  untrusted files (§6).

- **Primary trust boundary: the Gremlin Server remote request surface.** Bytes arriving over the WebSocket
  sub-protocol or HTTP are untrusted, whether a **string script** or **bytecode traversal**, along with the
  serialized request payload. The script engine, traversal machine, and structure/storage layer sit behind
  this boundary. There are two in-scope transports. The **WebSocket sub-protocol** (default) frames
  `RequestMessage`/`ResponseMessage`, so the **WebSocket protocol layer and frame decoder are a pre-auth
  surface distinct from payload deserialization** — the whole WS state machine (control frames,
  fragmentation, handshake), not just the sub-protocol payload, must be safe (§8 memory-safety covers it).
  **HTTP** (non-default) is a second request transport carrying script *and* bytecode, in-model as TinkerPop
  code, and since it carries scripts the §9 by-design RCE ruling applies to it too. *(documented —
  channelizers, remote endpoint)*
- **The script-execution boundary (the highest-stakes one).** A string-based request is evaluated by the
  Groovy script engine. Absent a configured sandbox / allow-list, **evaluating an attacker-supplied script
  is arbitrary code execution on the server**. This is inherent to the script-based interface, not a bug
  (§9). **No script restriction is on by default.** The shipped `gremlin-server.yaml` runs `gremlin-groovy`
  with no sandbox, and the sandbox controls (`GroovyCompilerGremlinPlugin` with `COMPILE_STATIC` +
  `SimpleSandboxExtension`, `timedInterrupt`) appear only in `gremlin-server-secure.yaml`. The question for
  triage is whether a given deployment restricts scripting (bytecode-only, allow-list, sandbox, or the
  Groovy-free `GremlinLangScriptEngine`) and who is authorized to submit scripts. *(documented —
  `gremlin-server.yaml` vs. `gremlin-server-secure.yaml`, `gremlin-applications.asciidoc` "Protecting Script
  Execution")*
- **The deserialization boundary.** GraphSON and GraphBinary readers parse untrusted request bytes (and, on
  the driver/GLV side, untrusted server-response bytes). Gryo is IO-only (not on the wire). *(documented —
  wire set is GraphSON + GraphBinary)*
- **Reachability preconditions** (the test a triager applies first):
  - A finding reachable only by submitting a **script** is in-model only subject to the §9 ruling: if the
    deployment allows scripting to the principal, server-side code execution is by-design. *(documented)*
  - A finding in the **Gremlin parser / bytecode / traversal machine / deserializers** reachable from a
    client operating within its privileges (or pre-auth) is **in-model** for memory-safety / bounded-
    resource robustness. *(maintainer)*
  - A finding requiring control of `gremlin-server.yaml` or host access is **out-of-model: trusted-input**.
    *(maintainer)*

## §5 Assumptions about the environment

- **Runtime:** JVM (server, core, groovy, driver). The GLVs run on their respective runtimes
  (CPython, .NET, Go, Node). *(documented — README / build)*
- **Deployment:** Gremlin Server is safe enough to run on the public internet **once hardened** beyond the
  shipped `gremlin-server.yaml`, at minimum with TLS plus an authenticator (either built-in mechanism
  suffices over TLS). The insecure getting-started default is a testing convenience, not a supported
  production posture. Realistically, a production deployment also layers a provider's
  `Authenticator`/`Authorizer` on top (no authorizer ships, see §5a), so auth/authz is a **shared
  responsibility**. The provider supplies the policy (the plugin implementations), while Gremlin Server owns
  the enforcement machinery that runs them on every request (§8). *(maintainer)*
- **Filesystem:** the graph data / config directories are private to the server process and not writable by
  untrusted local users. Securing the host is the operator's responsibility, and an attacker who can write
  them already has the operator's authority (§3). *(maintainer)*
- **Concurrency:** the standard request path is isolated per request (thread-pool dispatch, per-request
  script bindings), so one request should not affect another. Sessions are the deliberate stateful
  exception (§11b). *(maintainer)*
- **What the server does to its host** (negative inventory): listens on network ports; reads/writes its
  configured graph + data directories; reads config; **executes supplied Groovy when script requests are
  enabled** *(documented — script evaluation, `gremlin-applications.asciidoc`)*; loads provider graph
  implementations the operator configured. *(maintainer)*

## §5a Build-time and configuration variants

Knobs that change which security properties hold (Gremlin Server, `gremlin-server.yaml`):

- **Authentication** — `Authenticator` (PlainText/SASL against a credentials graph via `SimpleAuthenticator`,
  or `Krb5Authenticator` for Kerberos). Authentication is **off by default** (the shipped `gremlin-server.yaml`
  has no `authentication` block, and the default is `AllowAllAuthenticator`). Both built-in mechanisms,
  username/password (`SimpleAuthenticator`) and Kerberos (`Krb5Authenticator`), are **secure enough for a
  public-internet deployment when combined with TLS**. In practice, though, deployments layer a provider's
  own `Authenticator` on top (e.g. SSO / bearer-token), so the built-ins are more often a baseline than the
  production mechanism. *(documented — `SimpleAuthenticator` + `Krb5Authenticator` ship; auth off by default)*
- **Authorization** — an `Authorizer` SPI exists, but **no implementation ships** with Gremlin Server (the
  only one in-tree, `AllowListAuthorizer`, is a test example), so there is **no meaningful authorization
  control out of the box**, and providing it is expected to fall to the provider. Authorization is
  also **only feasible for bytecode requests**, not script requests (a script request gets full access to
  the execution environment, §9). In-model only when configured, and the default is none. *(documented —
  `gremlin-applications.asciidoc` "Authorization": no impl ships, bytecode-only)*
- **Restricting traversals via `TraversalStrategy`** — an operator/`Authorizer` can constrain a
  `GraphTraversalSource` with `ReadOnlyStrategy` (block mutations), `SubgraphStrategy`/`PartitionStrategy`
  (scope visible data / tenancy), and `VertexProgramDenyStrategy` (block OLAP). These are **traversal-layer,
  not storage-layer**: `PartitionStrategy` is bypassable via direct `Vertex.property()`, and
  `ReservedKeysVerificationStrategy` does not cover `mergeV`/`mergeE`. A client can also remove an applied
  strategy outright, whether by bytecode source instruction (`withoutStrategies()`) or by script, unless
  an `Authorizer` denies it (§9). In-model only when pinned by a configured `Authorizer`. *(documented —
  `the-traversal.asciidoc` "TraversalStrategy"; `gremlin-applications.asciidoc` "Authorization")*
- **TLS/SSL** — configurable on the server connector, **off by default** (`ssl.enabled: false` in the
  shipped `gremlin-server.yaml`, enabled only in `gremlin-server-secure.yaml`). Drives the
  transport-confidentiality property (§9). *(documented)*
- **Audit logging** — `enableAuditLog` records the authenticated user, remote address, and gremlin query.
  It is **off by default** (`false`, "for privacy reasons") and absent from the shipped config, so a default
  deployment keeps no attribution record (§9). *(documented — `gremlin-applications.asciidoc` config table;
  `Settings.enableAuditLog`)*
- **Script execution restriction** — sandbox / compilation customizers / allow-list / preferring
  bytecode-only (plus `LambdaRestrictionStrategy`, pinned by an `Authorizer`, to close the bytecode-lambda
  vector, since unpinned it is removable like any strategy, §9) / the Groovy-free
  `GremlinLangScriptEngine`. **Scripting and bytecode lambdas are unrestricted out of the box:** the shipped
  `gremlin-server.yaml` configures no sandbox and no lambda restriction, and the sandbox controls ship only in
  `gremlin-server-secure.yaml`. The docs are explicit that TinkerPop offers no complete out-of-the-box
  protection against nefarious scripts. *(documented — `gremlin-applications.asciidoc` "Protecting Script
  Execution", the two sample configs)*
- **Enabled serializers** — wire set is GraphSON 3.0 + GraphBinary. Gryo is IO-format-only, not on the wire,
  and defaults to a locked registration allow-list (`registrationRequired=true`). Disabling that lock removes
  the untrusted-input protection (§9). *(documented — sample configs, `gremlin-applications.asciidoc`
  "Serialization")*

## §6 Assumptions about inputs

Per-surface trust table:

| Surface | Input | Attacker-controllable? | Caller/operator must enforce |
| --- | --- | --- | --- |
| Gremlin Server — string script request | Groovy/Gremlin script text | **yes** (pre-auth if auth off) | auth; script restriction / sandbox / bytecode-only; who may script |
| Gremlin Server — bytecode/traversal request | serialized traversal bytecode | **yes**, within privileges | auth; traversal-step allow-list; resource limits |
| Gremlin Server — session id (`SessionOpProcessor`) | client-supplied session string | **yes** | keyed by the string with no owning-user check, so any client presenting the id shares the session (see §11b) |
| Request deserialization (GraphSON / GraphBinary) | serialized bytes | **yes** (pre-auth) | robustness of the wire serializers |
| Graph IO — Gryo/GraphSON/GraphML files (`io()` step, persistence, OLAP) | on-disk / cluster bytes | only if the caller loads untrusted files | GraphSON, locked Gryo (`registrationRequired=true`), and GraphML with the default XML factory owe deserializer integrity. Unlocked Gryo and a caller-supplied unhardened `XMLInputFactory` (XXE) are the caller's responsibility |
| Gremlin string parser (`gremlin-language` ANTLR) | Gremlin string | **yes** | parser robustness, no crash/hang/OOM on malformed input and no grammar breakout / step injection (distinct from execution cost, §8/Q7) |
| Any string the grammar accepts as an argument (e.g. a `regex` pattern) | Gremlin string / bytecode | **yes** | a grammatically valid string must not enable DoS (e.g. ReDoS via a pathological pattern), the Q7 super-linear-amplification carve-out (§8) |
| GLV (client) — server response | serialized bytes from the server | yes if the server is malicious/compromised, or a MITM (TLS off / cert not validated) | response-deserialization robustness; TLS with cert validation |
| `gremlin-server.yaml`, host, data dir | local | no — operator-trusted | filesystem permissions |

- **Deserialization is pre-auth (ordering note):** the server deserializes a request **before**
  authenticating it (§4 diagram), because authentication itself rides inside a deserialized
  `RequestMessage` (the SASL op). The wire deserializers therefore face fully anonymous input on every
  deployment, hardened or not, and a deserializer defect is reachable without credentials, which is what
  makes it security-critical under §8. *(documented — channelizer pipeline order)*
- **Shape / rate:** Gremlin Server has per-request limits (`evaluationTimeout`, `maxContentLength`,
  `maxParameters`, `maxWorkQueueSize`, session timeouts) but **no traversal-depth or result-count cap**.
  The bug-vs-capacity line is the §8 resource split. *(documented — `gremlin-applications.asciidoc` config
  table)*
- **Script-cache growth:** a client submitting many distinct, unparameterized scripts grows the compiled
  script / global-function cache toward `OutOfMemoryError`. The operator mitigates by bounding
  `classMapCacheSpecification` and preferring parameterized scripts. This is the Q7 resource split (a
  documented remote DoS unless bounded). *(documented — `gremlin-applications.asciidoc` "Cache Management")*

## §7 Adversary model

- **Client → server (primary):** the highest-stakes surface (most code, most exposure). A network client
  that can reach the Gremlin Server port from within the deployment, either **unauthenticated** (if auth is
  off / pre-auth) or **authenticated with limited privileges**, trying to execute code on the server (via
  scripts), read/write graph data outside its intent, crash or exhaust the server with malformed requests or
  expensive traversals, or exploit a deserializer. *Capabilities:* can open connections, send arbitrary
  protocol bytes / scripts / bytecode / serialized payloads, and supply large/malformed input. *(maintainer)*
- **Server → client:** a malicious or compromised server, or a network attacker on the wire when TLS is off,
  targeting a GLV through the response it returns — crafted response bytes to crash/OOM/RCE the client, or a
  server-issued auth challenge to harvest the client's credentials (§8/§9). *(maintainer)*
- **Untrusted input → embedded:** in the embedded environment there is no network client, but untrusted data
  can still reach an in-process `gremlin-core` primitive through the calling application — an `io()` reader
  fed an untrusted file, the `gremlin-language` parser fed an untrusted string, or an expensive step fed a
  crafted argument. The relevant robustness properties (§8) apply identically, since they attach to the
  primitive, not the environment (§2/§6). *(maintainer)*
- **Out of scope:** anyone with operator/host/config control (already authoritative). Also an attacker who
  reaches a server left on the **insecure getting-started default** (no TLS, no auth), which is an operator
  hardening failure (§9/§10), not a code bug, though the pre-auth robustness properties (§8) still apply to
  the exposed surface. Side-channel/timing adversaries are **out of scope**. *(maintainer —
  public exposure of a hardened server is in scope, whereas exposure of the insecure default is an operator
  failure)*

## §8 Security properties the project provides

**Assets these properties defend (what is being protected):**

- **Server availability** — the Gremlin Server process staying up under malformed or pre-auth input.
- **Server-side code-execution / host integrity** — not running attacker code beyond what scripting/lambdas
  grant by design (§9).
- **Graph data confidentiality and integrity** — read/write/delete of the graph, bounded by auth/authz and
  traversal strategies (§5a/§9).
- **Credentials** — the server-side credential store and the credentials a client presents (§5a/§9).
- **Client (GLV) integrity** — a client not harmed by hostile server responses.

**Properties:**

- **Enforcement of configured restrictions.** With an `Authenticator`/`Authorizer` set, an
  unauthenticated or unauthorized client cannot execute requests beyond its grants (§11b session sharing
  excepted). The same holds for restrictions living in server configuration that no request can touch: a
  *configured* script sandbox / compilation customizer / allow-list must not be evadable from a request.
  A restrictive `TraversalStrategy` (`ReadOnlyStrategy`, `SubgraphStrategy`, ...) counts as a configured
  restriction **only when an `Authorizer` denies its removal or modification**, because a request can
  otherwise remove strategies itself, whether by bytecode source instruction or by script (§9). This is
  the §8 anchor for the "bypasses a *configured* restriction" rulings in §9/§11a. *Violation symptom:*
  auth/authz bypass, or a request evading a configured restriction. *Severity:* security-critical.
  *(maintainer)*
- **Credential-store handling (when the built-in mechanism is used).** Where a deployment uses
  `SimpleAuthenticator` with the credentials graph, passwords are stored and verified as BCrypt hashes,
  never as plaintext. `Krb5Authenticator` holds no credential store (the KDC does), and a provider's own
  `Authenticator` and credential store are the provider's (§3). *Violation symptom:* plaintext credential
  storage, or disclosure of stored credential material to a client. *Severity:* high. *(documented —
  `SimpleAuthenticator`, credentials DSL)*
- **Memory / availability safety on the request + deserialization surface.** Malformed or pre-auth input
  (protocol frames, Gremlin strings, serialized payloads) yields a clean error, not a crash, OOM, hang, or
  unbounded allocation of the server. This covers not just the sub-protocol payload but the **transport
  layer itself**: any valid WebSocket frame (ping/pong/continuation/close, fragmentation, the upgrade
  handshake) or HTTP request, however crafted or sequenced, must be handled safely, with no path to access
  or DoS the server. *Violation symptom:* server crash / unbounded allocation / deadlock from malformed or
  pre-auth input, or a crafted frame sequence. *Severity:* security-critical (remote DoS) if pre-auth.
  *(maintainer)*
- **Request isolation.** One request's bindings and state must not leak into or alter another's
  (per-request bindings, thread-pool dispatch, §5). Sessions are the deliberate stateful exception
  (§11b). *Violation symptom:* cross-request state leakage or interference. *Severity:*
  security-critical. *(maintainer)*
- **Parser integrity (`gremlin-language`).** A crafted Gremlin string cannot break out of a string literal
  to inject additional traversal steps. This is the safe string-to-traversal path, distinct from building a
  Groovy string by concatenation, which is the calling application's concern (§9). *Violation symptom:*
  grammar breakout / step injection from a value that should stay a literal. *Severity:* critical.
  *(maintainer)*
- **Deserializer integrity.** The wire deserializers (GraphSON, GraphBinary) and **Gryo in its locked default
  (`registrationRequired=true`)** reading attacker bytes do not lead to arbitrary object instantiation / code
  execution beyond the registered type set. Because `inject()` and value arguments let a request carry any
  supported type, a bug in a **registered** type's (de)serializer that crashes/OOMs the reader is also
  in-model, on **both** the server (request) and the GLV (response) side. The GraphML reader disables
  external entities and DTDs by default (XXE-safe). *Violation symptom:* deserialization gadget / RCE / XXE,
  or a registered-type serializer crashing/OOMing either end. *Severity:* critical. Gryo is not on the wire,
  and unlocked Gryo or a caller-supplied unhardened XML factory is out-of-model (user responsibility, §9).
  *(documented)*
- **Resource bounds — split.** Malformed/pre-auth input that crashes/OOMs/hangs the server is **in-model**
  (above). Ordinary expensive traversals / large results are **operator capacity**, NOT in-model, unless a
  specific bug applies (super-linear amplification, a missing-where-expected limit, an unbounded traversal).
  A grammatically valid string the parser accepts must not itself enable DoS from small input, e.g. ReDoS via
  a pathological `regex` pattern, which is the in-model amplification case. *(maintainer)*
- **Client (GLV) robustness against hostile server responses.** A GLV deserializing response bytes from a
  malicious/compromised server (or a MITM) does not crash/OOM/execute code. *Violation symptom:* client
  crash/OOM/RCE from crafted responses. *Severity:* critical. *(maintainer)*
- **No pass-through input can harm the GLV.** Outbound bytecode/message construction is otherwise not a
  surface, because its input is the trusted calling application. This property covers only the
  pass-through case: an application forwarding an untrusted end-user string verbatim (e.g.
  `client.submit(userString)`) must not cause code execution, a crash, a hang, or resource exhaustion
  **in the GLV itself**. Its *server-side* effect is Gremlin-injection, the application's responsibility
  (§9). *Violation symptom:* a submitted string that harms the GLV that submitted it. *Severity:* high.
  *(maintainer)*

## §9 Security properties the project does *not* provide

*(The highest-value section for integrators.)*

- **Script execution is arbitrary code execution by design, not a sandbox.** When string-script requests
  are enabled, the Groovy script engine evaluates attacker-supplied code in the server process. The docs
  state scripts have "access to the full power of their language and the JVM." Submitting a script that runs
  server-side code is **`BY-DESIGN`** for a principal the deployment permits to script. It is the operator's
  job to restrict scripting (bytecode-only, allow-list, sandbox, or the Groovy-free `GremlinLangScriptEngine`)
  and to authenticate who may script. A scan reporting "Gremlin Server allows arbitrary code execution via
  scripts" is by-design unless it bypasses a *configured* restriction. *(documented —
  `gremlin-applications.asciidoc` "Protecting Script Execution")*
- **No transport confidentiality/integrity unless TLS is enabled.** TLS is off in the shipped default (see
  §5a). Until the operator enables it, the server does not defend against a network attacker reading or
  modifying client traffic. On the client side, a GLV sends its credentials in response to a server-issued
  auth challenge, so without TLS (or against a MITM) a driver can disclose those credentials to a hostile
  endpoint. A driver configured without TLS / certificate validation is a misconfiguration and is
  out-of-scope. *(documented — TLS off by default)*
- **No authentication or authorization by default (assumed).** If the shipped/default configuration runs
  with auth off, an exposed server is reachable by anyone on the network. This is an operator deployment
  concern, not a code bug. *(documented)*
- **No attribution/audit by default.** Audit logging is off by default (§5a), so a default deployment keeps
  no record of who ran which query. Enabling it (`enableAuditLog`) is the operator's responsibility (§10).
  *(documented)*
- **No built-in data-access control.** There is no element-level (CRUD) authorization on graph data.
  Access is bounded only by which traversal sources the operator exposes and by a provider-supplied
  `Authorizer` (which does not ship, §5a). A client within its grants can read/write/delete any data the
  exposed traversal source reaches. *(documented — no authorizer ships, authz is traversal-source/step-level)*
- **Gryo / Kryo deserializer integrity holds only under the locked default.** Gryo defaults to a registration
  allow-list (`registrationRequired=true`), so it is not an arbitrary-instantiation sink, and a break within
  that locked config is a `VALID` bug like any deserializer. **Running Gryo unlocked
  (`registrationRequired=false`) is not a safe boundary against untrusted bytes and is the user's
  responsibility.** (A few registered types use Java native serialization, a gadget caveat even when locked.)
  Gryo is not on the wire, so this is an IO/file-surface concern (`io()` step, persistence, OLAP).
  *(documented)*
- **A `TraversalStrategy` is not an access-control boundary on its own.** A remote request can remove or
  replace strategies on its traversal source, whether by bytecode source instruction
  (`withoutStrategies()`) or by script, so a strategy applied by the operator only restricts a client
  when an `Authorizer` denies strategy removal/modification, as the reference documentation states.
  Strategies also act at the traversal layer, not the storage layer (§5a). *(documented — `Bytecode`
  source instructions; `gremlin-applications.asciidoc` "Authorization")*
- **Ordinary resource exhaustion is not a defended property.** Expensive traversals / large results that
  consume CPU/memory are an operator capacity concern unless a specific bug applies (§8). *(maintainer)*
- **No defense against a malicious operator / host.** *(maintainer)*
- **False friends:**
  - "Authentication is available" does **not** mean it is **on**. It must be configured (§5a). *(maintainer)*
  - **Bytecode is safer than scripts but is not a sandbox.** A bytecode traversal still executes traversal
    steps server-side. Injection/abuse via a calling application that builds a Gremlin string by concatenating
    its own untrusted input (e.g. into Groovy) is the calling application's responsibility. Note this is
    distinct from a `gremlin-language` parser breakout, which is an in-model TinkerPop bug (§8). *(maintainer)*
  - **Bytecode-only is not RCE-free unless lambdas are restricted.** A bytecode traversal can carry a
    **string lambda** that is script-evaluated server-side by a `GremlinScriptEngine`.
    `LambdaRestrictionStrategy` blocks this but is **not enabled by default**, and, being a strategy, is
    removable by the request itself unless an `Authorizer` pins it (§8). Not enabling and pinning it means
    the operator/provider accepts the code-execution risk (`BY-DESIGN`, like scripting, §9).
    *(documented — `Lambda`, `LambdaRestrictionStrategy`)*
  - "Gremlin is just a query language" is misleading. The **string** form can run through a Groovy engine, so
    it is code, not a constrained query. *(maintainer)*

## §10 Downstream responsibilities (operator/deployer)

- Do **not** deploy the shipped getting-started `gremlin-server.yaml` as-is. Before exposing Gremlin Server
  on a public/untrusted network, harden it: enable TLS and an authenticator, and (given no authorizer ships)
  supply an `Authorizer`, especially if scripting is enabled. *(maintainer)*
- Enable authentication (`Authenticator`) and authorization (`Authorizer`) for any non-trivial deployment.
  *(documented)*
- **Restrict script execution:** prefer bytecode-based traversals (or the Groovy-free
  `GremlinLangScriptEngine`). If Groovy scripts are needed, apply the "Protecting Script Execution" controls
  (sandbox / compilation customizers / allow-list) and restrict who may submit scripts. **For bytecode,
  enable `LambdaRestrictionStrategy` and pin it with an `Authorizer` that denies strategy removal**, since
  bytecode-only is not RCE-free while string lambdas are allowed, and an unpinned strategy is removable by
  the request itself (§9). *(documented)*
- Enable TLS (off by default) where traffic crosses an untrusted segment. *(documented)*
- Enable audit logging (`enableAuditLog`, off by default) where attribution of requests is needed. *(documented)*
- Use the wire serializers (GraphBinary recommended for drivers). Keep Gryo locked
  (`registrationRequired=true`), and do not read untrusted files through **unlocked** Gryo. *(documented)*
- Apply per-request resource limits (`evaluationTimeout`, `maxContentLength`, etc.) appropriate to capacity.
  *(documented)*
- Set filesystem permissions so only the server user can read the config / data directories. *(maintainer)*

## §11 Known misuse patterns

*(Draft one-liners, expand before publishing.)*

- Exposing the shipped getting-started default (no TLS, no auth) on an untrusted network instead of
  hardening it first, especially with scripting enabled. *(maintainer)*
- Treating the script-engine sandbox as a complete RCE boundary rather than restricting who may script.
  *(maintainer)*
- Building Gremlin by concatenating the calling application's untrusted input into a Groovy string
  (Gremlin-injection). This is distinct from a `gremlin-language` parser breakout, which is in-model (§8).
  *(maintainer)*
- Reading untrusted files through **unlocked** Gryo (`registrationRequired=false`). *(documented)*

## §11a Known non-findings (recurring false positives)

*(Inferred unless tagged. The PMC's confirmations here are the highest-leverage suppression input.)*

- "Gremlin Server executes arbitrary code via scripts" — by-design when scripting is enabled for the
  principal (§9), and not a finding unless it bypasses a *configured* restriction. Disposition:
  `BY-DESIGN: property-disclaimed`. *(documented)*
- "Bytecode lambdas enable code execution" — a string lambda is server-side code like any script, so this
  is by-design when lambdas are permitted (§9), and not a finding unless it bypasses a *configured*
  restriction. Disposition: `BY-DESIGN: property-disclaimed`. *(documented)*
- "No authentication / no TLS by default" — operator deployment responsibility (§9/§10), not a code bug in
  itself. Disposition: `BY-DESIGN: property-disclaimed`. *(documented)*
- "Gryo/Kryo deserialization can be exploited" — Gryo is not on the wire, so a request-path report is
  factually mistaken and needs no disposition (§13). Under the locked default
  (`registrationRequired=true`) a real break is `VALID`, whereas exploiting **unlocked** Gryo on
  untrusted files is the user's responsibility (§9/§10). Disposition for the unlocked-file variant:
  `BY-DESIGN: property-disclaimed`. *(documented)*
- "Expensive traversal consumes CPU/memory" — operator capacity concern, unless a specific bug applies
  (§8/§9). Disposition: `BY-DESIGN: property-disclaimed`. *(maintainer)*
- "A configured `ReadOnlyStrategy` / `SubgraphStrategy` / `PartitionStrategy` was bypassed" — whether by
  removing the strategy (bytecode `withoutStrategies()` or script) or by stepping around it (direct
  `Vertex.property()`, `mergeV`/`mergeE`), a strategy is not an access-control boundary on its own (§9).
  A finding only exists where a configured `Authorizer` pins the strategy and the pin is evaded (§8).
  Disposition: `BY-DESIGN: property-disclaimed`. *(documented)*
- "Calling application concatenates untrusted input into a Groovy string (Gremlin-injection)" — the calling
  application's responsibility (§9). A `gremlin-language` parser breakout is the opposite: an in-model
  TinkerPop bug (§8). Disposition: `BY-DESIGN: property-disclaimed`. *(maintainer)*
- Findings in any test-only module, any module containing examples, or build tooling (e.g. `gremlin-test`,
  `gremlin-examples`, `gremlin-tools`) — out of scope (§3). Disposition:
  `OUT-OF-MODEL: unsupported-component`. *(maintainer)*

## §11b Known problems

*(Acknowledged, accepted weaknesses of Gremlin Server as it works today.)*

- **Sessions can be shared.** A session (`SessionOpProcessor`) is identified by a client-supplied string and
  keyed with **no owning-user check**, so any client presenting the id accesses the session's state. Sessions
  predate authentication and exist even with no users. The default server provides no per-user isolation, and
  this is accepted as how Gremlin Server works. A deployment needing isolation must use a provider that adds
  it. *(documented — `SessionOpProcessor`, `Session`)*

## §12 Conditions that would change this model

- A change to the default auth / TLS / scripting posture (e.g. auth-on-by-default, scripts-off-by-default).
- A new client-reachable surface or protocol on Gremlin Server.
- A change to the default-enabled wire serializer set, or the reintroduction of Gryo (or any Kryo-based
  format) as a wire serializer.
- Promoting a test-only / example module into a production trust surface.
- A change to the OLAP (Hadoop/Spark) trust posture.
- A report that cannot be routed to a single §13 disposition (→ revise the model).

## §13 Triage dispositions

A disposition is the answer a security report receives after triage: a TinkerPop bug that needs a fix,
some other party's responsibility, or known and accepted behavior. A report may contain more than one
finding, and each finding gets **exactly one** disposition from the table below. The rows do not
overlap. A finding that seems to match two rows, or none, means the table itself has a defect and takes
`MODEL-GAP` so that the model gets fixed (§12). A report claiming something the code simply does not do
(for example, Gryo reachable over the network) is answered by correcting the facts and needs no
disposition at all.

A finding is classified by **what the attack needs in order to work**, not by the setup it happened to
be demonstrated on. For example, a crash bug shown against a server running the insecure
getting-started configuration does not need that configuration. The same crash works, without
credentials, against a hardened server too, so it is `VALID`. The insecure default decides the outcome
only when the finding is, in substance, "this particular server was left unhardened."

The lists in §11a and §11b exist to save repeated work: each entry is a finding that keeps coming back,
recorded together with the disposition it takes. They are worked examples of this table, not extra
rules on top of it.

| Disposition | Meaning | Based on |
| --- | --- | --- |
| `VALID` | A real TinkerPop bug: it breaks one of the §8 security properties through an attacker or input the model covers. Examples: bypassing authentication/authorization or slipping past a configured restriction, crashing or hanging the server with malformed or unauthenticated input, a deserializer executing code it should not, or the parser mishandling crafted input or letting a value escape its string literal. | §8, §6, §7 |
| `VALID-HARDENING` | Not a vulnerability, but still useful: no §8 property is broken, and the report points at a practical change that would make one of the §11 misuse patterns harder to fall into. Example: "no authentication by default" is by design, but a startup warning when `AllowAllAuthenticator` is active would be a sensible outcome of that report and takes this disposition. Tracked as an ordinary improvement. | §11 |
| `OUT-OF-MODEL: trusted-input` | The attack only works for someone who already has the operator's own access: the host, the server process, `gremlin-server.yaml`, or the data directory. This includes a malicious operator (§9). Someone with that access can already do anything the operator can, so the attack gains nothing new. | §6, §7 |
| `OUT-OF-MODEL: adversary-not-in-scope` | The attack needs an attacker the model deliberately leaves out (side channels/timing), or the whole finding is that one specific deployment was left on the insecure getting-started default, which is that operator's mistake to fix. A complaint about the shipped default itself is `BY-DESIGN`. A real code bug that was merely demonstrated on an unhardened server is judged on its own needs, usually `VALID`. | §3, §7 |
| `OUT-OF-MODEL: unsupported-component` | The buggy code is not TinkerPop's (§3 triage test): it is in a test-only or example module, in build tooling, or in a provider's own code or their modifications to the reference code (including a remote graph provider that shares no TinkerPop code). Sent to the provider. | §3 |
| `OUT-OF-MODEL: non-default-build` | A genuine bug, but one that only exists when the operator turns on a discouraged, non-default §5a setting, for example a defect that only manifests with Gryo's registration lock disabled. Different from `BY-DESIGN`: behavior §9 already disclaims, such as scripts running code, is not a bug at all and takes that row instead. | §5a |
| `BY-DESIGN: property-disclaimed` | The behavior is exactly what §9 says TinkerPop does not defend against: scripts or bytecode lambdas executing code within what the deployment allows, the shipped no-TLS/no-auth default, ordinary resource exhaustion from expensive queries, reading untrusted files through unlocked Gryo, or an application building queries out of its own users' raw input. The software works as documented. When the scenario needs the operator's own access, `trusted-input` applies instead. | §9 |
| `KNOWN-PROBLEM` | Matches the acknowledged weakness in §11b (session sharing), which the §8 enforcement property explicitly excepts. Already known and accepted, not a new finding. A deployment that needs the missing protection must use a provider that adds it. | §11b |
| `MODEL-GAP` | Fits no row, or more than one. That is a defect in the model itself, and §12 requires revising it. | §12 |

## §14 Open questions for the maintainers

1. **Deployment posture.** ~~Is "Gremlin Server inside a trusted network, behind the app tier, not directly
   public" the right §2/§5 framing?~~ **RESOLVED (maintainer):** Gremlin Server *is* intended to be
   deployable on the public internet, but only after hardening beyond the shipped getting-started
   `gremlin-server.yaml`, meaning TLS plus an authenticator (either built-in mechanism is adequate over TLS).
   Reaching a server left on the insecure default is an operator hardening failure (`OUT-OF-MODEL`), not
   evidence of a trusted-network assumption. Auth/authz is a shared responsibility. Providers typically
   supply the `Authenticator`/`Authorizer` policy (no authorizer ships), while Gremlin Server owns the
   enforcement machinery (§2/§5/§5a/§7/§8/§10).
2. **Default auth/authz posture.** ~~Do the shipped/default Gremlin Server configs run with authentication
   and authorization **off**?~~ **RESOLVED (documented):** yes. `gremlin-server.yaml` has no
   `authentication` block (default `AllowAllAuthenticator`) and ships no authorizer. The docs state "Gremlin
   Server is configured to allow all requests to be processed (i.e. no authentication)" and "not shipped
   with `Authorizer` implementations" (`gremlin-applications.asciidoc` "Security"). Reaching a server left
   on that default is an operator hardening failure (`OUT-OF-MODEL`), not a code bug (per §14 Q1). The
   framework's duty to *run* a configured authenticator/authorizer remains an in-model §8 property.
3. **Provider boundary.** ~~Confirm that vulnerabilities in third-party provider graph databases route to
   those providers.~~ **RESOLVED (maintainer):** the boundary is drawn by **what code the defect reproduces
   in**, not where it runs. Unmodified TinkerPop **reference code** (`gremlin-core`, `gremlin-server`, the
   reference graph, serializers) is in-model even when running as part of a provider's product. A defect
   requiring the **provider's own code or their modifications** routes to the provider. This includes a
   **remote graph provider** that shares no TinkerPop code and only conforms to the WebSocket sub-protocol +
   serializer specs. A defect reproducible only under a **mix** of the two is `MODEL-GAP` (joint
   determination, not silently the provider's). Triage test: *does it reproduce on the unmodified reference
   distribution?* (§3).
4. **Script execution (highest-stakes).** ~~Confirm that string-script evaluation = server-side code
   execution by design, that restricting it (sandbox / allow-list / bytecode-only / who-may-script) is the
   operator's responsibility, and what restriction (if any) is **on by default**. Is "Gremlin Server runs
   arbitrary code via scripts" `BY-DESIGN` unless a *configured* restriction is bypassed?~~ **RESOLVED
   (documented):** yes, by-design, and no restriction on by default (§4/§5a/§9/§10/§11a).
5. **TLS default.** ~~Is TLS **off by default** on the server connector? Is no-TLS-by-default an operator
   responsibility (§9/§10)?~~ **RESOLVED (documented):** yes, `ssl.enabled: false` in the shipped
   `gremlin-server.yaml`, and the operator enables it (§3/§5a/§9/§10).
6. **Serialization / Gryo.** ~~Which serializers are registered by default?~~ **RESOLVED (documented):** the
   wire set is GraphSON 3.0 + GraphBinary. Gryo is IO-format-only (not on the wire) and defaults to a locked
   registration allow-list (`registrationRequired=true`). A Gryo request-path report is a non-finding. A
   flaw in a wire serializer or in **locked** Gryo is `VALID`, whereas **unlocked** Gryo on untrusted files
   is the user's responsibility (§2/§4/§5a/§6/§8/§9/§10/§11a).
7. **Resource line.** ~~Confirm the split, and whether built-in per-request limits exist.~~ **RESOLVED:**
   split confirmed *(maintainer)*. Malformed/pre-auth crash/OOM/hang is `VALID`. Ordinary expensive
   traversals / large results are operator capacity unless a specific bug applies (super-linear
   amplification, missing-where-expected limit, unbounded traversal/recursion). Built-in per-request limits
   exist (`evaluationTimeout`, `maxContentLength`, `maxParameters`, `maxWorkQueueSize`, session timeouts),
   with **no** traversal-depth or result-count cap *(documented)* (§6/§8/§9/§10/§11a).
8. **Parser robustness.** ~~Confirm parser/bytecode memory-safety against malformed input is a committed
   property.~~ **RESOLVED (maintainer):** yes, the `gremlin-language` parser and bytecode path must not
   crash/hang/OOM on **malformed input**, and a crafted string must not break out of a literal to inject
   steps (`VALID`, §8). This covers parsing robustness and integrity. The *execution* cost of a well-formed
   query is the Q7 capacity split, where an accidental/amplified OOM from a small query is in-model but a
   legitimately huge query is operator capacity. Injection via Groovy string-concatenation in the calling
   application is out-of-model (§9/§11a).
9. **GLV scope.** ~~Are the GLVs in scope, or deferred?~~ **RESOLVED (maintainer):** all GLVs (Java
   `gremlin-driver` included, since it is the Java GLV) are in scope. The in-model client surface is
   **response-deserialization robustness against a malicious/compromised server or MITM, plus TLS cert
   validation** (§8). Outbound bytecode/message construction is **not** a surface, because its input is the
   trusted calling application. Feeding untrusted end-user input into a traversal/script is Gremlin-injection
   and the calling application's responsibility (§9/§11a). Sending bad bytes *to* the server folds into the
   server's §8 request-robustness (§2/§6/§8).
10. **OLAP scope.** ~~Are `hadoop-gremlin` / `spark-gremlin` in scope, or operator infra?~~ **RESOLVED
    (maintainer):** OLAP is **in-model as TinkerPop reference code**, covering the `gremlin-core`/`tinkergraph`
    computer *and* the `hadoop-gremlin`/`spark-gremlin` reference modules (per the Q3 reference-code rule),
    reachable remotely via `withComputer()` bytecode (not via the ANTLR string grammar). Only the
    **third-party Hadoop/Spark runtime and the cluster deployment/config** are out-of-model (§3). The Gryo
    untrusted-file risk on this IO surface stays as in Q6 (§2/§3/§6).
11. **§11a seeds.** Generalized the test/example/tooling non-finding to a **category** (any test-only or
    example module, not a fixed name list). Remains a **standing invitation** for the PMC to append further
    recurring false-positives (§11a).
12. **Canonical location.** ~~Confirm this model lives as root `THREAT_MODEL.md`, referenced from
    `SECURITY.md`, wired from `AGENTS.md`.~~ **RESOLVED (documented):** verified. Root `THREAT_MODEL.md`,
    referenced from `SECURITY.md`, which is wired from `AGENTS.md`.

## §15 Machine-readable companion

Deferred for v0. A `threat-model.yaml` can later encode the §6 trust table, §2/§3 component scoping, §8
property/severity/symptom rows, §9 false friends, §11a non-findings, and §13 dispositions for automated
triage.
