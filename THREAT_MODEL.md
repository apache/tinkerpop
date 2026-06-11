<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Apache TinkerPop — Threat Model (v0 draft)

## §1 Header

- **Project:** Apache TinkerPop (`apache/tinkerpop`) — a graph computing framework: the Gremlin query
  language, the traversal machine, Gremlin Server (remote query execution), the GraphSON / Gryo /
  GraphBinary serialization formats, and the Gremlin Language Variants (Java, Python, .NET, Go, JS).
  *(documented — `README.md`, repo layout)*
- **Scope of this model:** the **`apache/tinkerpop` monorepo**, active branches `master`, `3.7-dev`,
  `3.8-dev` *(maintainer — colegreer, scope confirmation)*. The model focuses on the network-facing and
  deserialization surfaces (see §2); provider graph databases that embed TinkerPop are out of scope (§3).
- **Date:** 2026-06-05. **Status:** DRAFT v0 — first pass by the ASF Security team via the
  threat-model-producer rubric, for the TinkerPop PMC to react to. **Author:** ASF Security team, for PMC
  ratification.
- **Version binding:** versioned with the project; a report against release *N* is triaged against the
  model as it stood at *N*, not against `HEAD`.
- **Reporting cross-reference:** §8-property violations → report privately per the ASF process
  (`security@apache.org`); §3 / §9 findings are closed citing this document.
- **Provenance legend:** *(documented)* = stated in TinkerPop's own docs / reference / source;
  *(maintainer)* = confirmed by a TinkerPop PMC member through this process; *(inferred)* = reasoned from
  the reference docs, architecture, and graph-server domain knowledge, **not yet confirmed** — every
  *(inferred)* claim has a matching §14 open question.
- **Draft confidence:** a v0 with **no PMC confirmation folded in yet** — the deployment posture, the
  default auth/TLS stance, the script-execution sandboxing story, and the serialization (Gryo)
  disposition are the highest-value items for the PMC to confirm in §14.
- **What TinkerPop is:** TinkerPop is an embeddable graph-computing framework. Applications and graph
  databases (providers) embed `gremlin-core` to run Gremlin traversals; **Gremlin Server** exposes that
  capability over the network (WebSocket sub-protocol + HTTP), accepting both **string-based Gremlin
  scripts** (evaluated by the `gremlin-groovy` Groovy script engine) and **bytecode-based traversals**
  (the GLVs). *(documented — reference docs)*

## §2 Scope and intended use

- **Primary use:** an operator-deployed graph-traversal engine. In production it is typically embedded by
  a graph database (a "provider") or run as **Gremlin Server** behind the application tier, on a
  **trusted network** *(inferred — confirm deployment posture, §14 Q1)*. Gremlin Server is the main
  network-facing trust boundary.
- **Caller roles** (Gremlin Server is a network service):
  - **remote client** — connects over the WebSocket sub-protocol or HTTP, submits Gremlin (script or
    bytecode). Trust level depends entirely on whether the operator enabled authentication, authorization,
    and script restrictions. *(inferred — §14 Q2)*
  - **embedding application / provider** — links `gremlin-core` in-process and drives traversals directly.
    Trusted within the compile-time + configuration boundary. *(inferred)*
  - **operator** — runs Gremlin Server, controls `gremlin-server.yaml`, serializers, auth, TLS, the script
    engine configuration, and the host. Trusted for the instance. *(inferred)*

**Component-family table** *(in/out = in/out of this model; all rows inferred unless noted)*:

| Family | Entry point | Touches outside the process | In model? |
| --- | --- | --- | --- |
| **Gremlin Server** (`gremlin-server`) | WebSocket sub-protocol + HTTP request handlers | network (listens), invokes the script engine + traversal machine | **In** *(documented: remote execution endpoint)* |
| **Script engine** (`gremlin-groovy`) | `GremlinGroovyScriptEngine` evaluating string scripts | runs supplied Groovy in-process | **In — central code-execution surface; see §9** *(documented: scripts are evaluated)* |
| **Gremlin language parser** (`gremlin-language`) | ANTLR grammar for string Gremlin (script-engine-free) | — | **In** — parser robustness on untrusted Gremlin strings *(inferred)* |
| **Core traversal machine + structure API** (`gremlin-core`) | `GraphTraversal`, strategies, `Vertex`/`Edge` | filesystem (IO formats) | **In** *(documented)* |
| **Serialization** (`gremlin-core`/`gremlin-util`/`gremlin-shaded`) | GraphSON (JSON), GraphBinary, **Gryo (Kryo-based)** readers | deserializes wire/file bytes | **In — deserialization of untrusted bytes; see §9** *(documented: 3 formats; Gryo wraps shaded Kryo)* |
| **Java driver** (`gremlin-driver`) | client connecting to Gremlin Server; deserializes server responses | network (connects) | **In** (client side) *(inferred)* |
| **Gremlin Language Variants** (`gremlin-python`, `-dotnet`, `-go`, `-js`) | build bytecode, serialize/deserialize | network (connect) | **In, lower priority** — client-side; deserialize server responses *(inferred — §14 Q9)* |
| **Reference graph** (`tinkergraph-gremlin`) | in-memory graph implementation | filesystem (persistence option) | **In** — the reference provider *(inferred)* |
| **OLAP** (`hadoop-gremlin`, `spark-gremlin`) | `GraphComputer` over Hadoop/Spark | network, filesystem, cluster | **In if deployed; operator cluster infra** *(inferred — §14 Q10)* |
| **Operator tooling** (`gremlin-console`) | interactive REPL run by the operator | — | **Out** — operator-trusted local tool *(inferred)* |
| `gremlin-test`, `gremlin-examples`, `gremlin-tools`, `gremlin-annotations` | test/build/example code | — | **Out** *(see §3)* |

## §3 Out of scope (explicit non-goals)

- **Provider graph databases that embed TinkerPop** (e.g. third-party graph DBs implementing the
  structure/`GraphComputer` SPI). A vulnerability in a provider's own code is routed to that provider, not
  here; this model covers TinkerPop's own code and its *use* of the SPI. *(inferred — §14 Q3)*
- **The embedding application's own authentication / authorization of its end users.** TinkerPop has no
  concept of the embedding application's end users. *(inferred)*
- **Attackers who already control the host, the Gremlin Server process, `gremlin-server.yaml`, or the
  graph data directory.** They have the operator's authority by definition. *(inferred)*
- **`gremlin-test/`, `gremlin-examples/`, build/distribution tooling, `gremlin-console`** as a production
  trust surface. *(inferred)*
- **Confidentiality of data in transit when the operator has not enabled TLS** — see §10; the TLS posture
  is the operator's deployment responsibility unless the project claims TLS-by-default (§14 Q5). *(inferred)*

## §4 Trust boundaries and data flow

- **Primary trust boundary: the Gremlin Server remote request surface.** Bytes arriving over the WebSocket
  sub-protocol or HTTP — whether a **string script** or **bytecode traversal**, plus the serialized request
  payload — are untrusted. The script engine, traversal machine, and structure/storage layer sit behind
  this boundary. *(documented: remote endpoint; trust posture inferred — §14 Q2)*
- **The script-execution boundary (the highest-stakes one).** A string-based request is evaluated by the
  Groovy script engine. Absent a configured sandbox / allow-list, **evaluating an attacker-supplied script
  is arbitrary code execution on the server** — this is inherent to the script-based interface, not a bug
  (§9). The question for triage is whether a given deployment restricts scripting (bytecode-only,
  allow-list, sandbox) and who is authorized to submit scripts. *(documented: scripts are evaluated;
  sandbox specifics inferred — §14 Q4)*
- **The deserialization boundary.** GraphSON, GraphBinary, and Gryo readers parse untrusted request bytes
  (and, on the driver/GLV side, untrusted server-response bytes). Gryo is Kryo-based; Kryo deserialization
  of untrusted input is a well-known RCE class unless type registration is locked down. *(documented:
  formats exist; default-enabled set + Gryo hardening inferred — §14 Q6)*
- **Reachability preconditions** (the test a triager applies first):
  - A finding reachable only by submitting a **script** is in-model only subject to the §9 ruling: if the
    deployment allows scripting to the principal, server-side code execution is by-design. *(inferred)*
  - A finding in the **Gremlin parser / bytecode / traversal machine / deserializers** reachable from a
    client operating within its privileges (or pre-auth) is **in-model** for memory-safety / bounded-
    resource robustness. *(inferred)*
  - A finding requiring control of `gremlin-server.yaml` or host access is **out-of-model: trusted-input**.
    *(inferred)*

## §5 Assumptions about the environment

- **Runtime:** JVM (server, core, groovy, driver); the GLVs run on their respective runtimes
  (CPython, .NET, Go, Node). *(documented — README / build)*
- **Deployment:** Gremlin Server is assumed to run inside a **trusted network**, fronted by the
  application tier, not exposed directly to the public internet. *(inferred — §14 Q1)*
- **Filesystem:** the graph data / config directories are private to the server process and not writable by
  untrusted local users. *(inferred)*
- **Concurrency:** the server is multi-threaded and serves concurrent sessions; thread-safety of the
  traversal/IO path is a correctness assumption. *(inferred)*
- **What the server does to its host** (negative inventory — predominantly inferred, a confirmation
  target): listens on network ports; reads/writes its configured graph + data directories; reads config;
  **executes supplied Groovy when script requests are enabled**; loads provider graph implementations the
  operator configured. *(inferred — §14 Q4)*

## §5a Build-time and configuration variants

Knobs that change which security properties hold (Gremlin Server, `gremlin-server.yaml`):

- **Authentication** — `Authenticator` (PlainText/SASL with a credentials graph; `Krb5Authenticator` for
  Kerberos). Whether authentication is **off by default** (the shipped sample/getting-started configs) is
  the key question. *(documented: mechanisms exist; default-off inferred — §14 Q2)*
- **Authorization** — an `Authorizer` interface (e.g. allow-listing which Gremlin a principal may run).
  In-model only when configured; default likely none. *(documented: section exists; default inferred — §14 Q2)*
- **TLS/SSL** — configurable on the server connector; whether it is **off by default** drives the
  transport-confidentiality property (§9). *(inferred — §14 Q5)*
- **Script execution restriction** — "Protecting Script Execution": sandbox / compilation customizers /
  allow-list / preferring bytecode-only. Whether any restriction is on by default, or scripting is
  unrestricted out of the box, is the single highest-stakes config question. *(documented: guidance exists;
  default inferred — §14 Q4)*
- **Enabled serializers** — which of GraphSON / GraphBinary / **Gryo** are registered by default, and
  whether Gryo (Kryo) is locked to registered types. *(documented: formats exist; defaults inferred — §14 Q6)*

## §6 Assumptions about inputs

Per-surface trust table *(inferred unless noted)*:

| Surface | Input | Attacker-controllable? | Caller/operator must enforce |
| --- | --- | --- | --- |
| Gremlin Server — string script request | Groovy/Gremlin script text | **yes** (pre-auth if auth off) | auth; script restriction / sandbox / bytecode-only; who may script |
| Gremlin Server — bytecode/traversal request | serialized traversal bytecode | **yes**, within privileges | auth; traversal-step allow-list; resource limits |
| Request deserialization (GraphSON / GraphBinary / Gryo) | serialized bytes | **yes** (pre-auth) | enabled-serializer choice; Gryo type-registration lockdown |
| Gremlin string parser (`gremlin-language` ANTLR) | Gremlin string | **yes** | parser robustness (no crash/OOM/hang) |
| Driver / GLV — server response | serialized bytes from the server | yes if the server (or a MITM without TLS) is hostile | TLS; trust in the server |
| `gremlin-server.yaml`, host, data dir | local | no — operator-trusted | filesystem permissions |

- **Shape / rate:** whether Gremlin Server bounds per-request CPU/memory, result-set size, traversal depth,
  or concurrent requests — and the line between a bug and operator-managed capacity — is an open item
  (§8 resource line, §14 Q7). *(inferred)*

## §7 Adversary model

- **Primary adversary:** a network client that can reach the Gremlin Server port from within the deployment
  — either **unauthenticated** (if auth is off / pre-auth) or **authenticated with limited privileges** —
  trying to execute code on the server (via scripts), read/write graph data outside its intent, crash or
  exhaust the server with malformed requests or expensive traversals, or exploit a deserializer. *(inferred
  — §14 Q2/Q7)*
- **Capabilities assumed:** can open connections, send arbitrary protocol bytes / scripts / bytecode /
  serialized payloads, and supply large/malformed input. *(inferred)*
- **Out of scope:** anyone with operator/host/config control (already authoritative); a client that only
  reaches the server because it was directly publicly exposed against guidance (non-supported posture, §3);
  side-channel/timing adversaries unless the PMC wants them in. *(inferred — §14 Q1)*

## §8 Security properties the project provides

*(Inferred pending PMC confirmation — a property only counts once the project commits to it.)*

- **Authentication + authorization enforcement (when configured).** With an `Authenticator`/`Authorizer`
  set, an unauthenticated or unauthorized client cannot execute requests beyond its grants. *Violation
  symptom:* auth/authz bypass. *Severity:* security-critical. *(inferred — §14 Q2)*
- **Memory / availability safety on the request + deserialization surface.** Malformed or pre-auth input
  (protocol frames, Gremlin strings, serialized payloads) yields a clean error, not a crash, OOM, hang, or
  unbounded allocation of the server. *Violation symptom:* server crash / unbounded allocation / deadlock
  from malformed or pre-auth input. *Severity:* security-critical (remote DoS) if pre-auth. *(inferred —
  §14 Q7)*
- **Deserializer integrity.** GraphSON / GraphBinary / Gryo reading attacker bytes does not lead to
  arbitrary object instantiation / code execution beyond the documented type set. *Violation symptom:*
  deserialization gadget / RCE. *Severity:* critical. **The strength of this property for Gryo (Kryo)
  depends on type-registration lockdown** — see §9 / §14 Q6. *(inferred)*
- **Resource bounds — split, not unspecified.** Malformed/pre-auth input that crashes/OOMs/hangs the server
  is **in-model** (above). Ordinary expensive traversals are **operator capacity/resource management**, NOT
  in-model — unless a specific bug applies (super-linear amplification, a missing limit where one is
  expected, an unbounded traversal). *(inferred — §14 Q7)*

## §9 Security properties the project does *not* provide

*(The highest-value section for integrators — inferred unless tagged; confirm each.)*

- **Script execution is arbitrary code execution by design — not a sandbox.** When string-script requests
  are enabled, the Groovy script engine evaluates attacker-supplied code in the server process. Submitting
  a script that runs server-side code is **`BY-DESIGN`** for a principal the deployment permits to script;
  it is the operator's job to restrict scripting (bytecode-only, allow-list, sandbox) and to authenticate
  who may script. A scan reporting "Gremlin Server allows arbitrary code execution via scripts" is
  by-design unless it bypasses a *configured* restriction. *(inferred — §14 Q4)*
- **No transport confidentiality/integrity unless TLS is enabled.** If TLS is off (see §5a/§14 Q5), the
  server does not defend against a network attacker reading/modifying client traffic. *(inferred)*
- **No authentication or authorization by default (assumed).** If the shipped/default configuration runs
  with auth off, an exposed server is reachable by anyone on the network — an operator deployment concern,
  not a code bug. *(inferred — §14 Q2)*
- **Gryo / Kryo deserialization is not a safe boundary against untrusted input** unless type registration is
  locked down. Operators who enable Gryo on an untrusted surface own that risk; prefer GraphBinary. *(inferred
  — §14 Q6)*
- **Ordinary resource exhaustion is not a defended property.** Expensive traversals / large results that
  consume CPU/memory are an operator capacity concern unless a specific bug applies (§8). *(inferred — §14 Q7)*
- **No defense against a malicious operator / host.** *(inferred)*
- **False friends:**
  - "Authentication is available" does **not** mean it is **on** — it must be configured (§5a). *(inferred)*
  - **Bytecode is safer than scripts but is not a sandbox** — a bytecode traversal still executes traversal
    steps server-side; injection/abuse via an embedding app that builds traversals from its own untrusted
    input is the embedding app's responsibility. *(inferred)*
  - "Gremlin is just a query language" — the **string** form runs through a Groovy engine, so it is code,
    not a constrained query. *(inferred)*

## §10 Downstream responsibilities (operator/deployer)

*(Inferred unless tagged — confirm.)*

- Deploy Gremlin Server inside a trusted network; do **not** expose it directly to an untrusted/public
  network, especially with scripting enabled and auth off. *(inferred — §14 Q1)*
- Enable authentication (`Authenticator`) and authorization (`Authorizer`) for any non-trivial deployment.
  *(inferred — §14 Q2)*
- **Restrict script execution:** prefer bytecode-based traversals; if scripts are needed, apply the
  "Protecting Script Execution" controls (sandbox / compilation customizers / allow-list) and restrict who
  may submit scripts. *(documented: guidance exists — §14 Q4)*
- Enable TLS where traffic crosses an untrusted segment. *(inferred — §14 Q5)*
- Prefer **GraphBinary**; only enable **Gryo** on a trusted surface, with type registration locked down.
  *(inferred — §14 Q6)*
- Apply per-request / per-traversal resource limits and result-size caps appropriate to capacity. *(inferred
  — §14 Q7)*
- Set filesystem permissions so only the server user can read the config / data directories. *(inferred)*

## §11 Known misuse patterns

*(Draft one-liners — expand before publishing.)*

- Exposing Gremlin Server to an untrusted network with scripting enabled and authentication off. *(inferred)*
- Treating the script-engine sandbox as a complete RCE boundary rather than restricting who may script.
  *(inferred)*
- Building Gremlin (string or bytecode) by concatenating the embedding application's untrusted input
  (Gremlin-injection). *(inferred)*
- Enabling Gryo on an untrusted request surface without type-registration lockdown. *(inferred)*

## §11a Known non-findings (recurring false positives)

*(Inferred unless tagged; the PMC's confirmations here are the highest-leverage suppression input.)*

- "Gremlin Server executes arbitrary code via scripts" — by-design when scripting is enabled for the
  principal (§9); not a finding unless it bypasses a *configured* restriction. *(inferred — §14 Q4)*
- "No authentication / no TLS by default" — operator deployment responsibility (§9/§10); not a code bug in
  itself. *(inferred — §14 Q2/Q5)*
- "Gryo/Kryo deserialization can be exploited" — when the operator enabled Gryo on an untrusted surface;
  operator responsibility, prefer GraphBinary (§9/§10). *(inferred — §14 Q6)*
- "Expensive traversal consumes CPU/memory" — operator capacity concern, unless a specific bug applies
  (§8/§9). *(inferred — §14 Q7)*
- "Embedding app builds a traversal from untrusted input (Gremlin-injection)" — the embedding app's
  responsibility (§9). *(inferred)*
- Findings in `gremlin-test/`, `gremlin-examples/`, tooling — out of scope (§3). *(inferred)*

## §12 Conditions that would change this model

- A change to the default auth / TLS / scripting posture (e.g. auth-on-by-default, scripts-off-by-default).
- A new client-reachable surface or protocol on Gremlin Server.
- A change to the default-enabled serializer set, or Gryo type-registration policy.
- Promoting `gremlin-console` / `gremlin-examples` into a production trust surface.
- A change to the OLAP (Hadoop/Spark) trust posture.
- A report that cannot be routed to a single §13 disposition (→ revise the model).

## §13 Triage dispositions

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | Violates a §8 property via an in-scope adversary/input (auth/authz bypass, pre-auth/malformed-input crash/OOM/hang, deserializer RCE beyond the documented type set, parser memory-safety). | §8, §6, §7 |
| `VALID-HARDENING` | No §8 property broken, but a §11 misuse is easy enough to harden. | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires operator/host/config control. | §6, §7 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Requires a capability the model excludes (host control, side channel, direct public exposure against guidance). | §3, §7 |
| `OUT-OF-MODEL: unsupported-component` | Lands in `gremlin-test/`, `gremlin-examples/`, tooling, or a separate provider graph DB. | §3 |
| `OUT-OF-MODEL: non-default-build` | Only manifests under a discouraged/non-default §5a setting (e.g. Gryo enabled on an untrusted surface, scripting unrestricted where the deployment intends bytecode-only). | §5a |
| `BY-DESIGN: property-disclaimed` | Concerns a §9-disclaimed property (script execution within its grant, no-TLS/no-auth default, ordinary resource exhaustion, malicious operator). | §9 |
| `KNOWN-NON-FINDING` | Matches a §11a entry. | §11a |
| `MODEL-GAP` | Cannot be cleanly routed — triggers a §12 revision. | §12 |

## §14 Open questions for the maintainers

Every *(inferred)* claim in the body maps to one of these. Proposed answers are inline; please confirm,
correct, or strike.

1. **Deployment posture.** Is "Gremlin Server inside a trusted network, behind the app tier, not directly
   public" the right §2/§5 framing? *Proposed: yes.*
2. **Default auth/authz posture.** Do the shipped/default Gremlin Server configs run with authentication
   and authorization **off**, leaving it to the operator to enable? Is an unauthenticated exposed server an
   operator-misconfiguration (`OUT-OF-MODEL`) rather than a code bug? *Proposed: yes — auth/authz are
   opt-in.*
3. **Provider SPI boundary.** Confirm that vulnerabilities in third-party provider graph databases that
   embed TinkerPop route to those providers, not here (this model covers TinkerPop's own code + its use of
   the SPI). *Proposed: yes.*
4. **Script execution (highest-stakes).** Confirm that string-script evaluation = server-side code
   execution by design, that restricting it (sandbox / allow-list / bytecode-only / who-may-script) is the
   operator's responsibility, and what — if any — restriction is **on by default**. Is "Gremlin Server runs
   arbitrary code via scripts" `BY-DESIGN` unless a *configured* restriction is bypassed? *Proposed: yes,
   by-design; restriction is operator-configured.*
5. **TLS default.** Is TLS **off by default** on the server connector? Is no-TLS-by-default an operator
   responsibility (§9/§10)? *Proposed: off by default; operator enables.*
6. **Serialization / Gryo.** Which serializers are registered by default? Is **Gryo (Kryo)** locked to
   registered types, and is a Gryo-deserialization finding on an operator-enabled untrusted surface
   `OUT-OF-MODEL: non-default-build` (with GraphBinary the recommended default)? Conversely, is a
   deserializer flaw in the **default** set `VALID`? *Proposed: prefer GraphBinary; Gryo-on-untrusted is
   operator responsibility; a flaw in the default set is VALID.*
7. **Resource line.** Confirm the split: malformed/pre-auth input causing crash/OOM/hang is `VALID`;
   ordinary expensive traversals / large results are operator capacity unless a specific bug applies
   (super-linear amplification, missing-expected-limit, unbounded traversal/recursion). Are there built-in
   per-request limits (timeout, result cap, traversal depth)? *Proposed: split as stated.*
8. **Parser robustness.** Confirm that memory-safety / bounded-resource on the `gremlin-language` ANTLR
   parser and the bytecode path against malformed input is a property TinkerPop commits to (§8). *Proposed:
   yes.*
9. **GLV (driver) scope.** Are the Gremlin Language Variants (`gremlin-python`/`-dotnet`/`-go`/`-js`) in
   scope for this batch, or deferred? They are client-side and deserialize server responses. *Proposed:
   in-scope but lower priority; flag if any should be deferred.*
10. **OLAP scope.** Are `hadoop-gremlin` / `spark-gremlin` (`GraphComputer` over a cluster) in scope, or
    treated as operator cluster infrastructure out of this model? *Proposed: operator infra; flag if you
    want them fully in.*
11. **§11a seeds.** What do scanners/researchers most often report that the PMC considers a non-finding,
    beyond the seed list above? *(seeds §11a)*
12. **Canonical location / triage policy.** Confirm this model lives as root `THREAT_MODEL.md` referenced
    from a new `SECURITY.md`, wired from `AGENTS.md`, and that the PMC owns revisions. *Proposed: yes.*

## §15 Machine-readable companion

Deferred for v0. A `threat-model.yaml` can later encode the §6 trust table, §2/§3 component scoping, §8
property/severity/symptom rows, §9 false friends, §11a non-findings, and §13 dispositions for automated
triage.
