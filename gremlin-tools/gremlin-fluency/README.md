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

# Gremlin Fluency

Gremlin Fluency measures how well an AI agent writes Gremlin. It gives an agent a plain-English request and runs the
traversal the agent writes through TinkerPop's own feature tests. A traversal passes if the test still passes with
the agent's traversal in place of the original.

Gremlin Fluency is an internal development tool. It is built and tested with the rest of TinkerPop, but it is not
deployed and no convenience binaries are produced for it.

## What it tests

TinkerPop's Gherkin feature tests in `gremlin-test` define how Gremlin behaves across every language variant. Each
scenario names a graph, gives a traversal and states the result it must produce. A scenario may also carry an
*intent*, which is a description in plain English of what the traversal does:

```gherkin
Scenario: g_V_groupCount_byXlabelX
  Given the modern graph
  And the intent
    """
    How many vertices are there of each label? Return one map from label to count.
    """
  And the traversal of
    """
    g.V().groupCount().by(T.label)
    """
  When iterated to list
  Then the result should be unordered
    | result |
    | m[{"software":"d[2].l","person":"d[4].l"}] |
```

The intent has no effect on the test itself. Every language variant accepts the step and ignores it. Gremlin Fluency
reads the scenarios that carry an intent, about 250 of them, and treats each one as a task: given the intent,
write a traversal that produces the scenario's verified result.

Most of these scenarios test individual steps on the *modern* toy graph. The scenarios in the `patterns` directory are
closer to real use. They answer domain questions, mostly on *air-routes*, a realistic graph of airports, countries and
the routes between them, and are grouped by the kind of question they ask: reachability, paths, aggregation, ranking,
comparison, lookup, exploration and recommendation. Others in the same files, and a few in the feature files of steps
that write, start from an empty graph and add data that follows one of four *domain schemas*: a ledger of accounts and payments,
device telemetry, an organization chart and imported contacts. These graphs are unfamiliar to a model, and they hold
the values the preloaded graphs lack, such as decimals, UUIDs, durations, dates, characters and multi-properties, so
an agent has to get the type of a value right as well as the shape of the traversal. The schemas are described in the
"Domain Schemas" section of the developer documentation. The rest of the scenarios cover a few other test graphs and
writes to an empty graph. The scenarios were chosen to cover as many Gremlin steps and kinds of question as possible. Some ask a question
("How many airports can be reached from Austin (AUS) with one stop but not with a nonstop flight?"), while others
describe the traversal step by step, the way Gremlin is often explained in English.

Some scenarios carry an intent but cannot be scored fairly, for example because they expect an error or because the
intent does not name one of the scenario's parameters. These are set aside as *exclusions* and reported with each
run, rather than dropped without a word.

The rules for writing an intent are in the "Gremlin Language Test Cases" section of the developer documentation
(`docs/src/dev/developer/for-committers.asciidoc`). In short, an intent should admit only one reasonable reading, and
that reading should give the expected result on any graph with the same schema, not just the one the test uses.

## How it works

A run takes a *matrix* of *setups*, where each setup is one way of driving an agent: a model, a harness such as Claude
Code, and the context it is given, such as no help at all or access to `gremlin-mcp`. For every setup and every
scenario, the runner:

1. **Prompts the agent.** The prompt holds the intent and nothing else. The graph is not named, as it would not be
   for an agent working on a real graph, so the agent learns about the graph only from the request or from the tools
   it is given. The scenario name (which spells out the original traversal) and the expected result are never shown.
   A parameter such as `<vid1>` is left as a name, and the agent is asked to use it as a variable. The prompt template
   is in `src/main/resources/org/apache/tinkerpop/gremlin/fluency/prompt.txt`.
2. **Gets a traversal back.** A *driver* passes the prompt to the agent and returns its answer. The answer is only
   trimmed and stripped of a surrounding code fence. Writing valid, canonical gremlin-lang is part of what is measured,
   so nothing else is repaired.
3. **Scores it.** The scenario is copied with the agent's traversal in place of the original and run through the same
   Cucumber step definitions the feature tests use, on TinkerGraph. The feature files themselves are never changed.
   The result is one of `PASS`, `NO_ANSWER`, `DECLINED`, `PARSE_FAILURE`, `RUNTIME_ERROR`, `WRONG_RESULT` or
   `TIMEOUT`. An answer with no line that starts with `g.` is `DECLINED`. It is an agent explaining why it gave no
   traversal, not Gremlin that failed to parse, so it is not run.
4. **Checks the passes.** A traversal can pass by coincidence. For example, `both()` gives the same answer as `out()`
   for a vertex that has no incoming edges. The *differential* pass adds seeded random vertices and edges to the
   graph, using the graph's own labels, keys and values, then runs the original and the agent's traversal side by side
   and compares what they return and how they leave the graph. On a graph with a domain schema, the additions follow
   the schema, so they never add data it rules out, such as a second manager for an employee. A pass is then `AGREES`, `COINCIDENTAL`,
   `STATE_MISMATCH` or `NOT_CHECKED`, for scenarios where there is nothing to compare, such as one that only counts. A
   pass is *verified* unless it was found coincidental or left the graph in a different state.
5. **Summarises the run.** Each run ends with a snapshot and an HTML report. Together they give each setup's raw and
   verified pass rates, broken down by graph and feature file, the turns and tokens used and the tools called where
   the driver reports them, how each context compares with no context for the same model and harness, and every
   traversal each setup wrote for each scenario.

Each scenario is attempted several times per setup, because agents do not answer the same way every time.

## Running it

Build TinkerPop first, so that the `gremlin-test` jar holding the feature files is installed:

```bash
mvn clean install -DskipTests
```

Then run a matrix from the module directory:

```bash
cd gremlin-tools/gremlin-fluency
mvn exec:java -Dexec.mainClass=org.apache.tinkerpop.gremlin.fluency.FluencyRunner -Dexec.args=conf/example.yaml
```

On Java 17 and later, loading air-routes needs the `--add-opens` flags that the build gives its tests. They are
listed as `jdk17JvmArgs` in the root `pom.xml`. Pass them through `MAVEN_OPTS`, or to `java` if you launch the runner
directly.

The example matrix compares Claude Code with no context and with `gremlin-mcp`, and needs the `claude` CLI installed
and signed in. Agent setups make real model calls and can take a while: every scenario is attempted several times for
each setup. Nearly all of that time is spent waiting on the agent, so `parallelism` in the matrix, which sets how many
attempts are in flight at once, shortens a run about in proportion. Results do not depend on it, apart from the
timing each attempt records.

### Where results go

Each run writes to `~/.gremlin-fluency/runs/<run-id>`, where the run id is the time it started. Runs are kept outside
the build directory so they survive `mvn clean` and can be compared later. Add `--root <dir>` to the arguments to
write somewhere else. A run directory holds:

| File | Contents |
|---|---|
| `matrix.yaml` | A copy of the matrix the run used |
| `run.json` | The TinkerPop version and git commit, hashes of the prompt template and corpus, the settings, every scenario with its intent and original traversal, the exclusions and the setups |
| `attempts.jsonl` | One row per attempt: the traversal written, its outcome, the differential finding, timing, usage and the tools called |
| `<setup>/` | The scenario copies that were scored, so any attempt can be run again exactly as it was scored |
| `snapshot.json` | The summary of the run |
| `report.html` | The summary as a single page with no external files, which opens anywhere |

The snapshot and report are built only from `run.json` and `attempts.jsonl`, so they can be rebuilt for any run,
including one that stopped early:

```bash
mvn exec:java -Dexec.mainClass=org.apache.tinkerpop.gremlin.fluency.Snapshot -Dexec.args=<run-directory>
```

### Writing a matrix

```yaml
corpus:
  graphs: [modern, airroutes]      # optional; every annotated scenario if left out
attempts: 3                        # tries per scenario per setup
parallelism: 4                     # attempts in flight at once; 1 if left out
timeouts: {driver: 120s, traversal: 30s}
differential: {seeds: [1, 2, 3]}   # the perturbed graphs passes are checked on; [] turns the check off
setups:
  - name: reference
    driver: {builtin: reference}
  - name: sonnet-mcp
    model: claude-sonnet-5-5
    harness: claude-code
    context: [gremlin-mcp]
    server: true
    driver:
      command: [../src/main/bin/claude-code.sh, --mcp]
      env: {MODEL: claude-sonnet-5-5}
```

`model`, `harness` and `context` are labels recorded with the results so that setups can be grouped and compared. The
driver is what actually configures the agent. A relative driver command resolves against the directory of the matrix
file.

`server: true` starts an embedded Gremlin Server for the run. It serves each test graph as a read-only traversal source
with a neutral name, such as `g1`, so that a tool-using agent can explore the graph it is writing for without being
told which test graph it is. The empty graph is writable, and it is reset before each attempt to the state the
scenario starts from. With `parallelism` above 1, each attempt in flight gets an empty graph of its own.

There are two built-in drivers:

- `reference` answers with each scenario's original traversal. It is a control, not an agent. Anything short of a full
  pass means the run itself is broken, for example a graph that failed to load or a timeout set too tight, and the
  agent results beside it should not be trusted.
- `replay` answers from a YAML file that maps scenario names to traversals, given as `driver: {builtin: replay, file:
  answers.yaml}`. It runs known answers through the scorer without calling an agent.

### Writing a driver

Any harness can be driven by a command that reads the prompt on stdin and prints one traversal on stdout. The runner
starts the command in a new, empty directory, so no TinkerPop checkout, project instructions or memory are within
reach. It sets these environment variables:

| Variable | Value |
|---|---|
| `FLUENCY_ATTEMPT` | A number that identifies the attempt without revealing the scenario |
| `FLUENCY_GREMLIN_ENDPOINT` | Where the scenario's graph is served, for setups with `server: true` |

A non-zero exit, empty output or running past the driver timeout counts as no answer. A command may also write these
lines to stderr to have them recorded:

```
fluency-version: <harness and tool versions>
fluency-usage: input_tokens=<n> output_tokens=<n> turns=<n>
fluency-tools: <the tools the agent called, in order, separated by spaces>
```

A `fluency-tools:` line with nothing after it means the agent called no tools. A driver that writes no such line is
treated as not reporting its calls, and its attempts are left out of the report's tool use.

`src/main/bin/claude-code.sh` is the driver for Claude Code. It gives the agent no built-in tools, so it cannot search
the web or read files, and with `--mcp` it adds `gremlin-mcp` pointed at the run's server. Set `GREMLIN_MCP_PACKAGE`
to an npm package spec or the absolute path of a local build to choose which `gremlin-mcp` runs. Otherwise the
unpinned package is used, and its version can change between runs.

## What TinkerPop uses it for

The questions Gremlin Fluency is meant to answer are about what TinkerPop provides, more than about any one model:

- **Does a context help?** Comparing a setup with no help against the same model and harness given `gremlin-mcp`, the
  reference documentation or a skill shows whether that context makes agents better at Gremlin, and on which kinds
  of query.
- **Did a change make things better or worse?** A new `gremlin-mcp` tool, a rewritten docs section or a reworded tool
  description can be measured, not guessed at, by running the same matrix before and after.
- **Where do agents struggle?** The per-scenario results show which steps, patterns and graphs agents get wrong most
  often. They point at the documentation, error messages and tools that most need work.
- **Is an intent any good?** When many setups miss the same scenario in the same way, the intent is often ambiguous
  rather than the agents being wrong. The traversals each setup wrote make that easy to see, and the intent can be
  fixed.

Turns and tokens matter as much as pass rates when comparing contexts. A context that gets the same answers in fewer
turns is still an improvement.

## How intents help beyond this tool

The intents live in the public feature files, next to traversals and results that every language variant verifies on
every build. That makes each annotated scenario a three-way pairing of a plain-English request, an idiomatic
traversal and a known-correct answer, published under the Apache License in a repository that is widely read and
mirrored.

That has uses beyond Gremlin Fluency. It is example material for anyone, person or model, learning how a question
becomes Gremlin. It gives other evaluation harnesses a ready-made, checked set of tasks without needing anything from
this module. And because the scenarios are also generated into Python, JavaScript, .NET and Go tests, the same pairing
exists in each language variant's own syntax. Better intents in TinkerPop's own files mean better Gremlin examples
wherever those files end up.

## Adding to the corpus

New intents are added to the feature files in `gremlin-test`, never to this module. A good intent:

- describes what its traversal returns in domain terms ("the airports Austin flies to") rather than leaving details,
  such as edge direction, to chance;
- names every parameter the scenario uses, in angle brackets;
- on a domain schema, rests on data that follows the schema, with each value stored as the type the schema gives it;
- is reviewed by a person, because an intent that admits two readings measures nothing.

Run the reference setup after adding intents. It should still pass every scenario, and the differential pass should
find nothing against the original traversals.
