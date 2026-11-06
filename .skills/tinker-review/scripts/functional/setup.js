/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Mechanical setup for Phase-2 functional testing: build the PR from source and
// stand up a Gremlin Server from the freshly built assembly, so a blind subagent
// can exercise the change as a user would. This is the reproducible half of
// step 4 — the judgment (what to test) lives in the playbooks' Verify sections.
//
// Distinct from infrastructure/docker.js, which runs the STOCK published image
// for the Phase-1 knowledge graph. Functional testing must run the PR's OWN
// compiled code, so it builds and launches natively from `target/`.
//
// The build worktree is separate from the enrichment worktree (`<workDir>/src`)
// so Maven's `target/` output never pollutes the tree the agent reads during
// enrichment.

import { execFile, spawn } from "node:child_process";
import { promisify } from "node:util";
import { mkdir, writeFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { existsSync } from "node:fs";
import { createWriteStream } from "node:fs";

import { findAvailablePort, waitForHttp } from "../infrastructure/net.js";

const exec = promisify(execFile);

const DEFAULT_BUILD_TIMEOUT_MS = 30 * 60 * 1000; // full reactor build is slow
const DEFAULT_READY_TIMEOUT_MS = 60 * 1000;      // native JVM start + graph load

// TinkerGraph config for the functional server. Mirrors the stock empty graph.
const GRAPH_PROPERTIES = `gremlin.graph=org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
gremlin.tinkergraph.vertexIdManager=LONG
`;

// Init script binding the two traversal sources the reviewer may need — kept in
// lockstep with docker.js's INIT_GROOVY so both servers expose the same surface:
//   'g' — standard traversal
//   'a' — withComputer() for OLAP steps like connectedComponent()
const INIT_GROOVY = `def globals = [:]
globals << [g : traversal().withEmbedded(graph)]
globals << [a : traversal().withEmbedded(graph).withComputer()]
`;

// The release lines ship different servers: master (4.x) answers HTTP with the
// V4 serializers, while 3.7-dev and 3.8-dev run the WebSocket server with
// GraphSON V3 / GraphBinary V1 and op processors. The PR's own source says which
// line it is on: only 4.x has GraphSONMessageSerializerV4.
const V4_SERIALIZER = "gremlin-util/src/main/java/org/apache/tinkerpop/gremlin/util/ser/GraphSONMessageSerializerV4.java";

/**
 * Which server the built source runs: `http` (4.x) or `websocket` (3.x).
 * @param {string} buildWorktree
 * @returns {"http"|"websocket"}
 */
export function serverProtocol(buildWorktree) {
  return existsSync(join(buildWorktree, V4_SERIALIZER)) ? "http" : "websocket";
}

const SCRIPT_ENGINES = `scriptEngines: {
  gremlin-lang: {},
  gremlin-groovy: {
    plugins: { org.apache.tinkerpop.gremlin.server.jsr223.GremlinServerGremlinPlugin: {},
               org.apache.tinkerpop.gremlin.jsr223.ImportGremlinPlugin: {classImports: [java.lang.Math], methodImports: [java.lang.Math#*]},
               org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin: {files: [scripts/review-init.groovy]}}}}`;

/**
 * Build a self-contained server yaml for the functional test, following the
 * shipped default of the PR's release line (see serverProtocol) with host/port
 * pinned and the g/a bindings wired via an init script. Written fresh rather
 * than parsed from the assembly so there is no YAML dependency and no textual
 * patching of the shipped file. Only settings a review needs are set: metrics
 * are left out because their setting names differ between lines.
 *
 * @param {number} port
 * @param {"http"|"websocket"} protocol
 */
export function serverYaml(port, protocol) {
  if (protocol === "http") {
    return `host: localhost
port: ${port}
timeoutMillis: 30000
channelizer: org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer
graphs: {
  graph: conf/tinkergraph-review.properties}
${SCRIPT_ENGINES}
serializers:
  - { className: org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV4, config: { ioRegistries: [org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3] }}
  - { className: org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4 }
  - { className: org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4, config: { serializeResultToString: true }}
strictTransactionManagement: false
`;
  }
  return `host: localhost
port: ${port}
evaluationTimeout: 30000
channelizer: org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer
graphs: {
  graph: conf/tinkergraph-review.properties}
${SCRIPT_ENGINES}
serializers:
  - { className: org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3, config: { ioRegistries: [org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3] }}
  - { className: org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1 }
  - { className: org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1, config: { serializeResultToString: true }}
processors:
  - { className: org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor, config: { sessionTimeout: 28800000 }}
  - { className: org.apache.tinkerpop.gremlin.server.op.traversal.TraversalOpProcessor}
strictTransactionManagement: false
`;
}

/**
 * Write the review's graph config, init script and server yaml into a built
 * server assembly. Returns the yaml path to launch `bin/gremlin-server.sh` with.
 *
 * @param {string} assemblyDir - a `*-standalone` server directory
 * @param {number} port
 * @param {"http"|"websocket"} protocol - from serverProtocol
 * @returns {Promise<string>}
 */
export async function writeServerConfig(assemblyDir, port, protocol) {
  await mkdir(join(assemblyDir, "conf"), { recursive: true });
  await mkdir(join(assemblyDir, "scripts"), { recursive: true });
  await writeFile(join(assemblyDir, "conf", "tinkergraph-review.properties"), GRAPH_PROPERTIES);
  await writeFile(join(assemblyDir, "scripts", "review-init.groovy"), INIT_GROOVY);
  const yamlPath = join(assemblyDir, "conf", "gremlin-server-review.yaml");
  await writeFile(yamlPath, serverYaml(port, protocol));
  return yamlPath;
}

/**
 * Locate the built standalone server assembly under the build worktree. The
 * assembly directory carries the project version in its name, so it is globbed
 * rather than hard-coded.
 *
 * @param {string} buildWorktree
 * @returns {Promise<string>} absolute path to the *-standalone directory
 */
async function findAssembly(buildWorktree) {
  const targetDir = join(buildWorktree, "gremlin-server", "target");
  if (!existsSync(targetDir)) {
    throw new Error(`No gremlin-server/target under ${buildWorktree} — did the build succeed?`);
  }
  const entries = await readdir(targetDir);
  const match = entries.find(
    (e) => e.startsWith("apache-tinkerpop-gremlin-server-") && e.endsWith("-standalone"),
  );
  if (!match) {
    throw new Error(`No *-standalone assembly in ${targetDir} (found: ${entries.join(", ") || "nothing"})`);
  }
  return join(targetDir, match);
}

/**
 * @typedef {object} FunctionalHandle
 * @property {number} port        - localhost port the server listens on
 * @property {"http"|"websocket"} protocol - which server the PR's line runs (see serverProtocol)
 * @property {string} url         - endpoint to hand the subagent: `http://localhost:<port>/gremlin`
 *   on 4.x, `ws://localhost:<port>/gremlin` on 3.x
 * @property {number} pid         - PID of the server JVM (for teardown)
 * @property {string} buildWorktree - the `<workDir>/build` worktree to remove on teardown
 * @property {string} assemblyDir - the built `*-standalone` directory
 * @property {string} logFile     - server stdout/stderr log for diagnosis
 */

/**
 * Build the PR and start a Gremlin Server from the built artifacts.
 *
 * Steps: add a `build` worktree on the PR branch → `mvn clean install
 * -DskipTests` over the full reactor → locate the server assembly → write the
 * TinkerGraph config + init script + yaml → launch `bin/gremlin-server.sh`
 * natively on a free port → poll until ready.
 *
 * The caller (SKILL.md step 4) decides whether to run this at all, per the
 * playbooks' Verify gate. Skipped changes never pay the build cost.
 *
 * @param {string} workDir - the review work dir, e.g. `/tmp/pr-review-<pr>`
 * @param {object} opts
 * @param {number} opts.pr - PR number (the branch is `pr-review/<pr>`)
 * @param {string} opts.repoPath - the git repo the worktree is added against
 * @param {number} [opts.port] - fixed port (default: an OS-assigned free port)
 * @param {number} [opts.buildTimeoutMs]
 * @param {number} [opts.readyTimeoutMs]
 * @returns {Promise<FunctionalHandle>}
 */
export async function buildAndStart(workDir, opts) {
  const { pr, repoPath } = opts;
  if (!pr) throw new Error("buildAndStart requires opts.pr");
  if (!repoPath) throw new Error("buildAndStart requires opts.repoPath");

  const prBranch = `pr-review/${pr}`;
  const buildWorktree = join(workDir, "build");

  // Add the build worktree (idempotent — remove a stale one first).
  if (existsSync(buildWorktree)) {
    await exec("git", ["worktree", "remove", "--force", buildWorktree], { cwd: repoPath }).catch(() => {});
  }
  await exec("git", ["worktree", "prune"], { cwd: repoPath }).catch(() => {});
  // Detached: prBranch is already checked out in the src/ worktree, and git
  // refuses to check out one branch in two worktrees.
  await exec("git", ["worktree", "add", "--detach", buildWorktree, prBranch], { cwd: repoPath });

  // From here on, any failure stops the server (if it started) and removes the
  // build worktree, so a failed start leaves nothing for teardown to miss.
  let handle = { buildWorktree };
  try {
    handle = await buildAndLaunch(workDir, buildWorktree, opts);
    await waitForHttp(handle.port, opts.readyTimeoutMs || DEFAULT_READY_TIMEOUT_MS)
      .catch((err) => { throw new Error(`${err.message} — see ${handle.logFile}`); });
  } catch (err) {
    await stop(handle, { repoPath }).catch(() => {});
    throw err;
  }
  return handle;
}

// Build the reactor in the build worktree, write the server config into the
// assembly, and launch it. Returns the handle; readiness is the caller's job.
async function buildAndLaunch(workDir, buildWorktree, opts) {
  // Full-reactor build without tests, so every module reflects the PR.
  await exec(
    "mvn",
    ["-f", join(buildWorktree, "pom.xml"), "clean", "install", "-DskipTests"],
    { cwd: buildWorktree, timeout: opts.buildTimeoutMs || DEFAULT_BUILD_TIMEOUT_MS, maxBuffer: 64 * 1024 * 1024 },
  );

  const assemblyDir = await findAssembly(buildWorktree);
  const port = opts.port || await findAvailablePort();

  const protocol = serverProtocol(buildWorktree);
  const yamlPath = await writeServerConfig(assemblyDir, port, protocol);

  // Launch natively in the foreground ("console" mode reads the yaml arg via the
  // catch-all case) and detach, capturing output to a log for diagnosis.
  const logFile = join(workDir, "functional-server.log");
  const out = createWriteStream(logFile);
  await new Promise((resolve) => out.once("open", resolve));

  const child = spawn(
    join(assemblyDir, "bin", "gremlin-server.sh"),
    [yamlPath],
    { cwd: assemblyDir, stdio: ["ignore", out, out], detached: true },
  );
  child.unref();

  const scheme = protocol === "http" ? "http" : "ws";
  return { port, protocol, url: `${scheme}://localhost:${port}/gremlin`, pid: child.pid, buildWorktree, assemblyDir, logFile };
}

/**
 * Stop the functional server and remove its build worktree.
 *
 * @param {FunctionalHandle} handle
 * @param {object} [opts]
 * @param {string} [opts.repoPath] - repo to prune the worktree against (defaults to none)
 * @returns {Promise<void>}
 */
export async function stop(handle, opts = {}) {
  if (handle?.pid) {
    try { process.kill(handle.pid, "SIGTERM"); } catch { /* already gone */ }
  }
  if (handle?.buildWorktree && opts.repoPath) {
    await exec("git", ["worktree", "remove", "--force", handle.buildWorktree], { cwd: opts.repoPath }).catch(() => {});
    await exec("git", ["worktree", "prune"], { cwd: opts.repoPath }).catch(() => {});
  }
}
