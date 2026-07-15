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

import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { writeFile, mkdir, rm } from "node:fs/promises";
import { join, basename, extname } from "node:path";
import { existsSync } from "node:fs";
import gremlin from "gremlin";

import { startServer, stopServer } from "./infrastructure/docker.js";
import { extractMulti } from "./extraction/tree-sitter.js";
import { populate } from "./graph/populate.js";
import { populateDiscussions } from "./graph/populate-discussions.js";
import { completeness } from "./patterns/completeness.js";
import { coverageGaps } from "./patterns/coverage-gaps.js";
import { highCentrality } from "./patterns/centrality.js";
import { blastRadius } from "./patterns/blast-radius.js";
import { clusterAnalysis } from "./patterns/cluster-analysis.js";
import { communityDetection } from "./patterns/community-detection.js";
import { architecture } from "./patterns/architecture.js";
import { confidenceAudit } from "./patterns/confidence-audit.js";
import { classifyExternals } from "./patterns/classify-externals.js";
import { findRemovalRefs } from "./patterns/removal-refs.js";
import { orphans } from "./patterns/orphans.js";
import { createPrDiscussion } from "./enrichment/api.js";
import { discoverDiscussions } from "./discovery/discussions.js";

const exec = promisify(execFile);

const LANGUAGE_HINTS = {
  dart: "dart",
  java: "java",
  go: "go",
  py: "python",
  js: "javascript",
  mjs: "javascript",
  cs: "csharp",
};

function log(msg) {
  process.stdout.write(`[review] ${msg}\n`);
}

// Every language present among the changed files, most-changed first. The graph
// is built from all of them (extractMulti); by-name edges stay language-scoped so
// mixing languages doesn't invent cross-language edges. Falls back to ["java"].
function detectLanguages(changedFiles) {
  const extCounts = new Map();
  for (const file of changedFiles) {
    const ext = extname(file).slice(1);
    if (LANGUAGE_HINTS[ext]) {
      extCounts.set(LANGUAGE_HINTS[ext], (extCounts.get(LANGUAGE_HINTS[ext]) || 0) + 1);
    }
  }
  const langs = [...extCounts.entries()].sort((a, b) => b[1] - a[1]).map(([lang]) => lang);
  return langs.length > 0 ? langs : ["java"];
}

async function getChangedFiles(repoPath, prBranch, remote = "upstream", baseBranch = "master") {
  const { stdout: baseCommit } = await exec(
    "git", ["merge-base", prBranch, `${remote}/${baseBranch}`],
    { cwd: repoPath }
  );
  const base = baseCommit.trim();

  const { stdout: diffOutput } = await exec(
    "git", ["diff", "--name-only", `${base}...${prBranch}`],
    { cwd: repoPath }
  );
  return diffOutput.trim().split("\n").filter(Boolean);
}

/**
 * The base-version source text of each changed file, keyed by path, so extraction
 * can diff base against head and grade every member's `changeLevel`. Reads
 * `git show <merge-base>:<path>`; a path git can't produce — added by the PR, or
 * binary — is omitted, and extraction treats a missing entry as an added file
 * (STRUCTURAL). Returns `{ [path]: content }`.
 */
export async function getBaseContents(repoPath, prBranch, changedFiles, remote = "upstream", baseBranch = "master") {
  const { stdout: baseCommit } = await exec(
    "git", ["merge-base", prBranch, `${remote}/${baseBranch}`], { cwd: repoPath }
  );
  const base = baseCommit.trim();
  const contents = {};
  await Promise.all(changedFiles.map(async (path) => {
    try {
      const { stdout } = await exec(
        "git", ["show", `${base}:${path}`], { cwd: repoPath, maxBuffer: 32 * 1024 * 1024 }
      );
      contents[path] = stdout;
    } catch {
      // Absent at base (added by the PR) or unreadable (binary) — leave unset.
    }
  }));
  return contents;
}

/**
 * Per-file line churn and deletion status for the PR, so downstream analysis can
 * describe *how* code changed (reduced vs. expanded), not just that it changed.
 * Merges `--numstat` (line counts) with `--name-status` (A/M/D/R). Binary files
 * report 0/0. Returns `{ [path]: { added, removed, deleted } }`.
 */
export async function computeChurn(repoPath, prBranch, remote = "upstream", baseBranch = "master") {
  const { stdout: baseCommit } = await exec(
    "git", ["merge-base", prBranch, `${remote}/${baseBranch}`], { cwd: repoPath }
  );
  const range = `${baseCommit.trim()}...${prBranch}`;

  const [numstat, nameStatus] = await Promise.all([
    exec("git", ["diff", "--numstat", range], { cwd: repoPath }).then((r) => r.stdout).catch(() => ""),
    exec("git", ["diff", "--name-status", range], { cwd: repoPath }).then((r) => r.stdout).catch(() => ""),
  ]);

  const churn = {};
  for (const line of numstat.trim().split("\n").filter(Boolean)) {
    const [added, removed, ...pathParts] = line.split("\t");
    const path = pathParts.join("\t");
    if (!path) continue;
    churn[path] = { added: added === "-" ? 0 : Number(added), removed: removed === "-" ? 0 : Number(removed), deleted: false };
  }
  for (const line of nameStatus.trim().split("\n").filter(Boolean)) {
    const [status, ...pathParts] = line.split("\t");
    const path = pathParts.join("\t");
    if (status && status[0] === "D" && path) {
      churn[path] = churn[path] || { added: 0, removed: 0 };
      churn[path].deleted = true;
    }
  }
  return churn;
}

function classifyDomains(changedFiles) {
  const paths = changedFiles.join("\n").toLowerCase();
  const domains = ["general"];

  if (paths.includes("gremlin-dart") || paths.includes("gremlin-swift") || paths.includes("gremlin-rust") ||
      paths.includes("gremlin-go/") || paths.includes("gremlin-python/") || paths.includes("gremlin-dotnet/") ||
      paths.includes("gremlin-js/")) {
    domains.push("glv");
  }
  if (paths.includes("gremlin-driver/") || paths.includes("gremlin-server/") || paths.includes("gremlin-util/") ||
      paths.includes("serializ") || paths.includes("graphson") || paths.includes("graphbinary") || paths.includes("gryo")) {
    domains.push("driver-server");
  }
  if (paths.includes("gremlin-language/") || paths.includes(".g4")) {
    domains.push("grammar");
  }
  if (paths.includes("gremlin-core/") && (paths.includes("step/map/") || paths.includes("step/filter/") ||
      paths.includes("step/sideeffect/") || paths.includes("step/branch/"))) {
    domains.push("new-step");
  }

  return domains;
}

// Structural/infra filenames that describe packaging, not a topic — they match
// boilerplate (e.g. NOTICE lives in every ASF license header) and must never
// become search keywords.
const STRUCTURAL_NAMES = new Set([
  "index", "package", "pom", "build", "notice", "license", "dockerfile",
  "docker-compose", "readme", "changelog", "makefile", "setup", "config",
]);
// Generic verbs/nouns in PR titles that carry no topic signal.
const GENERIC_WORDS = new Set([
  "remove", "removes", "removed", "removal", "add", "adds", "added", "fix",
  "fixes", "fixed", "update", "updates", "updated", "support", "refactor",
  "improve", "improves", "cleanup", "bump", "upgrade", "migrate", "implement",
  "introduce", "enable", "disable", "allow", "the", "and", "for", "with",
]);
// Tokens so common across the repo that matching on them finds everything.
const UBIQUITOUS_TOKENS = new Set([
  "gremlin", "server", "client", "test", "tests", "docker", "tinkerpop",
  "apache", "core", "impl", "util", "utils", "common", "base", "default",
  "abstract", "main", "java", "python",
]);

// Split an identifier/filename into lowercased word tokens (camelCase and
// non-alphanumeric boundaries), e.g. "Krb5Authenticator" -> ["krb5","authenticator"].
function tokenizeName(name) {
  return name
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .split(/[^A-Za-z0-9]+/)
    .map((t) => t.toLowerCase())
    .filter((t) => t.length >= 3);
}

// Keywords for discovery search (dev-list + proposals). Topic first: PR-title
// words (the human statement of intent) lead so they can't be truncated by the
// cap, then distinctive tokens from changed-file basenames. Structural names,
// generic verbs, and repo-ubiquitous tokens are filtered out so we don't search
// on noise like "NOTICE" or "gremlin-server". Exported for testing.
export function extractKeywords(changedFiles, prTitle) {
  const keywords = [];
  const seen = new Set();
  const push = (word) => {
    const k = word.toLowerCase();
    if (k.length < 3 || seen.has(k)) return;
    if (GENERIC_WORDS.has(k) || UBIQUITOUS_TOKENS.has(k)) return;
    seen.add(k);
    keywords.push(k);
  };

  for (const w of prTitle.split(/[\s\-_:,.()]+/)) {
    if (!/^\d+$/.test(w)) push(w);
  }
  for (const file of changedFiles) {
    const base = file.split("/").pop().replace(/\.\w+$/, "");
    if (STRUCTURAL_NAMES.has(base.toLowerCase())) continue;
    for (const tok of tokenizeName(base)) push(tok);
  }
  return keywords.slice(0, 8);
}

async function cleanupWorktree(repoPath, worktreePath, prBranch) {
  if (existsSync(worktreePath)) {
    await exec("git", ["worktree", "remove", "--force", worktreePath], { cwd: repoPath }).catch(() => {});
  }
  await exec("git", ["worktree", "prune"], { cwd: repoPath }).catch(() => {});
  await exec("git", ["branch", "-D", prBranch], { cwd: repoPath }).catch(() => {});
}

// ============================================================
// SETUP — fetch PR, create worktree, start server
// Returns a session object the agent uses for all subsequent phases.
// ============================================================

async function detectRemote(repoPath) {
  const { stdout } = await exec("git", ["remote", "-v"], { cwd: repoPath });
  const lines = stdout.split("\n");
  for (const line of lines) {
    if (line.includes("apache/tinkerpop") && line.includes("(fetch)")) {
      return line.split(/\s+/)[0];
    }
  }
  // Fall back to origin if no apache/tinkerpop remote found
  return "origin";
}

// Detect the branch a PR targets (e.g. master, 3.8-dev, 3.7-dev) from the
// GitHub API. Falls back to "master" if the API is unreachable.
async function detectBaseBranch(pr) {
  try {
    const { get } = await import("node:https");
    const prData = await new Promise((resolve, reject) => {
      get(`https://api.github.com/repos/apache/tinkerpop/pulls/${pr}`, {
        headers: { "User-Agent": "tinker-review", "Accept": "application/json" },
      }, (res) => {
        let data = "";
        res.on("data", (chunk) => { data += chunk; });
        res.on("end", () => resolve(JSON.parse(data)));
      }).on("error", reject);
    });
    return prData.base?.ref || "master";
  } catch {
    return "master";
  }
}

export async function setup(params) {
  const { pr, repoPath, options = {} } = params;
  const remote = options.remote || await detectRemote(repoPath);
  const baseBranch = options.baseBranch || await detectBaseBranch(pr);
  const prBranch = `pr-review/${pr}`;
  const workDir = `/tmp/pr-review-${pr}`;
  const worktreePath = `${workDir}/src`;

  await mkdir(workDir, { recursive: true });
  await cleanupWorktree(repoPath, worktreePath, prBranch);

  log(`PR #${pr} — targets ${baseBranch}, fetching...`);
  await exec("git", ["fetch", remote, baseBranch], { cwd: repoPath }).catch(() => {});
  await exec("git", ["fetch", remote, `pull/${pr}/head:${prBranch}`], { cwd: repoPath });
  await exec("git", ["worktree", "add", worktreePath, prBranch], { cwd: repoPath });

  const changedFiles = await getChangedFiles(repoPath, prBranch, remote, baseBranch);
  const languages = detectLanguages(changedFiles);
  const language = languages[0];
  const domains = classifyDomains(changedFiles);
  log(`PR #${pr} — classified as: ${domains.join(", ")} (${languages.join("+")}, ${changedFiles.length} files changed)`);

  log(`Starting Gremlin Server...`);
  const handle = await startServer({ port: options.port });
  log(`Gremlin Server ready on port ${handle.port}`);

  const connection = new gremlin.driver.DriverRemoteConnection(handle.url);
  const g = gremlin.process.traversal().withRemote(connection);
  const aConnection = new gremlin.driver.DriverRemoteConnection(handle.url, { traversalSource: "a" });
  const a = gremlin.process.traversal().withRemote(aConnection);

  await writeFile(join(workDir, "session.json"), JSON.stringify({
    url: handle.url,
    port: handle.port,
    containerId: handle.containerId,
    worktreePath,
    workDir,
    repoPath,
    pr,
    baseBranch,
  }, null, 2));

  return {
    pr,
    repoPath,
    remote,
    baseBranch,
    prBranch,
    workDir,
    worktreePath,
    changedFiles,
    language,
    languages,
    domains,
    handle,
    connection,
    aConnection,
    g,
    a,
  };
}

// ============================================================
// PHASE 1 — extract, populate, discover, run checks
// Writes evidence JSON to workDir. The Gremlin Server container stays alive for
// enrichment; the CLI process itself exits once Phase 1 completes (see main()).
// ============================================================

export async function phase1(session) {
  const { pr, repoPath, remote, baseBranch = "master", prBranch, workDir, worktreePath, changedFiles, language, domains, g, a } = session;
  const languages = session.languages || [language];

  log(`Phase 1: Extracting structure (${languages.join("+")})...`);
  const baseContents = await getBaseContents(repoPath, prBranch, changedFiles, remote, baseBranch);
  const extraction = await extractMulti(worktreePath, languages, { changedFiles, baseContents });
  log(`Phase 1 complete: ${extraction.files.length} files, ${extraction.functions.length} functions, ${extraction.types.length} types`);
  const neighborhood = extraction.hierarchyNeighborhood;
  if (neighborhood && neighborhood.files > 0) {
    log(`  hierarchy neighborhood: +${neighborhood.files} context files parsed for override/hierarchy edges${neighborhood.truncated ? " (TRUNCATED — hierarchy blast radius is a lower bound)" : ""}`);
  }

  log(`Populating graph...`);
  const graphStats = await populate(g, extraction, { changedFiles, worktreePath });
  log(`Graph populated: ${graphStats.vertices} vertices, ${graphStats.edges} edges`);

  let prTitle = `PR #${pr}`;
  let prBody = "";
  try {
    const { get } = await import("node:https");
    const prData = await new Promise((resolve, reject) => {
      get(`https://api.github.com/repos/apache/tinkerpop/pulls/${pr}`, {
        headers: { "User-Agent": "tinker-review", "Accept": "application/json" },
      }, (res) => {
        let data = "";
        res.on("data", (chunk) => { data += chunk; });
        res.on("end", () => resolve(JSON.parse(data)));
      }).on("error", reject);
    });
    prTitle = prData.title || prTitle;
    prBody = (prData.body || "").slice(0, 2000);
  } catch {
    const { stdout } = await exec("git", ["log", "-1", "--format=%s", prBranch], { cwd: repoPath }).catch(() => ({ stdout: prTitle }));
    prTitle = stdout.trim() || prTitle;
  }

  await createPrDiscussion(g, pr, prTitle.trim(), "");

  log(`Discovering discussions...`);
  const { stdout: diffText } = await exec(
    "git", ["diff", "--unified=0", `${(await exec("git", ["merge-base", prBranch, `${remote}/${baseBranch}`], { cwd: repoPath })).stdout.trim()}...${prBranch}`],
    { cwd: repoPath }
  ).catch(() => ({ stdout: "" }));

  const prKeywords = extractKeywords(changedFiles, prTitle.trim());
  const discussions = await discoverDiscussions({
    pr,
    prTitle: prTitle,
    prBody,
    diff: diffText,
    keywords: prKeywords,
    repoPath,
  });
  log(`  jiras: ${discussions.jiras.length} found${discussions.jiraMissing ? " (none referenced)" : ""}`);
  log(`  pr comments: ${discussions.prComments.issue.length} issue + ${discussions.prComments.review.length} review`);
  log(`  dev list: ${discussions.devList.length} found${discussions.devListSearchPerformed ? ` (searched: ${prKeywords.join(", ")})` : ""}`);
  log(`  proposals: ${discussions.proposals.length} found`);

  log(`Loading discussions into graph...`);
  await populateDiscussions(g, discussions, { pr, prTitle: prTitle.trim(), changedFiles });

  log(`Classifying external callees...`);
  const externalsResult = await classifyExternals(g, worktreePath);
  log(`  externals: ${externalsResult.library.length} library / ${externalsResult.project.length} project / ${externalsResult.unresolved.length} unresolved`);

  log(`Finding references to removed code...`);
  const removalRefsResult = await findRemovalRefs(g, worktreePath);
  log(`  removal_refs: ${removalRefsResult.total} surviving references to ${removalRefsResult.deletedCodeSymbols.length} removed symbols`);

  log(`Running checks...`);
  const completenessResults = await completeness(g, {
    vertexLabel: "File",
    expectedEdges: ["out:defines"],
  });

  const coverageResult = await coverageGaps(g, { changedOnly: true });
  const centralityResult = await highCentrality(g, { changedOnly: true, topN: 10, minDegree: 3 });
  const blastResult = await blastRadius(g, { depth: 3, changedOnly: true });
  blastResult.neighborhood = extraction.hierarchyNeighborhood || null;
  const clusterResult = await clusterAnalysis(a, { changedOnly: true });
  const churn = await computeChurn(repoPath, prBranch, remote, baseBranch).catch(() => null);
  const communityResult = await communityDetection(g, { churn });
  const confidenceResult = await confidenceAudit(g);
  const orphansResult = await orphans(g, { vertexLabel: "Function", expectedEdge: "tests", direction: "in", changedOnly: true });
  log(`  completeness: ${completenessResults.filter(r => r.missing.length > 0).length} gaps found`);
  log(`  coverage_gaps: ${coverageResult.uncovered.length} functions without tests`);
  log(`  centrality: ${centralityResult.aboveThreshold} hotspots`);
  log(`  blast_radius: max ${blastResult.maxReachable} reachable, ${blastResult.types.length} changed types with hierarchy impact`);
  log(`  clusters: ${clusterResult.clusterCount} (${clusterResult.coherent ? "coherent" : "fragmented"})`);
  log(`  communities: ${communityResult.communityCount} themes, modularity ${communityResult.modularity} (${communityResult.isolatedCount} isolated)`);
  log(`  confidence: ${confidenceResult.distribution.EXTRACTED} extracted / ${confidenceResult.distribution.INFERRED} inferred / ${confidenceResult.distribution.AMBIGUOUS} ambiguous`);
  log(`  orphans: ${orphansResult.totalOrphaned} functions with no test`);

  log(`Generating architecture map...`);
  const architectureResult = await architecture(g, { clusterResult, changedOnly: true });
  log(`  architecture: ${architectureResult.nodes.length} nodes, ${architectureResult.edges.length} edges`);

  const evidence = {
    meta: {
      pr,
      title: prTitle.trim(),
      domains,
      language,
      languages,
      changedFileCount: changedFiles.length,
      timestamp: new Date().toISOString(),
    },
    graphStats,
    architecture: architectureResult,
    checks: {
      completeness: completenessResults,
      coverageGaps: coverageResult,
      centrality: centralityResult,
      blastRadius: blastResult,
      clusters: clusterResult,
      communities: communityResult,
      confidence: confidenceResult,
      externals: externalsResult,
      removalRefs: removalRefsResult,
      orphans: orphansResult,
    },
    discussions,
    changedFiles,
  };

  const jsonPath = join(workDir, "evidence.json");
  await writeFile(jsonPath, JSON.stringify(evidence, null, 2), "utf-8");
  log(`Evidence written: ${jsonPath}`);

  return { evidence, jsonPath };
}

// ============================================================
// TEARDOWN — stop server, remove worktree
// Call this ONLY when all phases are complete.
// ============================================================

export async function teardown(sessionOrWorkDir) {
  let repoPath, worktreePath, prBranch, containerId, workDir;

  if (typeof sessionOrWorkDir === "string") {
    // Called with just a workDir path — read session.json
    const { readFile: rf } = await import("node:fs/promises");
    workDir = sessionOrWorkDir;
    const sessionData = JSON.parse(await rf(join(sessionOrWorkDir, "session.json"), "utf-8"));
    repoPath = sessionData.repoPath;
    worktreePath = sessionData.worktreePath;
    containerId = sessionData.containerId;
    prBranch = `pr-review/${sessionData.pr}`;
  } else {
    // Called with a live session object
    const session = sessionOrWorkDir;
    workDir = session.workDir;
    repoPath = session.repoPath;
    worktreePath = session.worktreePath;
    prBranch = session.prBranch;
    containerId = session.handle?.containerId;
    if (session.aConnection) await session.aConnection.close().catch(() => {});
    if (session.connection) await session.connection.close().catch(() => {});
  }

  // Stop the functional-test server and remove its build worktree, if one was
  // stood up (step 4 persists its handle to functional.json).
  if (workDir) {
    const { readFile: rf } = await import("node:fs/promises");
    const handle = await rf(join(workDir, "functional.json"), "utf-8")
      .then((s) => JSON.parse(s))
      .catch(() => null);
    if (handle) {
      const { stop: stopFunctional } = await import("./functional/setup.js");
      await stopFunctional(handle, { repoPath }).catch(() => {});
    }
  }

  if (containerId) {
    await exec("docker", ["stop", containerId]).catch(() => {});
    await exec("docker", ["rm", containerId]).catch(() => {});
  }
  await cleanupWorktree(repoPath, worktreePath, prBranch);
  log(`Teardown complete.`);
}

// ============================================================
// CLI entry point — runs Phase 1 only, leaves server running
// for the agent to continue with enrichment/synthesis.
// ============================================================

if (process.argv[1] && basename(process.argv[1]) === "review.js") {
  const pr = parseInt(process.argv[2], 10);
  if (!pr || isNaN(pr)) {
    console.error("Usage: node review.js <pr-number> [repo-path]");
    console.error("  Runs Phase 1 and outputs evidence JSON.");
    console.error("  Server stays running for enrichment. Call teardown when done.");
    process.exit(1);
  }
  const repoPath = process.argv[3] || process.cwd();

  const session = await setup({ pr, repoPath });
  try {
    const { jsonPath } = await phase1(session);
    log(`Server running on port ${session.handle.port} — ready for enrichment.`);
    log(`Work directory: ${session.workDir}`);
    log(`Worktree: ${session.worktreePath}`);
    log(`To teardown: call teardown(session) or stop container ${session.handle.containerId}`);

    // Phase 1 is done. Close OUR Gremlin connections so this process exits
    // cleanly and completion is detectable (exit code 0 + the sentinel line
    // below). The Gremlin Server container stays up independently — enrichment
    // (scripts/enrichment/cli.js) opens its own connection per command from
    // session.json, so nothing depends on this process lingering.
    await session.connection.close().catch(() => {});
    await session.aConnection.close().catch(() => {});
    log(`PHASE1_COMPLETE pr=${pr} evidence=${jsonPath} port=${session.handle.port} container=${session.handle.containerId}`);
    process.exit(0);
  } catch (err) {
    await teardown(session).catch(() => {});
    throw err;
  }
}
