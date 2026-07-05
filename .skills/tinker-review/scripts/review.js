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
import { extract } from "./extraction/tree-sitter.js";
import { populate } from "./graph/populate.js";
import { populateDiscussions } from "./graph/populate-discussions.js";
import { completeness } from "./patterns/completeness.js";
import { coverageGaps } from "./patterns/coverage-gaps.js";
import { highCentrality } from "./patterns/centrality.js";
import { blastRadius } from "./patterns/blast-radius.js";
import { clusterAnalysis } from "./patterns/cluster-analysis.js";
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

function detectLanguage(changedFiles) {
  const extCounts = new Map();
  for (const file of changedFiles) {
    const ext = extname(file).slice(1);
    if (LANGUAGE_HINTS[ext]) {
      extCounts.set(LANGUAGE_HINTS[ext], (extCounts.get(LANGUAGE_HINTS[ext]) || 0) + 1);
    }
  }

  let best = null;
  let bestCount = 0;
  for (const [lang, count] of extCounts) {
    if (count > bestCount) {
      best = lang;
      bestCount = count;
    }
  }
  return best || "java";
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

function extractKeywords(changedFiles, prTitle) {
  const keywords = new Set();
  for (const file of changedFiles) {
    const parts = file.split("/");
    const filename = parts[parts.length - 1].replace(/\.\w+$/, "");
    if (filename.length > 3 && !["index", "package", "pom", "build"].includes(filename.toLowerCase())) {
      keywords.add(filename);
    }
  }
  const titleWords = prTitle.split(/[\s\-_:]+/).filter((w) => w.length > 3 && !/^\d+$/.test(w));
  for (const w of titleWords.slice(0, 3)) {
    keywords.add(w);
  }
  return [...keywords].slice(0, 6);
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
  const language = detectLanguage(changedFiles);
  const domains = classifyDomains(changedFiles);
  log(`PR #${pr} — classified as: ${domains.join(", ")} (${language}, ${changedFiles.length} files changed)`);

  log(`Starting Gremlin Server...`);
  const handle = await startServer();
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

  log(`Phase 1: Extracting structure (${language})...`);
  const extraction = await extract(worktreePath, language, { changedFiles });
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
  const confidenceResult = await confidenceAudit(g);
  const orphansResult = await orphans(g, { vertexLabel: "Function", expectedEdge: "tests", direction: "in", changedOnly: true });
  log(`  completeness: ${completenessResults.filter(r => r.missing.length > 0).length} gaps found`);
  log(`  coverage_gaps: ${coverageResult.uncovered.length} functions without tests`);
  log(`  centrality: ${centralityResult.aboveThreshold} hotspots`);
  log(`  blast_radius: max ${blastResult.maxReachable} reachable, ${blastResult.types.length} changed types with hierarchy impact`);
  log(`  clusters: ${clusterResult.clusterCount} (${clusterResult.coherent ? "coherent" : "fragmented"})`);
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
  let repoPath, worktreePath, prBranch, containerId;

  if (typeof sessionOrWorkDir === "string") {
    // Called with just a workDir path — read session.json
    const { readFile: rf } = await import("node:fs/promises");
    const sessionData = JSON.parse(await rf(join(sessionOrWorkDir, "session.json"), "utf-8"));
    repoPath = sessionData.repoPath;
    worktreePath = sessionData.worktreePath;
    containerId = sessionData.containerId;
    prBranch = `pr-review/${sessionData.pr}`;
  } else {
    // Called with a live session object
    const session = sessionOrWorkDir;
    repoPath = session.repoPath;
    worktreePath = session.worktreePath;
    prBranch = session.prBranch;
    containerId = session.handle?.containerId;
    if (session.aConnection) await session.aConnection.close().catch(() => {});
    if (session.connection) await session.connection.close().catch(() => {});
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
