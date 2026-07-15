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

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const TEMPLATE = readFileSync(join(__dirname, "template.html"), "utf-8");

function esc(str) {
  return String(str || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// Render a function/type's change level as a badge. STRUCTURAL (signature churn)
// reads loudest; BEHAVIORAL is a body change; FORMATTING is muted; NONE shows
// nothing.
function changeLevelBadge(level) {
  switch (level) {
    case "STRUCTURAL": return `<span class="badge badge-structural">structural</span>`;
    case "BEHAVIORAL": return `<span class="badge badge-modified">behavioral</span>`;
    case "FORMATTING": return `<span class="badge badge-formatting">formatting</span>`;
    default: return "";
  }
}

/**
 * Fields typed as raw *code* (appendixFunctional.testCode / .fullOutput) are
 * wrapped by the renderer in its own `<pre><code>` and escaped. Agents
 * sometimes pre-wrap the value in `<pre>`, `<code>`, or `<p>` anyway; escaping
 * that renders the tags literally. Strip a single leading/trailing wrapper of
 * those kinds so a slip degrades to clean text instead of visible markup. This
 * is a lenient guard, not an HTML sanitizer — the field contract is raw text.
 */
function stripCodeWrapper(str) {
  let s = String(str || "").trim();
  const wrapper = /^<(pre|code|p)(?:\s[^>]*)?>([\s\S]*)<\/\1>$/i;
  // Peel nested wrappers (e.g. `<pre><code>…</code></pre>`), up to two layers.
  // Only peel an *unambiguous* single wrapper: if the inner content still holds
  // the same tag, the string is not one wrapper (e.g. two sibling `<code>`
  // blocks), so leave it alone rather than over-strip.
  for (let i = 0; i < 2; i++) {
    const m = s.match(wrapper);
    if (!m) break;
    const inner = m[2].trim();
    if (new RegExp(`</${m[1]}>`, "i").test(inner)) break;
    s = inner;
  }
  return s;
}

/**
 * Render the full report from evidence data + agent narrative.
 * Output matches the structure of reference-report.html exactly.
 *
 * Input contract — data fields (from review.js):
 *   meta: { pr, title, domains: [], language, changedFileCount, timestamp }
 *   graphStats: { vertices, edges, breakdown: { files, functions, types, tests, calls } }
 *   checks: { completeness, coverageGaps, centrality, blastRadius, clusters, confidence }
 *   discussions: { jiras, devList, secondary, prComments, devListSearchKeywords, ... }
 *   changedFiles: []
 *
 * Input contract — narrative fields (from agent/synthesize.js):
 *   summary: "HTML string"
 *   clusters: { svg: "<svg>...</svg>", assessment: "HTML string" }
 *   guidedWalk: [{ title, badge: "attention|info|safe", badgeText, body: "HTML" }]
 *   functionalTest: { plan: "HTML", results: [{name, pass, output}], observations: ["HTML"] }
 *     (results rows are THEME-level, each naming the scenario labels it covers)
 *   findings: [{ title, snippet: "code", body: "HTML" }]
 *   openQuestions: [{ title, body: "HTML", meta: "string" }]
 *   appendixFunctional: { environment: "HTML", testCode: "raw text", fullOutput: "raw text" }
 *     (testCode = the COMPLETE labeled battery; renderer wraps it in <pre><code>, do NOT pre-wrap)
 */
function notProvided(sectionId, title) {
  return `<section id="${sectionId}">\n  <h2>${esc(title)}</h2>\n  <p class="section-intro" style="color: var(--danger);">Section not provided.</p>\n</section>`;
}

export function render(evidence) {
  const { meta, graphStats, checks, discussions, summary, clusters, communityAssessment,
    guidedWalk, functionalTest, findings, openQuestions, appendixFunctional } = evidence;

  const parts = [];
  parts.push(renderHeader(meta));
  parts.push(renderNav());
  parts.push(summary ? renderSummary(summary) : notProvided("summary", "Summary"));
  parts.push(discussions ? renderContext(discussions) : notProvided("context", "Discovered Context"));
  parts.push(renderClusters(clusters, checks && checks.clusters, evidence.architecture));
  parts.push(renderCommunitySection(checks && checks.communities, communityAssessment));
  parts.push(guidedWalk && guidedWalk.length > 0 ? renderGuidedWalk(guidedWalk) : notProvided("guided-walk", "Guided Walk"));
  parts.push(functionalTest ? renderFunctionalTest(functionalTest) : notProvided("functional-test", "Functional Test"));
  parts.push(findings && findings.length > 0 ? renderFindings(findings) : notProvided("findings", "Findings"));
  parts.push(openQuestions && openQuestions.length > 0 ? renderOpenQuestions(openQuestions) : notProvided("open-questions", "Open Questions"));
  parts.push(renderAppendixStructural(checks, graphStats));
  parts.push(appendixFunctional ? renderAppendixFunctional(appendixFunctional) : notProvided("appendix-functional", "Appendix: Functional Test Details"));
  parts.push(`<footer>Graph Review &mdash; Apache TinkerPop | PR #${esc(String(meta.pr))} | Generated ${esc(meta.timestamp)}</footer>`);

  return TEMPLATE.replace("{{content}}", parts.join("\n\n"));
}

function renderHeader(meta) {
  const domains = (meta.domains || []).join(", ");
  return `<header>
  <h1>PR #${esc(String(meta.pr))} &mdash; ${esc(meta.title)}</h1>
  <div class="meta">
    <span class="badge badge-domain">${esc(domains)}</span>
    <span>${meta.changedFileCount} files changed</span>
  </div>
</header>`;
}

function renderNav() {
  return `<nav>
  <h2>Contents</h2>
  <ul>
    <li><a href="#summary">Summary</a></li>
    <li><a href="#context">Context</a></li>
    <li><a href="#clusters">Clusters</a></li>
    <li><a href="#communities">Communities</a></li>
    <li><a href="#guided-walk">Guided Walk</a></li>
    <li><a href="#functional-test">Functional Test</a></li>
    <li><a href="#findings">Findings</a></li>
    <li><a href="#open-questions">Open Questions</a></li>
    <li><a href="#appendix-structural">Appendix: Structural</a></li>
    <li><a href="#appendix-functional">Appendix: Functional Test</a></li>
  </ul>
</nav>`;
}

function renderSummary(summary) {
  return `<section id="summary">\n  <h2>Summary</h2>\n  ${summary}\n</section>`;
}

function renderContext(disc) {
  const cards = [];

  for (const jira of (disc.jiras || [])) {
    let commentsHtml = "";
    if (jira.comments && jira.comments.length > 0) {
      const items = jira.comments.map(c =>
        `<li><em>${esc(c.author)}:</em> "${esc((c.body || "").slice(0, 150))}"</li>`
      ).join("\n      ");
      commentsHtml = `\n    <p style="margin-top: 0.5rem;"><strong>JIRA comments:</strong></p>\n    <ul style="margin: 0.25rem 0 0 1.5rem; font-size: 0.875rem;">\n      ${items}\n    </ul>`;
    }
    cards.push(`<div class="card card-context">
    <h3><span class="badge badge-context">JIRA</span> <a href="${esc(jira.url)}">${esc(jira.id)}</a>: ${esc(jira.title)}</h3>
    <p>Found in: ${esc(jira.found_in || "pr")}. Status: <strong>${esc(jira.status)}</strong>.</p>${commentsHtml}
  </div>`);
  }

  for (const sec of (disc.secondary || [])) {
    cards.push(`<div class="card card-context">
    <h3><span class="badge badge-context">JIRA</span> <a href="${esc(sec.url)}">${esc(sec.id || "")}</a>: ${esc(sec.title)}</h3>
    <p>Found via: ${esc(sec.found_via)} (${esc(sec.found_in)}).</p>
  </div>`);
  }

  if (disc.devList && disc.devList.length > 0) {
    for (const d of disc.devList) {
      cards.push(`<div class="card card-context">\n    <h3>Dev List: ${esc(d.title)}</h3>\n    <p><a href="${esc(d.url)}">${esc(d.url)}</a></p>\n  </div>`);
    }
  } else {
    const kw = (disc.devListSearchKeywords || []).join(", ");
    const meta = kw ? `\n    <p class="discovery-meta">Searched dev@tinkerpop.apache.org (last 1 year) for: ${esc(kw)}</p>` : "";
    cards.push(`<div class="card">\n    <h3>Dev List</h3>\n    <p>No discussion found.</p>${meta}\n  </div>`);
  }

  if (disc.proposalMissing !== false) {
    cards.push(`<div class="card">\n    <h3>Proposals</h3>\n    <p>No matching proposal in <code>docs/src/dev/future/</code>.</p>\n  </div>`);
  }

  return `<section id="context">\n  <h2>Discovered Context</h2>\n  ${cards.join("\n  ")}\n</section>`;
}

function generateClusterSvg(clusterData, architecture) {
  if (!clusterData || !clusterData.clusters || clusterData.clusters.length === 0) return "";

  const clusters = clusterData.clusters;
  const primary = clusters[0];
  const satellites = clusters.slice(1);
  const hasSatellites = satellites.length > 0;

  const svgWidth = hasSatellites ? 900 : 700;
  const primaryWidth = hasSatellites ? 650 : 680;

  // Group primary cluster files by directory
  const groups = new Map();
  for (const filePath of primary.files) {
    const parts = filePath.split("/");
    const filename = parts.pop();
    const dir = parts.slice(-2).join("/") || "root";
    if (!groups.has(dir)) groups.set(dir, []);
    groups.get(dir).push(filename);
  }

  let svgContent = "";
  const groupEntries = [...groups.entries()].slice(0, 6);

  // Primary cluster background
  const primaryHeight = Math.max(200, groupEntries.length * 70 + 60);
  svgContent += `<rect x="10" y="10" width="${primaryWidth}" height="${primaryHeight}" rx="8" fill="#e8f5e9" stroke="#4caf50" stroke-width="1.5" stroke-dasharray="4"/>`;
  svgContent += `<text x="25" y="32" font-size="11" font-weight="600" fill="#2e7d32">Cluster 1 — Primary (${primary.size} files)</text>`;

  // Render groups inside primary
  const colors = ["#1976d2", "#f57c00", "#7b1fa2", "#388e3c", "#455a64", "#546e7a"];
  let y = 50;
  for (let i = 0; i < groupEntries.length; i++) {
    const [dir, files] = groupEntries[i];
    const color = colors[i % colors.length];
    const boxHeight = Math.min(files.length * 12 + 30, 90);
    svgContent += `<rect x="30" y="${y}" width="280" height="${boxHeight}" rx="5" fill="#fff" stroke="${color}" stroke-width="1.5"/>`;
    svgContent += `<text x="170" y="${y + 18}" font-size="9" font-weight="600" fill="${color}" text-anchor="middle">${esc(dir)}</text>`;
    const shown = files.slice(0, 4);
    for (let j = 0; j < shown.length; j++) {
      svgContent += `<text x="170" y="${y + 33 + j * 12}" font-size="8" fill="#333" text-anchor="middle">${esc(shown[j])}</text>`;
    }
    if (files.length > 4) {
      svgContent += `<text x="170" y="${y + 33 + 4 * 12}" font-size="8" fill="#666" text-anchor="middle">+${files.length - 4} more</text>`;
    }
    y += boxHeight + 10;
  }

  // Satellite clusters
  if (hasSatellites) {
    let satY = 10;
    for (const sat of satellites.slice(0, 3)) {
      const satHeight = Math.min(sat.files.length * 12 + 50, 100);
      svgContent += `<rect x="680" y="${satY}" width="210" height="${satHeight}" rx="8" fill="#fff3e0" stroke="#ff9800" stroke-width="1.5" stroke-dasharray="4"/>`;
      svgContent += `<text x="695" y="${satY + 20}" font-size="10" font-weight="600" fill="#e65100">Satellite (${sat.size} files)</text>`;
      const shown = sat.files.slice(0, 4);
      for (let j = 0; j < shown.length; j++) {
        const fname = shown[j].split("/").pop();
        svgContent += `<text x="785" y="${satY + 38 + j * 13}" font-size="8" fill="#333" text-anchor="middle">${esc(fname)}</text>`;
      }
      satY += satHeight + 15;
    }
  }

  // Draw edges between directory groups based on cross-file function calls
  const archEdges = (architecture && architecture.edges) || [];
  if (archEdges.length > 0 && groupEntries.length > 1) {
    const fileToGroup = new Map();
    for (let i = 0; i < groupEntries.length; i++) {
      const [dir, files] = groupEntries[i];
      for (const f of files) {
        fileToGroup.set(f, i);
      }
    }

    // Also map full paths to group index
    for (const filePath of primary.files) {
      const fname = filePath.split("/").pop();
      const parts = filePath.split("/");
      const dir = parts.slice(-3, -1).join("/") || "root";
      for (let i = 0; i < groupEntries.length; i++) {
        if (groupEntries[i][0] === dir || groupEntries[i][1].includes(fname)) {
          fileToGroup.set(filePath, i);
          break;
        }
      }
    }

    const groupCenters = groupEntries.map((_, i) => ({ x: 170, y: 50 + i * 80 + 35 }));
    const drawnEdges = new Set();

    for (const edge of archEdges) {
      const fromGroup = fileToGroup.get(edge.from) ?? fileToGroup.get(edge.from.split("/").pop());
      const toGroup = fileToGroup.get(edge.to) ?? fileToGroup.get(edge.to.split("/").pop());
      if (fromGroup != null && toGroup != null && fromGroup !== toGroup) {
        const key = `${Math.min(fromGroup, toGroup)}-${Math.max(fromGroup, toGroup)}`;
        if (!drawnEdges.has(key) && groupCenters[fromGroup] && groupCenters[toGroup]) {
          drawnEdges.add(key);
          const from = groupCenters[fromGroup];
          const to = groupCenters[toGroup];
          svgContent += `<line x1="${from.x + 140}" y1="${from.y}" x2="${to.x + 140}" y2="${to.y}" stroke="#999" stroke-width="1.5" stroke-dasharray="3" marker-end="url(#arrow)"/>`;
        }
      }
    }

    if (drawnEdges.size > 0) {
      svgContent = `<defs><marker id="arrow" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M 0 0 L 10 5 L 0 10 z" fill="#999"/></marker></defs>` + svgContent;
    }
  }

  const svgHeight = Math.max(primaryHeight + 20, 200);
  return `<svg class="cluster-svg" viewBox="0 0 ${svgWidth} ${svgHeight}" xmlns="http://www.w3.org/2000/svg">
  ${svgContent}
</svg>`;
}

function renderClusters(clusters, clusterData, architecture) {
  const svg = generateClusterSvg(clusterData, architecture);
  const assessment = (clusters && clusters.assessment) || "<p>No assessment provided.</p>";

  return `<section id="clusters">
  <h2>Change Coherence</h2>
  <p class="section-intro"><strong>Connected components</strong> — the coarse guard: it asks whether any changed file is wholly unreachable from the rest. A strong signal when it fires, but usually one cluster in a well-connected codebase. The finer, density-based view is <a href="#communities">Thematic Communities</a> below.</p>
  ${svg}
  <div class="card card-safe">
    <h3>Assessment</h3>
    ${assessment}
  </div>
</section>`;
}

function renderCommunitySection(communityData, assessment) {
  if (!communityData || !communityData.interpretation) return notProvided("communities", "Thematic Communities");

  const interp = communityData.interpretation;
  const svg = generateCommunitySvg(communityData);

  // The main section is the analyst's meaning-making (enrichment). Until that runs,
  // fall back to the deterministic reading so the section is never empty.
  const body = assessment
    ? assessment
    : `<p>${esc(interp.headline)}</p>${interp.reading.length
        ? `<ul style="margin: 0.5rem 0 0 1.25rem;">${interp.reading.map((r) => `<li>${esc(r)}</li>`).join("")}</ul>`
        : ""}
    <p style="font-size: 0.82rem; color: var(--muted, #666); margin: 0.5rem 0 0;">Automated reading — run enrichment for an analyst's assessment of what these communities mean for this PR.</p>`;

  return `<section id="communities">
  <h2>Thematic Communities <span class="badge">Louvain</span></h2>
  <p class="section-intro">Louvain modularity over the code subgraph (Discussion/Step hubs excluded) groups the change into densely-tied themes even when everything is technically reachable. Box colour encodes change mode — <span style="color:#c62828">red reduced</span>, <span style="color:#2e7d32">green expanded</span>, <span style="color:#e65100">orange reworked</span>, <span style="color:#3949ab">indigo unchanged context</span>. Per-community membership is in the <a href="#appendix-structural">structural appendix</a>.</p>
  ${svg}
  <div class="card ${assessment ? "card-safe" : ""}">
    <h3>Assessment</h3>
    ${body}
  </div>
</section>`;
}

function generateCommunitySvg(communityData) {
  const communities = (communityData && communityData.communities) || [];
  if (communities.length === 0) return "";

  const modeColor = (c) => {
    const m = c.churn && c.churn.mode;
    if (m === "reduced" || m === "purely reduced") return { stroke: "#e53935", fill: "#ffebee", label: "#c62828" };
    if (m === "expanded") return { stroke: "#43a047", fill: "#e8f5e9", label: "#2e7d32" };
    if (m === "reworked") return { stroke: "#fb8c00", fill: "#fff3e0", label: "#e65100" };
    return { stroke: "#5c6bc0", fill: "#e8eaf6", label: "#3949ab" }; // unchanged context / unknown
  };

  const shown = communities.slice(0, 9);
  const cols = Math.min(shown.length, 3);
  const boxW = 270, gapX = 20, gapY = 18;
  const svgWidth = cols * boxW + (cols + 1) * gapX;

  const heightOf = (c) => 44 + Math.min(c.files.length, 4) * 13 + (c.files.length > 4 ? 13 : 0);
  const positions = shown.map((c, i) => ({ c, col: i % cols, row: Math.floor(i / cols) }));
  const numRows = Math.max(...positions.map((p) => p.row)) + 1;
  const rowH = [];
  for (let r = 0; r < numRows; r++) rowH[r] = Math.max(...positions.filter((p) => p.row === r).map((p) => heightOf(p.c)));
  const rowY = [];
  let acc = gapY;
  for (let r = 0; r < numRows; r++) { rowY[r] = acc; acc += rowH[r] + gapY; }

  let content = "";
  for (const { c, col, row } of positions) {
    const x = gapX + col * (boxW + gapX);
    const y = rowY[row];
    const h = rowH[row];
    const cc = modeColor(c);
    const kind = c.role.split(",")[0];
    const churnBadge = c.churn ? `−${c.churn.removed}/+${c.churn.added}` : "unchanged context";
    content += `<rect x="${x}" y="${y}" width="${boxW}" height="${h}" rx="8" fill="${cc.fill}" stroke="${cc.stroke}" stroke-width="1.5"/>`;
    content += `<text x="${x + 12}" y="${y + 19}" font-size="11" font-weight="600" fill="${cc.label}">Community ${c.id + 1} — ${esc(kind)}</text>`;
    content += `<text x="${x + 12}" y="${y + 33}" font-size="9" fill="#555">${c.size} vertices · ${esc(churnBadge)}</text>`;
    const files = c.files.slice(0, 4).map((f) => f.split("/").pop());
    for (let j = 0; j < files.length; j++) {
      content += `<text x="${x + 12}" y="${y + 48 + j * 13}" font-size="8" fill="#333">${esc(files[j])}</text>`;
    }
    if (c.files.length > 4) content += `<text x="${x + 12}" y="${y + 48 + 4 * 13}" font-size="8" fill="#666">+${c.files.length - 4} more files</text>`;
  }

  return `<svg class="cluster-svg" viewBox="0 0 ${svgWidth} ${acc}" xmlns="http://www.w3.org/2000/svg">
  ${content}
</svg>`;
}

function renderGuidedWalk(walk) {
  const steps = walk.map((step, i) => {
    const cls = step.badge === "attention" ? "card-attention" : step.badge === "safe" ? "card-safe" : "card-info";
    const badgeCls = step.badge === "attention" ? "badge-attention" : step.badge === "safe" ? "badge-safe" : "";
    const badge = step.badgeText ? ` <span class="badge ${badgeCls}">${esc(step.badgeText)}</span>` : "";
    return `<div class="card ${cls}">
    <h3><span class="walk-number">${i + 1}</span> ${step.title}${badge}</h3>
    ${step.body}
  </div>`;
  }).join("\n  ");
  return `<section id="guided-walk">\n  <h2>Guided Walk</h2>\n  ${steps}\n</section>`;
}

function renderFunctionalTest(ft) {
  const plan = ft.plan ? `<div class="card card-test">\n    <h3>Test Plan</h3>\n    ${ft.plan}\n  </div>` : "";

  let results = "";
  if (ft.results && ft.results.length > 0) {
    const passed = ft.results.filter(r => r.pass).length;
    const total = ft.results.length;
    const badgeText = passed === total ? "All Pass" : `${passed}/${total}`;
    const grid = ft.results.map(r =>
      `<span class="${r.pass ? "test-pass" : "test-fail"}">${r.pass ? "PASS" : "FAIL"}</span><span>${esc(r.name)}</span><span><code>${esc(r.output || "")}</code></span>`
    ).join("\n      ");
    results = `<div class="card card-safe">
    <h3>Results: ${passed}/${total} passed <span class="badge badge-safe">${badgeText}</span></h3>
    <div class="test-grid">\n      ${grid}\n    </div>
  </div>`;
  }

  let obs = "";
  if (ft.observations && ft.observations.length > 0) {
    const items = ft.observations.map(o => `<li>${o}</li>`).join("\n      ");
    obs = `<div class="card">\n    <h3>Observations</h3>\n    <ul style="margin: 0.5rem 0 0 1.5rem; font-size: 0.9rem;">\n      ${items}\n    </ul>\n  </div>`;
  }

  return `<section id="functional-test">
  <h2>Functional Test</h2>
  <p class="section-intro">Built from PR source and tested against a live Gremlin Server. Tests derived from documentation and Gherkin features only. <a href="#appendix-functional">Full execution details in appendix.</a></p>
  ${plan}
  ${results}
  ${obs}
</section>`;
}

function renderFindings(findings) {
  const cards = findings.map((f, i) => {
    const snippet = f.snippet ? `\n    <pre><code>${esc(f.snippet)}</code></pre>` : "";
    return `<div class="card finding">
    <p class="finding-title">${i + 1}. ${esc(f.title)}</p>${snippet}
    ${f.body}
  </div>`;
  }).join("\n  ");
  return `<section id="findings">\n  <h2>Findings</h2>\n  ${cards}\n</section>`;
}

function renderOpenQuestions(questions) {
  const cards = questions.map(q => {
    const meta = q.meta ? `\n    <p class="discovery-meta">${esc(q.meta)}</p>` : "";
    return `<div class="card">\n    <h3>${esc(q.title)}</h3>\n    ${q.body}${meta}\n  </div>`;
  }).join("\n  ");
  return `<section id="open-questions">\n  <h2>Open Questions</h2>\n  ${cards}\n</section>`;
}

function renderConfidence(confidence) {
  if (!confidence || !confidence.distribution) return "";
  const d = confidence.distribution;
  const ambiguous = confidence.ambiguous || [];

  const chip = (label, value, cls) =>
    `<div class="stat-box"><div class="value">${value || 0}</div><div class="label">${label}</div></div>`;

  let ambiguousHtml;
  if (ambiguous.length === 0) {
    ambiguousHtml = `<p class="section-intro">No AMBIGUOUS edges — nothing was linked purely by keyword search or low-confidence guess.</p>`;
  } else {
    const rows = ambiguous.map(a => {
      const via = a.foundVia ? ` <span class="discovery-meta">via ${esc(a.foundVia)}</span>` : "";
      const found = a.foundIn ? esc(a.foundIn) : "&mdash;";
      return `<tr><td><code>${esc(a.relation)}</code></td><td class="fn-name">${esc(a.from)}</td><td class="fn-name">${esc(a.to)}${via}</td><td>${found}</td></tr>`;
    }).join("\n      ");
    ambiguousHtml = `<table class="gap-table">
    <thead><tr><th>Relation</th><th>From</th><th>To</th><th>Found in</th></tr></thead>
    <tbody>\n      ${rows}\n    </tbody>
  </table>`;
  }

  const untagged = d.UNTAGGED
    ? `<div class="stat-box"><div class="value">${d.UNTAGGED}</div><div class="label">Untagged</div></div>`
    : "";

  return `
  <h3>Signal Confidence</h3>
  <p class="section-intro">Every graph edge is tagged by how it was established. <strong>EXTRACTED</strong> edges are observed directly in source or the git diff; <strong>INFERRED</strong> edges are name-resolved or evidence-backed deductions; <strong>AMBIGUOUS</strong> edges are keyword-search or low-confidence guesses and are listed below for human review.</p>
  <div class="stats-grid">
    ${chip("Extracted", d.EXTRACTED)}
    ${chip("Inferred", d.INFERRED)}
    ${chip("Ambiguous", d.AMBIGUOUS)}
    ${untagged}
  </div>
  <h4 style="margin-top: 1rem;">Ambiguous edges (${ambiguous.length}) &mdash; verify before relying on</h4>
  ${ambiguousHtml}
`;
}

function renderAppendixStructural(checks, graphStats) {
  const hotspots = checks?.centrality?.hotspots || [];
  const blast = checks?.blastRadius?.functions || [];
  const stats = graphStats || {};
  const bd = stats.breakdown || {};
  const confidenceHtml = renderConfidence(checks?.confidence);

  const hotspotRows = hotspots.slice(0, 10).map(h => {
    const badge = changeLevelBadge(h.changeLevel);
    return `<tr><td class="fn-name">${esc(h.name)}</td><td>${esc((h.filePath || "").split("/").pop())}</td><td class="num">${h.inDegree}</td><td class="num">${h.outDegree}</td><td class="num"><strong>${h.totalDegree}</strong></td><td>${badge}</td></tr>`;
  }).join("\n      ");

  const blastRows = blast.slice(0, 10).map(b => {
    const badge = changeLevelBadge(b.changeLevel);
    return `<tr><td class="fn-name">${esc(b.name)}</td><td>${esc((b.filePath || "").split("/").pop())}</td><td class="num">${b.reachableCount}</td><td>${badge}</td></tr>`;
  }).join("\n      ");

  const hierarchy = checks?.blastRadius?.types || [];
  const truncated = checks?.blastRadius?.neighborhood?.truncated;
  const hierarchyRows = hierarchy.slice(0, 10).map(t => {
    return `<tr><td class="fn-name">${esc(t.name)}</td><td>${esc(t.kind || "")}</td><td>${esc((t.filePath || "").split("/").pop())}</td><td class="num">${t.implementerCount}</td></tr>`;
  }).join("\n      ");
  const communities = checks?.communities?.communities || [];
  const communityRows = communities.map(c => {
    const labels = Object.entries(c.labelCounts || {}).sort((a, b) => b[1] - a[1]).map(([l, n]) => `${l}:${n}`).join(", ");
    const files = c.files.map(f => `<code>${esc(f)}</code>`).join("<br>");
    return `<tr><td>${c.id + 1}</td><td>${esc(c.role)}</td><td class="num">${c.size}</td><td>${esc(labels)}</td><td>${files}</td></tr>`;
  }).join("\n      ");
  const isolated = checks?.communities?.isolatedCount ?? 0;
  const communitiesHtml = communities.length === 0 ? "" : `
  <h3>Community Membership</h3>
  <p class="section-intro">Louvain communities over the code subgraph, largest first (modularity ${checks?.communities?.modularity}). The reading is in <a href="#communities">Thematic Communities</a>; this is the raw membership. Role carries the change mode and line churn (−removed/+added). ${isolated} vertices have no in-subgraph edge and are omitted; every clustered vertex carries a <code>community</code> property for direct querying.</p>
  <table class="gap-table">
    <thead><tr><th>#</th><th>Role</th><th>Vertices</th><th>Label mix</th><th>Files</th></tr></thead>
    <tbody>\n      ${communityRows}\n    </tbody>
  </table>`;

  const truncNote = truncated ? ` <strong>Neighborhood truncated — these counts are a lower bound.</strong>` : "";
  const hierarchyHtml = hierarchy.length === 0 ? "" : `
  <h3>Type Hierarchy Impact</h3>
  <p class="section-intro">For each changed type (typically an interface), the number of functions declared by everything that implements or extends it within 3 levels — impact that flows through the type hierarchy rather than direct calls.${truncNote}</p>
  <table class="gap-table">
    <thead><tr><th>Type</th><th>Kind</th><th>File</th><th>Implementer fns</th></tr></thead>
    <tbody>\n      ${hierarchyRows}\n    </tbody>
  </table>`;

  return `<section id="appendix-structural">
  <h2>Appendix: Structural Data</h2>

  <h3>Structural Hotspots</h3>
  <p class="section-intro">Functions with the most incoming/outgoing call edges among changed code. Inherently-central methods (equals, toString) included only when modified by this PR.</p>
  <table class="gap-table">
    <thead><tr><th>Function</th><th>File</th><th>In</th><th>Out</th><th>Total</th><th></th></tr></thead>
    <tbody>\n      ${hotspotRows}\n    </tbody>
  </table>

  <h3>Blast Radius</h3>
  <p class="section-intro">Reachable callers and overriders within 3 hops upstream — higher means more code affected by behavioral changes. Includes impact flowing through interface/abstract overrides, not just direct calls.</p>
  <table class="gap-table">
    <thead><tr><th>Function</th><th>File</th><th>Reachable</th><th></th></tr></thead>
    <tbody>\n      ${blastRows}\n    </tbody>
  </table>
${hierarchyHtml}
${communitiesHtml}
${confidenceHtml}
  <h3>Graph Statistics</h3>
  <div class="stats-grid">
    <div class="stat-box"><div class="value">${stats.vertices || 0}</div><div class="label">Vertices</div></div>
    <div class="stat-box"><div class="value">${stats.edges || 0}</div><div class="label">Edges</div></div>
    <div class="stat-box"><div class="value">${bd.files || 0}</div><div class="label">Files</div></div>
    <div class="stat-box"><div class="value">${bd.functions || 0}</div><div class="label">Functions</div></div>
    <div class="stat-box"><div class="value">${bd.types || 0}</div><div class="label">Types</div></div>
    <div class="stat-box"><div class="value">${bd.tests || 0}</div><div class="label">Tests</div></div>
  </div>
</section>`;
}

function renderAppendixFunctional(af) {
  return `<section id="appendix-functional">
  <h2>Appendix: Functional Test Details</h2>

  <h3>Execution Environment</h3>
  <div class="card">\n    ${af.environment}\n  </div>

  <h3>Test Code</h3>
  <p class="section-intro">Complete, unabbreviated test battery. Each scenario is labeled in a comment; the Functional Test section refers to these labels.</p>
  <pre><code>${esc(stripCodeWrapper(af.testCode))}</code></pre>

  <h3>Full Output</h3>
  <pre><code>${esc(stripCodeWrapper(af.fullOutput))}</code></pre>
</section>`;
}

// CLI: node render.js <input.json> [output.html]
import { basename } from "node:path";
if (process.argv[1] && basename(process.argv[1]) === "render.js") {
  const inputPath = process.argv[2];
  if (!inputPath) {
    console.error("Usage: node render.js <evidence.json> [output.html]");
    process.exit(1);
  }
  const outputPath = process.argv[3] || inputPath.replace(/\.json$/, ".html");
  const { readFileSync, writeFileSync } = await import("node:fs");
  const evidence = JSON.parse(readFileSync(inputPath, "utf-8"));
  const html = render(evidence);
  writeFileSync(outputPath, html, "utf-8");
  console.log(`Report written: ${outputPath}`);
}
