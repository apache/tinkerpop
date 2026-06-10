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

import { get } from "node:https";
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";

const JIRA_BASE = "https://issues.apache.org/jira";
const DEV_LIST_API = "https://lists.apache.org/api/stats.lua";
const TINKERPOP_JIRA_PATTERN = /TINKERPOP-(\d+)/g;
const DEV_LIST_LINK_PATTERN = /https?:\/\/lists\.apache\.org\/[^\s)\]>"]*/g;
const PROPOSAL_DIR = "docs/src/dev/future";

function httpGet(url) {
  return new Promise((resolve, reject) => {
    get(url, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return httpGet(res.headers.location).then(resolve, reject);
      }
      let data = "";
      res.on("data", (chunk) => { data += chunk; });
      res.on("end", () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve(data);
        } else {
          reject(new Error(`HTTP ${res.statusCode}: ${url}`));
        }
      });
    }).on("error", reject);
  });
}

async function fetchJira(ticketId) {
  try {
    const url = `${JIRA_BASE}/rest/api/2/issue/${ticketId}?fields=summary,description,status,issuetype,comment`;
    const data = JSON.parse(await httpGet(url));
    const comments = (data.fields?.comment?.comments || []).map((c) => ({
      author: c.author?.displayName || c.author?.name || "unknown",
      body: (c.body || "").slice(0, 1000),
      timestamp: c.created,
    }));
    return {
      id: ticketId,
      url: `${JIRA_BASE}/browse/${ticketId}`,
      source: "jira",
      title: data.fields?.summary || ticketId,
      body: (data.fields?.description || "").slice(0, 2000),
      status: data.fields?.status?.name || "unknown",
      type: data.fields?.issuetype?.name || "unknown",
      comments,
    };
  } catch {
    return null;
  }
}

async function fetchPrComments(prNumber) {
  try {
    const url = `https://api.github.com/repos/apache/tinkerpop/issues/${prNumber}/comments?per_page=50`;
    const data = JSON.parse(await httpGet(url));
    if (!Array.isArray(data)) return [];
    return data.map((c) => ({
      author: c.user?.login || "unknown",
      body: (c.body || "").slice(0, 1000),
      timestamp: c.created_at,
    }));
  } catch {
    return [];
  }
}

async function fetchPrReviewComments(prNumber) {
  try {
    const url = `https://api.github.com/repos/apache/tinkerpop/pulls/${prNumber}/comments?per_page=50`;
    const data = JSON.parse(await httpGet(url));
    if (!Array.isArray(data)) return [];
    return data.map((c) => ({
      author: c.user?.login || "unknown",
      body: (c.body || "").slice(0, 1000),
      path: c.path || "",
      timestamp: c.created_at,
    }));
  } catch {
    return [];
  }
}

async function searchDevList(keywords) {
  try {
    const query = encodeURIComponent(keywords.join(" "));
    const url = `${DEV_LIST_API}?list=dev&domain=tinkerpop.apache.org&q=${query}&d=lte=1y`;
    const data = JSON.parse(await httpGet(url));
    if (!data.emails || data.emails.length === 0) return [];

    return data.emails
      .filter((e) => e.subject && !e.subject.startsWith("Re:"))
      .slice(0, 5)
      .map((e) => ({
        url: `https://lists.apache.org/thread/${e.mid}`,
        source: "devlist",
        title: e.subject,
        body: (e.body || "").slice(0, 1000),
        date: new Date(e.epoch * 1000).toISOString().slice(0, 10),
      }));
  } catch {
    return [];
  }
}

async function findMatchingProposals(repoPath, keywords) {
  const proposalDir = join(repoPath, PROPOSAL_DIR);
  try {
    const files = await readdir(proposalDir);
    const proposals = [];

    for (const file of files) {
      if (!file.endsWith(".asciidoc") && !file.endsWith(".adoc")) continue;
      const content = await readFile(join(proposalDir, file), "utf-8");
      const lowerContent = content.toLowerCase();
      const matchedKeywords = keywords.filter((k) => lowerContent.includes(k.toLowerCase()));
      if (matchedKeywords.length > 0) {
        const titleMatch = content.match(/^=+\s*(.+)$/m);
        proposals.push({
          file,
          path: `${PROPOSAL_DIR}/${file}`,
          source: "proposal",
          title: titleMatch ? titleMatch[1].trim() : file,
          matchedKeywords,
          snippet: content.slice(0, 500),
        });
      }
    }

    return proposals.sort((a, b) => b.matchedKeywords.length - a.matchedKeywords.length);
  } catch {
    return [];
  }
}

function extractLinksFromText(text) {
  const jiraRefs = [...new Set([...text.matchAll(TINKERPOP_JIRA_PATTERN)].map((m) => m[0]))];
  const devListRefs = [...new Set([...text.matchAll(DEV_LIST_LINK_PATTERN)].map((m) => m[0]))];
  return { jiraRefs, devListRefs };
}

async function followLinks(discussions) {
  const secondary = [];

  for (const disc of discussions) {
    const commentBodies = (disc.comments || []).map((c) => c.body).join("\n");
    const textToScan = [disc.body || "", disc.title || "", commentBodies].join("\n");
    const { jiraRefs, devListRefs } = extractLinksFromText(textToScan);

    for (const jiraId of jiraRefs) {
      if (discussions.some((d) => d.id === jiraId)) continue;
      const jira = await fetchJira(jiraId);
      if (jira) {
        secondary.push({
          ...jira,
          found_in: `${disc.source}_body`,
          found_via: disc.id || disc.url,
        });
      }
    }

    for (const url of devListRefs) {
      if (discussions.some((d) => d.url === url)) continue;
      secondary.push({
        url,
        source: "devlist",
        title: "(referenced thread)",
        body: "",
        found_in: `${disc.source}_body`,
        found_via: disc.id || disc.url,
      });
    }
  }

  return secondary;
}

/**
 * Discover discussions and proposals related to a PR.
 *
 * Strategy:
 * - JIRA: scan all text for TINKERPOP-XXXX. Fetch if found. If absent, note missing.
 * - Dev list: scan for explicit links. If none, search archives (max 1 year back).
 *   If search returns nothing, note missing.
 * - Proposals: check docs/src/dev/future/ for documents matching PR keywords.
 *   If not referenced anywhere, still check by keyword match.
 * - Cross-reference: follow one hop from each discovered discussion to find
 *   additional JIRAs and dev list threads referenced within them.
 *
 * @param {object} params
 * @param {number} params.pr - PR number (for fetching comments from GitHub)
 * @param {string} params.prTitle - PR title
 * @param {string} params.prBody - PR body/description
 * @param {string} params.diff - The full diff text
 * @param {string[]} params.keywords - Keywords for search (class names, step names, etc.)
 * @param {string} params.repoPath - Path to the repo (for proposal discovery)
 * @returns {Promise<DiscoveryResult>}
 */
export async function discoverDiscussions(params) {
  const { pr, prTitle = "", prBody = "", diff = "", keywords = [], repoPath = "" } = params;

  // Fetch PR comments from GitHub (issue comments + inline review comments)
  const issueComments = pr ? await fetchPrComments(pr) : [];
  const reviewComments = pr ? await fetchPrReviewComments(pr) : [];
  const prCommentBodies = [...issueComments, ...reviewComments].map((c) => c.body);

  const allText = [prTitle, prBody, ...prCommentBodies, diff].join("\n");

  // --- JIRA (direct) ---
  const jiraMatches = [...new Set([...allText.matchAll(TINKERPOP_JIRA_PATTERN)].map((m) => m[0]))];
  const jiras = (await Promise.all(jiraMatches.map(fetchJira))).filter(Boolean);
  for (const j of jiras) { j.found_in = "pr"; }

  // --- Dev list (direct) ---
  const devListLinks = [...new Set([...allText.matchAll(DEV_LIST_LINK_PATTERN)].map((m) => m[0]))];
  const explicitDevList = devListLinks.map((url) => ({
    url,
    source: "devlist",
    title: "(linked thread)",
    body: "",
    found_in: "pr",
  }));

  let searchedDevList = [];
  let devListSearchPerformed = false;
  if (explicitDevList.length === 0 && keywords.length > 0) {
    devListSearchPerformed = true;
    searchedDevList = await searchDevList(keywords);
    for (const d of searchedDevList) { d.found_in = "search"; }
  }

  // --- Cross-reference: follow one hop from direct discoveries ---
  const directDiscussions = [...jiras, ...explicitDevList, ...searchedDevList];
  const secondaryDiscussions = await followLinks(directDiscussions);

  // --- Proposals ---
  const proposalLinks = [...allText.matchAll(/docs\/src\/dev\/future\/[^\s)\]>"]+/g)].map((m) => m[0]);
  let proposals = [];
  if (repoPath) {
    proposals = await findMatchingProposals(repoPath, keywords);
  }

  return {
    jiras,
    jiraMissing: jiraMatches.length === 0,

    devList: [...explicitDevList, ...searchedDevList],
    devListMissing: explicitDevList.length === 0 && searchedDevList.length === 0,
    devListSearchPerformed,
    devListSearchKeywords: devListSearchPerformed ? keywords : [],

    secondary: secondaryDiscussions,

    prComments: { issue: issueComments, review: reviewComments },

    proposals,
    proposalLinked: proposalLinks.length > 0,
    proposalMissing: proposalLinks.length === 0 && proposals.length === 0,
  };
}
