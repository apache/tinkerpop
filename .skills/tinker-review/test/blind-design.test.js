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

// The blind design pass appendix: absent, skipped, and reconciled forms.

import { test } from "node:test";
import assert from "node:assert/strict";

import { render } from "../scripts/renderer/render.js";

const meta = { pr: 1, headSha: "abc123", title: "t", domains: [], timestamp: "now" };

/** The appendix section of a rendered report. */
function appendix(html) {
  const m = html.match(/<section id="appendix-blind-design">[\s\S]*?<\/section>/);
  assert.ok(m, "appendix section present");
  return m[0];
}

test("an absent appendixBlindDesign renders as not provided and is in the nav", () => {
  const html = render({ meta });
  assert.match(html, /href="#appendix-blind-design"/);
  assert.match(appendix(html), /Section not provided/);
});

test("a skipped pass renders its gate reason", () => {
  const html = render({ meta, appendixBlindDesign: { skipped: "<p>Tests-only change.</p>" } });
  const section = appendix(html);
  assert.match(section, /Skipped\./);
  assert.match(section, /<p>Tests-only change\.<\/p>/);
  assert.doesNotMatch(section, /<h3>Objections<\/h3>/);
});

test("surfaces and reconciled objections render with outcome badges, escaping plain-text fields", () => {
  const html = render({
    meta,
    appendixBlindDesign: {
      surfaces: [{ id: "S1", kind: "api", name: "GqlExecutor.execute()", commitment: "Providers implement Iterator<GqlRow>" }],
      objections: [
        { id: "S1.a", surface: "S1", type: "alternative", claim: "Use Object[] rows", outcome: "openQuestion", note: "<p>Not weighed in <code>tp-x.1</code>.</p>" },
        { id: "S1.b", surface: "S1", type: "premise", claim: "Spec says X", outcome: "finding", note: "" },
        { id: "S1.c", surface: "S1", type: "counterargument", claim: "Breaks beta users", outcome: "dropped", note: "<p>Answered.</p>" },
      ],
    },
  });
  const section = appendix(html);
  assert.match(section, /Providers implement Iterator&lt;GqlRow&gt;/);
  assert.match(section, /<span class="badge badge-context">open question<\/span>/);
  assert.match(section, /<span class="badge badge-attention">finding<\/span>/);
  assert.match(section, /<span class="badge badge-safe">answered by record<\/span>/);
  assert.match(section, /<code>tp-x\.1<\/code>/);
});

test("a pass with no objections says so", () => {
  const html = render({ meta, appendixBlindDesign: { surfaces: [{ id: "S1", kind: "grammar", name: "q", commitment: "c" }], objections: [] } });
  assert.match(appendix(html), /raised no objections/);
});
