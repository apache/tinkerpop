/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

/**
 * @fileoverview Splits a docs page into its heading-bounded sections.
 *
 * The docs converter emits an `<a id="..."></a>` anchor on the line before each heading. A
 * section runs from its anchor to the next heading of the same or a higher level, so it
 * includes its own sub-sections. Sections let a large page be served in part.
 */

/** One heading-bounded section of a page. */
export interface DocSection {
  readonly anchor: string;
  readonly title: string;
  readonly level: number;
  /** The section's Markdown, from its anchor through the end of its last sub-section. */
  readonly markdown: string;
  /** The section's own text: its heading and what precedes its first sub-section. */
  readonly ownText: string;
}

const ANCHOR = /^<a id="([^"]+)"><\/a>$/;
const HEADING = /^(#{1,6})\s+(.+?)\s*#*\s*$/;

/** The anchored sections of a page, in page order. Headings without an anchor are skipped. */
export const parseSections = (markdown: string): DocSection[] => {
  const lines = markdown.split('\n');
  const starts: Array<{ anchor: string; title: string; level: number; line: number }> = [];
  const headingLevels: Array<{ line: number; level: number }> = [];

  let inFence = false;
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i] as string;
    if (line.startsWith('```')) inFence = !inFence;
    if (inFence) continue;
    const heading = HEADING.exec(line);
    if (heading === null) continue;
    const level = (heading[1] as string).length;
    headingLevels.push({ line: i, level });
    const anchor = i > 0 ? ANCHOR.exec(lines[i - 1] as string) : null;
    if (anchor !== null) {
      starts.push({ anchor: anchor[1] as string, title: heading[2] as string, level, line: i - 1 });
    }
  }

  return starts.map(start => {
    const headingLine = start.line + 1;
    const next = headingLevels.find(h => h.line > headingLine && h.level <= start.level);
    // End before the next heading's anchor line, when it has one.
    let end = next === undefined ? lines.length : next.line;
    if (next !== undefined && ANCHOR.test(lines[next.line - 1] ?? '')) end = next.line - 1;
    const firstChild = headingLevels.find(h => h.line > headingLine);
    const ownEnd = firstChild === undefined ? lines.length : Math.min(firstChild.line, end);
    return {
      anchor: start.anchor,
      title: start.title,
      level: start.level,
      markdown: lines.slice(start.line, end).join('\n').trimEnd(),
      ownText: lines.slice(headingLine, ownEnd).join('\n'),
    };
  });
};
