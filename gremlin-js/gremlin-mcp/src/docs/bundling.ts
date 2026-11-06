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
 * @fileoverview Copies this checkout's docs into the gremlin-mcp package.
 *
 * Reads the Markdown docs tree built by `bin/process-docs.sh` (`target/docs/markdown` at the
 * repository root), selects the reference and Gremlin Semantics pages, and writes them to
 * `gremlin-docs/<version>/` in the package. The version comes from the llms.txt pointer the
 * docs build stamps on each page.
 *
 * Run by the npm build, where missing docs only produce a warning, and with `--required` by
 * the npm deploy, where missing or mismatched docs fail the release.
 */

import { readFile, readdir, rm } from 'node:fs/promises';
import { join, relative, sep } from 'node:path';
import { selectPages, REFERENCE_DIR } from './selection.js';
import { writeVersion } from './store.js';

const POINTER = /tinkerpop\.apache\.org\/docs\/([^/\s)]+)\/llms\.txt/;

/** Every file under a directory, as `/`-separated paths relative to it. */
const listFiles = async (dir: string): Promise<string[]> => {
  const entries = await readdir(dir, { recursive: true, withFileTypes: true });
  return entries
    .filter(e => e.isFile())
    .map(e => relative(dir, join(e.parentPath, e.name)).split(sep).join('/'));
};

/** Bundles the docs; returns a message describing what happened. */
export const bundleDocs = async (
  sourceDir: string,
  destRoot: string,
  expectedVersion: string,
  required: boolean
): Promise<string> => {
  await rm(destRoot, { recursive: true, force: true });

  let files: string[];
  try {
    files = await listFiles(sourceDir);
  } catch {
    files = [];
  }
  const landing = `${REFERENCE_DIR}index.md`;
  if (!files.includes(landing)) {
    const message =
      `No Markdown docs found at ${sourceDir}; gremlin-mcp is packaged without bundled docs. ` +
      'Run bin/process-docs.sh first to bundle them.';
    if (required) throw new Error(message);
    return `WARNING: ${message}`;
  }

  const read = (path: string) => readFile(join(sourceDir, ...path.split('/')), 'utf8');
  const version = POINTER.exec(await read(landing))?.[1];
  if (version === undefined) throw new Error(`No docs version found in ${landing}`);
  if (version !== expectedVersion) {
    const message = `Docs at ${sourceDir} are for ${version}, but gremlin-mcp is ${expectedVersion}`;
    if (required) throw new Error(message);
    return `WARNING: ${message}; not bundling them.`;
  }

  const pages = await selectPages(files, read);
  const contents = new Map<string, string>();
  for (const page of pages) contents.set(page, await read(page));
  const dir = await writeVersion(
    destRoot,
    { version, source: 'bundled', created: new Date().toISOString(), pages },
    contents
  );
  return `Bundled ${pages.length} docs pages for ${version} into ${dir}`;
};
