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
 * @fileoverview The fixed locations gremlin-mcp reads docs from.
 *
 * Kept apart from the rest of the docs code because it resolves the package directory from
 * `import.meta.url`, which the CommonJS-compiled unit tests cannot load.
 */

import { homedir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { DocsLocations } from './store.js';

/** Directory in the gremlin-mcp package holding the bundled docs version. */
export const BUNDLED_DOCS_DIR = 'gremlin-docs';

/** The package root, two levels above this compiled module (`dist/docs/locations.js`). */
export const packageRoot = (): string => fileURLToPath(new URL('../../', import.meta.url));

/** Where installed docs versions live: `~/.gremlin-mcp/docs/`. */
export const installRoot = (): string => join(homedir(), '.gremlin-mcp', 'docs');

export const defaultDocsLocations = (): DocsLocations => ({
  bundledRoot: join(packageRoot(), BUNDLED_DOCS_DIR),
  installRoot: installRoot(),
});
