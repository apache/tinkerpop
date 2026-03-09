#!/usr/bin/env node
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Patches antlr4ng to add CJS TypeScript declaration files.
 *
 * antlr4ng ships a CJS bundle (dist/index.cjs) but lacks the accompanying
 * dist/index.d.cts type declarations that TypeScript's NodeNext module
 * resolution requires when compiling CJS output. This script creates the
 * missing .d.cts file and updates the package exports map so TypeScript
 * correctly resolves types in both ESM and CJS contexts.
 *
 * This is a stop-gap until antlr4ng ships .d.cts files natively.
 * Relevant issues/discussions:
 * - https://github.com/mike-lischke/antlr4ng/issues/23 (CJS/ESM dual module support)
 * - https://github.com/mike-lischke/antlr4ng/issues/26 (TypeScript resolution issues)
 * - https://github.com/mike-lischke/antlr4ng/discussions/21 (Discussion on package structure)
 */

import { copyFileSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const pkgDir = resolve(__dirname, '..', 'node_modules', 'antlr4ng');
const distDir = resolve(pkgDir, 'dist');

// Copy index.d.ts → index.d.cts
const dts = resolve(distDir, 'index.d.ts');
const dcts = resolve(distDir, 'index.d.cts');
copyFileSync(dts, dcts);

// Update exports map to include "types" in the "require" condition
const pkgPath = resolve(pkgDir, 'package.json');
const pkg = JSON.parse(readFileSync(pkgPath, 'utf8'));
pkg.exports = {
  types: './dist/index.d.ts',
  require: { types: './dist/index.d.cts', default: './dist/index.cjs' },
  import: { types: './dist/index.d.ts', default: './dist/index.mjs' },
};
writeFileSync(pkgPath, JSON.stringify(pkg, null, 2) + '\n');

console.log('antlr4ng patched: added dist/index.d.cts and updated exports map');
