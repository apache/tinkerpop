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

// Bundles entry.js for browsers using esbuild. Fails if any Node built-ins leak into the bundle.
import esbuild from 'esbuild';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import fs from 'node:fs';

const dir = path.dirname(fileURLToPath(import.meta.url));
const packageRoot = path.resolve(dir, '../..');
const generatedDir = path.join(dir, '.generated');

if (!fs.existsSync(path.join(packageRoot, 'build/esm/index.js'))) {
  console.error('build/esm/index.js is missing - run "npm run build" first.');
  process.exit(1);
}

fs.mkdirSync(generatedDir, { recursive: true });

await esbuild.build({
  entryPoints: [path.join(dir, 'entry.js')],
  bundle: true,
  platform: 'browser',
  format: 'iife',
  outfile: path.join(generatedDir, 'bundle.js'),
  logLevel: 'info',
});

// The fixture only ever references "./bundle.js" as a sibling file, so it is served from the same
// generated directory rather than the committed test/browser/ one.
fs.copyFileSync(path.join(dir, 'fixture.html'), path.join(generatedDir, 'fixture.html'));
