/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

import assert from 'assert';
import path from 'path';
import { fileURLToPath } from 'url';
import ts from 'typescript';

const fixture = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'types', 'enum-types.ts');

describe('Enum typing', function () {
  this.timeout(60000);

  it('should type enum objects with exactly their members', function () {
    const program = ts.createProgram([fixture], {
      noEmit: true,
      strict: true,
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.NodeNext,
      moduleResolution: ts.ModuleResolutionKind.NodeNext,
      lib: ['lib.es2022.d.ts', 'lib.dom.d.ts'],
      skipLibCheck: true,
    });
    const diagnostics = ts.getPreEmitDiagnostics(program)
      .filter((d) => d.file && path.resolve(d.file.fileName) === fixture)
      .map((d) => ts.flattenDiagnosticMessageText(d.messageText, '\n'));
    assert.deepStrictEqual(diagnostics, []);
  });
});
