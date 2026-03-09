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

import assert from 'assert';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { GremlinTranslator } from '../../../lib/language/translator/GremlinTranslator.js';
import { TranslatorException } from '../../../lib/language/translator/TranslatorException.js';
import { Translator } from '../../../lib/language/translator/Translator.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Search for translations.json up from the current directory
function findTranslationsPath() {
  let current = __dirname;
  while (current !== path.parse(current).root) {
    const p = path.join(
      current,
      'gremlin-test/src/main/resources/org/apache/tinkerpop/gremlin/language/translator/translations.json',
    );
    if (fs.existsSync(p)) return p;
    current = path.dirname(current);
  }
  return null;
}

const translationsPath = findTranslationsPath();

describe('GremlinTranslator corpus', function () {
  let scenarios = [];
  if (translationsPath && fs.existsSync(translationsPath)) {
    scenarios = JSON.parse(fs.readFileSync(translationsPath, 'utf8'));
  }

  it('translations.json should be present and loaded', function () {
    assert.ok(
      translationsPath !== null,
      'Could not find translations.json in the project structure. Run mvn generate-resources in gremlin-test.',
    );
    assert.ok(scenarios.length > 0, 'translations.json is empty');
  });

  describe('per-scenario translation', function () {
    scenarios.forEach((scenario) => {
      scenario.traversals.forEach((traversal) => {
        const { original } = traversal;

        // JavaScript
        const jsExpected = traversal['javascript'];
        if (jsExpected !== undefined && jsExpected !== null) {
          it(`[javascript] translates [${scenario.scenario}]: ${original}`, function () {
            const result = GremlinTranslator.translate(original, 'g', 'JAVASCRIPT');
            assert.strictEqual(result.getTranslated(), jsExpected);
          });
        } else {
          it(`[javascript] throws for untranslatable [${scenario.scenario}]: ${original}`, function () {
            assert.throws(() => GremlinTranslator.translate(original, 'g', 'JAVASCRIPT'), TranslatorException);
          });
        }

        // Python
        const pyExpected = traversal['python'];
        if (pyExpected !== undefined && pyExpected !== null) {
          it(`[python] translates [${scenario.scenario}]: ${original}`, function () {
            const result = GremlinTranslator.translate(original, 'g', 'PYTHON');
            assert.strictEqual(result.getTranslated(), pyExpected);
          });
        } else {
          it(`[python] throws for untranslatable [${scenario.scenario}]: ${original}`, function () {
            assert.throws(() => GremlinTranslator.translate(original, 'g', 'PYTHON'), TranslatorException);
          });
        }

        // .NET
        const dotnetExpected = traversal['dotnet'];
        if (dotnetExpected !== undefined && dotnetExpected !== null) {
          it(`[dotnet] translates [${scenario.scenario}]: ${original}`, function () {
            const result = GremlinTranslator.translate(original, 'g', 'DOTNET');
            assert.strictEqual(result.getTranslated(), dotnetExpected);
          });
        } else {
          it(`[dotnet] throws for untranslatable [${scenario.scenario}]: ${original}`, function () {
            assert.throws(() => GremlinTranslator.translate(original, 'g', 'DOTNET'), TranslatorException);
          });
        }

        // Go
        const goExpected = traversal['go'];
        if (goExpected !== undefined && goExpected !== null) {
          it(`[go] translates [${scenario.scenario}]: ${original}`, function () {
            const result = GremlinTranslator.translate(original, 'g', 'GO');
            assert.strictEqual(result.getTranslated(), goExpected);
          });
        } else {
          it(`[go] throws for untranslatable [${scenario.scenario}]: ${original}`, function () {
            assert.throws(() => GremlinTranslator.translate(original, 'g', 'GO'), TranslatorException);
          });
        }

        // Anonymized
        const anonymizedExpected = traversal['anonymized'];
        if (anonymizedExpected !== undefined && anonymizedExpected !== null) {
          it(`[anonymized] translates [${scenario.scenario}]: ${original}`, function () {
            const result = GremlinTranslator.translate(original, 'g', 'ANONYMIZED');
            assert.strictEqual(result.getTranslated(), anonymizedExpected);
          });
        } else {
          it(`[anonymized] throws for untranslatable [${scenario.scenario}]: ${original}`, function () {
            assert.throws(() => GremlinTranslator.translate(original, 'g', 'ANONYMIZED'), TranslatorException);
          });
        }

        // Groovy
        const groovyExpected = traversal['groovy'];
        if (groovyExpected !== undefined && groovyExpected !== null) {
          it(`[groovy] translates [${scenario.scenario}]: ${original}`, function () {
            const result = GremlinTranslator.translate(original, 'g', 'GROOVY');
            assert.strictEqual(result.getTranslated(), groovyExpected);
          });
        } else {
          it(`[groovy] throws for untranslatable [${scenario.scenario}]: ${original}`, function () {
            assert.throws(() => GremlinTranslator.translate(original, 'g', 'GROOVY'), TranslatorException);
          });
        }

        // Java
        const javaExpected = traversal['java'];
        if (javaExpected !== undefined && javaExpected !== null) {
          it(`[java] translates [${scenario.scenario}]: ${original}`, function () {
            const result = GremlinTranslator.translate(original, 'g', 'JAVA');
            assert.strictEqual(result.getTranslated(), javaExpected);
          });
        } else {
          it(`[java] throws for untranslatable [${scenario.scenario}]: ${original}`, function () {
            assert.throws(() => GremlinTranslator.translate(original, 'g', 'JAVA'), TranslatorException);
          });
        }
      });
    });
  });
});
