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

import assert from 'assert';
import { normalizeLogger } from '../../lib/driver/logger.js';

describe('logger', function () {
  it('returns a no-op when no logger is provided', function () {
    const log = normalizeLogger();
    // Should not throw and should not require a target.
    assert.doesNotThrow(() => log('info', 'hello'));
  });

  it('returns a no-op for null', function () {
    const log = normalizeLogger(null);
    assert.doesNotThrow(() => log('warn', 'hello'));
  });

  it('passes through a callback logger with level, message, and args', function () {
    const calls = [];
    const log = normalizeLogger((level, message, ...args) => calls.push([level, message, args]));
    log('debug', 'msg', 1, 2);
    assert.deepStrictEqual(calls, [['debug', 'msg', [1, 2]]]);
  });

  it('dispatches to a logger object method matching the level', function () {
    const calls = [];
    const obj = {
      debug: (m, ...a) => calls.push(['debug', m, a]),
      info: (m, ...a) => calls.push(['info', m, a]),
      warn: (m, ...a) => calls.push(['warn', m, a]),
      error: (m, ...a) => calls.push(['error', m, a]),
    };
    const log = normalizeLogger(obj);
    log('info', 'hi', 'x');
    log('error', 'boom');
    assert.deepStrictEqual(calls, [
      ['info', 'hi', ['x']],
      ['error', 'boom', []],
    ]);
  });

  it('silently ignores levels missing on the logger object', function () {
    const log = normalizeLogger({ error: () => { throw new Error('should not be called'); } });
    assert.doesNotThrow(() => log('debug', 'hello'));
  });

  it('throws for an unsupported logger type', function () {
    assert.throws(() => normalizeLogger(42), /logger must be/);
  });
});
