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
 * @fileoverview Tests for describing the cause of a failure in error messages.
 */

import { Cause } from 'effect';
import { describe, it, expect } from '@jest/globals';
import { describeCause, Errors } from '../src/errors';

/**
 * Shaped like the gremlin driver's ResponseError, which keeps the server's status message apart from its own.
 */
class FakeResponseError extends Error {
  constructor(
    message: string,
    readonly statusMessage: string
  ) {
    super(message);
  }
}

describe('describeCause', () => {
  it('should give the message of an error', () => {
    expect(describeCause(new Error('connection refused'))).toBe('connection refused');
  });

  it('should add the server status message a driver error carries', () => {
    const error = new FakeResponseError(
      'Server returned HTTP 500: Internal Server Error',
      'The provided traversal has a mutating step and thus is not read only'
    );
    expect(describeCause(error)).toBe(
      'Server returned HTTP 500: Internal Server Error: The provided traversal has a mutating step and thus is not read only'
    );
  });

  it('should not repeat a status message the error message already holds', () => {
    const error = new FakeResponseError('Server error: not read only (500)', 'not read only');
    expect(describeCause(error)).toBe('Server error: not read only (500)');
  });

  it('should look inside an exception Effect wrapped', () => {
    expect(describeCause(new Cause.UnknownException(new Error('timed out')))).toBe('timed out');
  });

  it('should describe values that are not errors', () => {
    expect(describeCause('bad request')).toBe('bad request');
    expect(describeCause(undefined)).toBeUndefined();
  });

  it('should keep the cause in a query error message', () => {
    const error = Errors.query(describeCause(new Error('not read only')) ?? 'unknown');
    expect(error.message).toBe('Query failed: not read only');
  });
});
