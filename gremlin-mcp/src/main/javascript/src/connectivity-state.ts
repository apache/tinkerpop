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

import type { GremlinConnectionError } from './errors.js';

export type GremlinConnectivityState = 'unknown' | 'connected' | 'disconnected';

type ConnectivityListener = (
  next: GremlinConnectivityState,
  previous: GremlinConnectivityState
) => void;

let connectivityState: GremlinConnectivityState = 'unknown';
const listeners = new Set<ConnectivityListener>();

const transitionConnectivityState = (next: GremlinConnectivityState): void => {
  const previous = connectivityState;
  if (previous === next) {
    return;
  }
  connectivityState = next;
  for (const listener of listeners) {
    listener(next, previous);
  }
};

export const markGremlinConnected = (): void => {
  transitionConnectivityState('connected');
};

export const markGremlinDisconnected = (): void => {
  transitionConnectivityState('disconnected');
};

export const getGremlinConnectivityState = (): GremlinConnectivityState => connectivityState;

export const onGremlinConnectivityStateChange = (listener: ConnectivityListener): (() => void) => {
  listeners.add(listener);
  return () => listeners.delete(listener);
};

export const isGremlinConnectionError = (error: unknown): error is GremlinConnectionError =>
  typeof error === 'object' &&
  error !== null &&
  '_tag' in error &&
  (error as { _tag?: string })._tag === 'GremlinConnectionError';

// Test-only utility to reset global singleton state between test cases.
export const resetGremlinConnectivityStateForTests = (): void => {
  connectivityState = 'unknown';
  listeners.clear();
};
