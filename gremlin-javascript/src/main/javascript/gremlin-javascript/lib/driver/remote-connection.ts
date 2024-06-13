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

/**
 * @author Jorge Bay Gondra
 */

import Bytecode from '../process/bytecode.js';
import { TraversalStrategy } from '../process/traversal-strategy.js';
import { Traversal, Traverser } from '../process/traversal.js';
import type { ConnectionOptions } from './connection.js';

export type RemoteConnectionOptions = ConnectionOptions & { session?: string };

/**
 * Represents an abstraction of a "connection" to a "server" that is capable of processing a traversal and
 * returning results.
 */
export abstract class RemoteConnection {
  /**
   * @param {String} url The resource uri.
   * @param {RemoteConnectionOptions} [options] The connection options.
   */
  constructor(
    public url: string,
    protected readonly options: RemoteConnectionOptions = {},
  ) {}

  /**
   * Opens the connection, if its not already opened.
   * @returns {Promise}
   */
  abstract open(): Promise<void>;

  /**
   * Returns true if connection is open
   * @returns {Boolean}
   */
  abstract get isOpen(): boolean;

  /**
   * Determines if the connection is already bound to a session. If so, this indicates that the
   * <code>#createSession()</code> cannot be called so as to produce child sessions.
   * @returns {boolean}
   */
  get isSessionBound(): boolean {
    return false;
  }

  /**
   * Submits the <code>Bytecode</code> provided and returns a <code>RemoteTraversal</code>.
   * @param {Bytecode} bytecode
   * @returns {Promise} Returns a <code>Promise</code> that resolves to a <code>RemoteTraversal</code>.
   */
  abstract submit(bytecode: Bytecode | null): Promise<RemoteTraversal>;

  /**
   * Create a new <code>RemoteConnection</code> that is bound to a session using the configuration from this one.
   * If the connection is already session bound then this function should throw an exception.
   * @returns {RemoteConnection}
   */
  abstract createSession(): RemoteConnection;

  /**
   * Submits a <code>Bytecode.GraphOp.commit</code> to the server and closes the connection.
   * @returns {Promise}
   */
  abstract commit(): Promise<void>;

  /**
   * Submits a <code>Bytecode.GraphOp.rollback</code> to the server and closes the connection.
   * @returns {Promise}
   */
  abstract rollback(): Promise<void>;

  /**
   * Closes the connection where open transactions will close according to the features of the graph provider.
   * @returns {Promise}
   */
  abstract close(): Promise<void>;
}

/**
 * Represents a traversal as a result of a {@link RemoteConnection} submission.
 */
export class RemoteTraversal extends Traversal {
  constructor(
    public traversers: Traverser<any>[],
    public sideEffects?: any[],
  ) {
    super(null, null, new Bytecode());
  }
}

export class RemoteStrategy extends TraversalStrategy {
  /**
   * Creates a new instance of RemoteStrategy.
   * @param {RemoteConnection} connection
   */
  constructor(public connection: RemoteConnection) {
    // gave this a fqcn that has a local "js:" prefix since this strategy isn't sent as bytecode to the server.
    // this is a sort of local-only strategy that actually executes client side. not sure if this prefix is the
    // right way to name this or not, but it should have a name to identify it.
    super('js:RemoteStrategy');
  }

  /** @override */
  apply(traversal: Traversal) {
    if (traversal.traversers) {
      return Promise.resolve();
    }

    return this.connection.submit(traversal.getBytecode()).then(function (remoteTraversal: RemoteTraversal) {
      traversal.sideEffects = remoteTraversal.sideEffects;
      traversal.traversers = remoteTraversal.traversers;
    });
  }
}
