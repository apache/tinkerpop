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

import GremlinLang from '../process/gremlin-lang.js';
import { TraversalStrategy } from '../process/traversal-strategy.js';
import { Traversal, Traverser } from '../process/traversal.js';
import type { ConnectionOptions } from './connection.js';

/**
 * Represents an abstraction of a "connection" to a "server" that is capable of processing a traversal and
 * returning results.
 */
export abstract class RemoteConnection {
  /**
   * @param {String} url The resource uri.
   * @param {ConnectionOptions} [options] The connection options.
   */
  constructor(
    public url: string,
    public readonly options: ConnectionOptions = {},
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
   * Submits the <code>GremlinLang</code> provided and returns a <code>RemoteTraversal</code>.
   * @param {GremlinLang} gremlinLang
   * @returns {Promise} Returns a <code>Promise</code> that resolves to a <code>RemoteTraversal</code>.
   */
  abstract submit(gremlinLang: GremlinLang): Promise<RemoteTraversal>;

  /**
   * Closes the connection and releases any associated resources.
   * @returns {Promise}
   */
  abstract close(): Promise<void>;
}

/**
 * Represents a traversal as a result of a {@link RemoteConnection} submission.
 */
export class RemoteTraversal extends Traversal {
  constructor(
    source: AsyncGenerator<any>,
    sideEffects?: any[],
  ) {
    super(null, null);
    this._resultsStream = source;
  }
}

export class RemoteStrategy extends TraversalStrategy {
  /**
   * Creates a new instance of RemoteStrategy.
   * @param {RemoteConnection} connection
   */
  constructor(public connection: RemoteConnection) {
    super();
  }

  /** @override */
  apply(traversal: Traversal) {
    if (traversal._resultsStream) {
      return Promise.resolve();
    }

    return this.connection.submit(traversal.getGremlinLang()).then(function (remoteTraversal: RemoteTraversal) {
      traversal.sideEffects = remoteTraversal.sideEffects;
      traversal._resultsStream = remoteTraversal._resultsStream;
    });
  }
}
