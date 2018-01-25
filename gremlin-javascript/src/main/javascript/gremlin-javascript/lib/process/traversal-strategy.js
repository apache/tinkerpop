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
'use strict';

const utils = require('../utils');

class TraversalStrategies {
  /**
   * Creates a new instance of TraversalStrategies.
   * @param {TraversalStrategies} [parent] The parent strategies from where to clone the values from.
   * @constructor
   */
  constructor(parent) {
    if (parent) {
      // Clone the strategies
      this.strategies = [...parent.strategies];
    }
    else {
      this.strategies = [];
    }
  }

  /** @param {TraversalStrategy} strategy */
  addStrategy(strategy) {
    this.strategies.push(strategy);
  }

  /**
   * @param {Traversal} traversal
   * @returns {Promise}
   */
  applyStrategies(traversal) {
    // Apply all strategies serially
    return this.strategies.reduce((promise, strategy) => {
      return promise.then(() => strategy.apply(traversal));
    }, Promise.resolve());
  }
}

/** @abstract */
class TraversalStrategy {
  /**
   * @abstract
   * @param {Traversal} traversal
   * @returns {Promise}
   */
  apply(traversal) {

  }
}

module.exports = {
  TraversalStrategies: TraversalStrategies,
  TraversalStrategy: TraversalStrategy
};