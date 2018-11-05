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

const util = require('util');
const inspect = util.inspect.custom || 'inspect';
const utils = require('../utils');
const emptyMap = Object.freeze(new utils.ImmutableMap());

/**
 * Represents the response returned from the execution of a Gremlin traversal or script.
 */
class ResultSet {

  /**
   * Creates a new instance of {@link ResultSet}.
   * @param {Array} items
   * @param {Map} [attributes]
   */
  constructor(items, attributes) {
    if (!Array.isArray(items)) {
      throw new TypeError('items must be an Array instance');
    }

    this._items = items;

    /**
     * Gets a Map representing the attributes of the response.
     * @type {Map}
     */
    this.attributes = attributes || emptyMap;

    /**
     * Gets the amount of items in the result.
     * @type {Number}
     */
    this.length = items.length;
  }

  /**
   * Gets the iterator associated with this instance.
   * @returns {Iterator}
   */
  [Symbol.iterator]() {
    return this._items[Symbol.iterator]();
  }

  /**
   * Provides a representation useful for debug and tracing.
   */
  [inspect]() {
    return this._items;
  }

  /**
   * Gets an array of result items.
   * @returns {Array}
   */
  toArray() {
    return this._items;
  }

  /**
   * Returns the first item.
   * @returns {Object|null}
   */
  first() {
    const item = this._items[0];
    return item !== undefined ? item : null;
  }
}

module.exports = ResultSet;