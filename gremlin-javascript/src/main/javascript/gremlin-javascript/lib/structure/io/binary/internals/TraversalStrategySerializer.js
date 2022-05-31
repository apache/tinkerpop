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
 * @author Igor Ostapenko
 */
'use strict';

const { TraversalStrategySerializer: GraphsonTraversalStrategySerializer } = require('../../type-serializers');

module.exports = class TraversalStrategySerializer {
  constructor(ioc) {
    this.ioc = ioc;
    // this.ioc.serializers[ioc.DataType.TRAVERSALSTRATEGY] = this; // TODO: it's not expected to be deserialized
    this.graphsonTraversalStrategySerializer = new GraphsonTraversalStrategySerializer();
  }

  canBeUsedFor(value) {
    return this.graphsonTraversalStrategySerializer.canBeUsedFor(value);
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.TRAVERSALSTRATEGY, 0x01]);
      }
      const strategy_class = [0x00, 0x00, 0x00, 0x00]; // ''
      const configuration = [0x00, 0x00, 0x00, 0x00]; // {}
      return Buffer.from([...strategy_class, ...configuration]);
      // TODO: should we delegate these to respective serializers like classSerializer.serialize(undefined, false) ?
      // Consider to make it the same way for all other serializers
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.TRAVERSALSTRATEGY, 0x00]));
    }

    // let's repeat the logic of Graphson's TraversalStrategySerializer:
    const strategy_class = item.fqcn;
    const conf = {};
    for (const k in item.configuration) {
      if (item.configuration.hasOwnProperty(k)) {
        conf[k] = item.configuration[k];
      }
    }

    // {strategy_class}
    bufs.push(this.ioc.classSerializer.serialize(strategy_class, false));

    // {configuration}
    bufs.push(this.ioc.mapSerializer.serialize(conf, false));

    return Buffer.concat(bufs);
  }
};
