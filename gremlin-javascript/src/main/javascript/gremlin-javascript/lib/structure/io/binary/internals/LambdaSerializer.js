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

const { valueKey, LambdaSerializer: GraphsonLambdaSerializer } = require('../../type-serializers');

module.exports = class LambdaSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    // this.ioc.serializers[ioc.DataType.LAMBDA] = this; // it's not expected to be deserialized
    this.graphsonLambdaSerializer = new GraphsonLambdaSerializer();
  }

  canBeUsedFor(value) {
    return this.graphsonLambdaSerializer.canBeUsedFor(value);
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.LAMBDA, 0x01]);
      }
      const language = [0x00, 0x00, 0x00, 0x00]; // ''
      const script = [0x00, 0x00, 0x00, 0x00]; // ''
      const arguments_length = [0x00, 0x00, 0x00, 0x00]; // 0
      return Buffer.from([...language, ...script, ...arguments_length]);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.LAMBDA, 0x00]));
    }

    const graphson = this.graphsonLambdaSerializer.serialize(item);
    const language = graphson[valueKey].language;
    const script = graphson[valueKey].script;
    const arguments_ = graphson[valueKey]['arguments'];

    // {language}
    bufs.push(this.ioc.stringSerializer.serialize(language, false));

    // {script}
    bufs.push(this.ioc.stringSerializer.serialize(script, false));

    // {arguments_length}
    bufs.push(this.ioc.intSerializer.serialize(arguments_, false));

    return Buffer.concat(bufs);
  }
};
