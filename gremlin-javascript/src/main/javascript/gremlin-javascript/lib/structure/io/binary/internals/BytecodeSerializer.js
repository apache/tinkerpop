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

const Bytecode = require('../../../../process/bytecode');

module.exports = class {

  constructor(ioc) {
    this.ioc = ioc;
  }

  canBeUsedFor(value) {
    return (value instanceof Bytecode);
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null)
      if (fullyQualifiedFormat)
        return Buffer.from([this.ioc.DataType.BYTECODE, 0x01]);
      else
        return Buffer.from([0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00]); // {steps_length} = 0, {sources_length} = 0

    const bufs = [];
    if (fullyQualifiedFormat)
      bufs.push( Buffer.from([this.ioc.DataType.BYTECODE, 0x00]) );

    // {steps_length}{step_0}...{step_n}
    bufs.push( this.ioc.intSerializer.serialize(item.stepInstructions.length, false) ); // TODO: what if steps_length > INT32_MAX
    for (let i = 0; i < item.stepInstructions.length; i++) {
      // {step_i} is composed of {name}{values_length}{value_0}...{value_n}
      const step = item.stepInstructions[i];
      const name = step[0];
      const values_length = step.length - 1;
      bufs.push( this.ioc.stringSerializer.serialize(name, false) );
      bufs.push( this.ioc.intSerializer.serialize(values_length, false) );
      for (let j = 0; j < values_length; j++)
        bufs.push( this.ioc.anySerializer.serialize(step[1 + j], true) );
    }

    // {sources_length}{source_0}...{source_n}
    bufs.push( this.ioc.intSerializer.serialize(item.sourceInstructions.length, false) ); // TODO: what if sources_length > INT32_MAX
    for (let i = 0; i < item.sourceInstructions.length; i++) {
      // {source_i} is composed of {name}{values_length}{value_0}...{value_n}
      const source = item.sourceInstructions[i];
      const name = source[0];
      const values_length = source.length - 1;
      bufs.push( this.ioc.stringSerializer.serialize(name, false) );
      bufs.push( this.ioc.intSerializer.serialize(values_length, false) );
      for (let j = 0; j < values_length; j++)
        bufs.push( this.ioc.anySerializer(source[1 + j], true) );
    }

    return Buffer.concat(bufs);
  }

  deserialize(buffer) {
    // TODO
  }

}
