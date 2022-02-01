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

const t = require('../../../../process/traversal');

module.exports = class EnumSerializer {

  constructor(ioc) {
    this.ioc = ioc;

    const DT = ioc.DataType;
    this.types = [
      { name: 'Barrier',     code: DT.BARRIER,     enum: t.barrier },
      { name: 'Cardinality', code: DT.CARDINALITY, enum: t.cardinality },
      { name: 'Column',      code: DT.COLUMN,      enum: t.column },
      { name: 'Direction',   code: DT.DIRECTION,   enum: t.direction },
      { name: 'Operator',    code: DT.OPERATOR,    enum: t.operator },
      { name: 'Order',       code: DT.ORDER,       enum: t.order },
      { name: 'Pick',        code: DT.PICK,        enum: t.pick },
      { name: 'Pop',         code: DT.POP,         enum: t.pop },
      { name: 'Scope',       code: DT.SCOPE,       enum: t.scope },
      { name: 'T',           code: DT.T,           enum: t.t },
    ];
    this.byname = {};
    this.bycode = {};
    for (const type of this.types) {
      this.ioc.serializers[type.code] = this;
      this.byname[type.name] = type;
      this.bycode[type.code] = type;
    }
  }

  canBeUsedFor(value) {
    if (!(value instanceof t.EnumValue))
      return false;
    if (! this.byname[value.typeName])
      throw new Error(`EnumSerializer.serialize: typeName=${value.typeName} is not supported.`);

    return true;
  }

  serialize(item, fullyQualifiedFormat=true) {
    const type = this.byname[item.typeName];
    if (item.elementName === undefined || item.elementName === null)
      if (fullyQualifiedFormat)
        return Buffer.from([type.code, 0x01]);
      else
        return Buffer.from([0x00,0x00,0x00,0x00]);

    const bufs = [];
    if (fullyQualifiedFormat)
      bufs.push( Buffer.from([type.code, 0x00]) );

    bufs.push( this.ioc.stringSerializer.serialize(item.elementName, false) );

    return Buffer.concat(bufs);
  }

  deserialize(buffer, fullyQualifiedFormat=true) {
    let len = 0;
    let cursor = buffer;

    try {
      if (buffer === undefined || buffer === null || !(buffer instanceof Buffer))
        throw new Error('buffer is missing');
      if (buffer.length < 1)
        throw new Error('buffer is empty');

      let type = undefined;
      if (fullyQualifiedFormat) {
        const type_code = cursor.readUInt8(); len++; cursor = cursor.slice(1);
        type = this.bycode[type_code];
        if (!type)
          throw new Error(`unexpected {type_code}=${type_code}`);

        if (cursor.length < 1)
          throw new Error('{value_flag} is missing');
        const value_flag = cursor.readUInt8(); len++; cursor = cursor.slice(1);
        if (value_flag === 1)
          return { v: new t.EnumValue(type.name, null), len };
        if (value_flag !== 0)
          throw new Error('unexpected {value_flag}');
      }

      let elementName, elementName_len;
      try {
        ({ v: elementName, len: elementName_len } = this.ioc.stringSerializer.deserialize(cursor, false));
        len += elementName_len; cursor = cursor.slice(elementName_len);
      } catch (e) {
        throw new Error(`elementName: ${e.message}`);
      }

      const v = new t.EnumValue(type ? type.name : undefined, elementName); // TODO: should we use type.enum in order to point to an existing instance? beware, it has non-obvious case sensitivity logic for keys
      return { v, len };
    }
    catch (e) {
      throw this.ioc.utils.des_error({ serializer: this, args: arguments, cursor, msg: e.message });
    }
  }

};
