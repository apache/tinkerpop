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

import { Buffer } from 'buffer';
import { PrimitiveProviderDefinedType } from '../../../graph.js';

export default class PrimitivePDTSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.PRIMITIVEPDT] = this;
  }

  canBeUsedFor(value) {
    return value instanceof PrimitiveProviderDefinedType;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.PRIMITIVEPDT, 0x01]);
      }
      const bufs = [];
      bufs.push(this.ioc.stringSerializer.serialize('', false));
      bufs.push(this.ioc.stringSerializer.serialize('', false));
      return Buffer.concat(bufs);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.PRIMITIVEPDT, 0x00]));
    }
    bufs.push(this.ioc.stringSerializer.serialize(item.name, true));
    bufs.push(this.ioc.stringSerializer.serialize(item.value, true));
    return Buffer.concat(bufs);
  }

  async deserializeValue(reader, valueFlag, typeCode) {
    const name = await this.ioc.anySerializer.deserialize(reader);
    if (!name) {
      throw new Error('PrimitivePDTSerializer: name cannot be null or empty');
    }
    const value = await this.ioc.anySerializer.deserialize(reader);
    const pdt = new PrimitiveProviderDefinedType(name, value != null ? String(value) : '');
    const pdtRegistry = reader.pdtRegistry;
    if (pdtRegistry) {
      const hydrated = pdtRegistry.hydratePrimitive(pdt);
      if (!(hydrated instanceof PrimitiveProviderDefinedType)) {
        return hydrated;
      }
    }
    return pdt;
  }

  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (type_code !== this.ioc.DataType.PRIMITIVEPDT) {
      throw new Error(`PrimitivePDTSerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00) {
      throw new Error(`PrimitivePDTSerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag, type_code);
  }
}
