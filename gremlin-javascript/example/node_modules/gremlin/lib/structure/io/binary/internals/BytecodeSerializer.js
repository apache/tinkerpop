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
const t = require('../../../../process/traversal');

module.exports = class BytecodeSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.BYTECODE] = this;
  }

  canBeUsedFor(value) {
    return value instanceof Bytecode || value instanceof t.Traversal;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.BYTECODE, 0x01]);
      }
      const steps_length = [0x00, 0x00, 0x00, 0x00]; // 0
      const sources_length = [0x00, 0x00, 0x00, 0x00]; // 0
      return Buffer.from([...steps_length, ...sources_length]);
    }

    if (item instanceof t.Traversal) {
      item = item.getBytecode();
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.BYTECODE, 0x00]));
    }

    // {steps_length}{step_0}...{step_n}
    bufs.push(this.ioc.intSerializer.serialize(item.stepInstructions.length, false)); // TODO: what if steps_length > INT32_MAX
    for (let i = 0; i < item.stepInstructions.length; i++) {
      // {step_i} is composed of {name}{values_length}{value_0}...{value_n}
      const step = item.stepInstructions[i];
      const name = step[0];
      const values_length = step.length - 1;
      bufs.push(this.ioc.stringSerializer.serialize(name, false));
      bufs.push(this.ioc.intSerializer.serialize(values_length, false));
      for (let j = 0; j < values_length; j++) {
        bufs.push(this.ioc.anySerializer.serialize(step[1 + j], true));
      }
    }

    // {sources_length}{source_0}...{source_n}
    bufs.push(this.ioc.intSerializer.serialize(item.sourceInstructions.length, false)); // TODO: what if sources_length > INT32_MAX
    for (let i = 0; i < item.sourceInstructions.length; i++) {
      // {source_i} is composed of {name}{values_length}{value_0}...{value_n}
      const source = item.sourceInstructions[i];
      const name = source[0];
      const values_length = source.length - 1;
      bufs.push(this.ioc.stringSerializer.serialize(name, false));
      bufs.push(this.ioc.intSerializer.serialize(values_length, false));
      for (let j = 0; j < values_length; j++) {
        bufs.push(this.ioc.anySerializer.serialize(source[1 + j], true));
      }
    }

    return Buffer.concat(bufs);
  }

  deserialize(buffer, fullyQualifiedFormat = true) {
    let len = 0;
    let cursor = buffer;

    try {
      if (buffer === undefined || buffer === null || !(buffer instanceof Buffer)) {
        throw new Error('buffer is missing');
      }
      if (buffer.length < 1) {
        throw new Error('buffer is empty');
      }

      if (fullyQualifiedFormat) {
        const type_code = cursor.readUInt8();
        len++;
        if (type_code !== this.ioc.DataType.BYTECODE) {
          throw new Error('unexpected {type_code}');
        }
        cursor = cursor.slice(1);

        if (cursor.length < 1) {
          throw new Error('{value_flag} is missing');
        }
        const value_flag = cursor.readUInt8();
        len++;
        if (value_flag === 1) {
          return { v: null, len };
        }
        if (value_flag !== 0) {
          throw new Error('unexpected {value_flag}');
        }
        cursor = cursor.slice(1);
      }

      const v = new Bytecode();

      // steps

      let steps_length, steps_length_len;
      try {
        ({ v: steps_length, len: steps_length_len } = this.ioc.intSerializer.deserialize(cursor, false));
        len += steps_length_len;
      } catch (err) {
        err.message = '{steps_length}: ' + err.message;
        throw err;
      }
      if (steps_length < 0) {
        throw new Error('{steps_length} is less than zero');
      }
      cursor = cursor.slice(steps_length_len);

      // {step_i} is composed of {name}{values_length}{value_0}...{value_n}
      for (let i = 0; i < steps_length; i++) {
        // {name} is a String
        let name, name_len;
        try {
          ({ v: name, len: name_len } = this.ioc.stringSerializer.deserialize(cursor, false));
          len += name_len;
        } catch (err) {
          err.message = `{step_${i}} {name}: ` + err.message;
          throw err;
        }
        cursor = cursor.slice(name_len);

        // {values_length} is an Int describing the amount values
        let values_length, values_length_len;
        try {
          ({ v: values_length, len: values_length_len } = this.ioc.intSerializer.deserialize(cursor, false));
          len += values_length_len;
        } catch (err) {
          err.message = `{step_${i}} {values_length}: ` + err.message;
          throw err;
        }
        if (values_length < 0) {
          throw new Error(`{step_${i}} {values_length} is less than zero`);
        }
        cursor = cursor.slice(values_length_len);

        // {value_i} is a fully qualified typed value composed of {type_code}{type_info}{value_flag}{value} describing the step argument
        const values = [];
        let value, value_len;
        for (let j = 0; j < values_length; j++) {
          try {
            ({ v: value, len: value_len } = this.ioc.anySerializer.deserialize(cursor));
            len += value_len;
            values.push(value);
          } catch (err) {
            err.message = `{step_${i}} {value_${j}}: ` + err.message;
            throw err;
          }
          cursor = cursor.slice(value_len);
        }

        v.addStep(name, values);
      }

      // sources

      let sources_length, sources_length_len;
      try {
        ({ v: sources_length, len: sources_length_len } = this.ioc.intSerializer.deserialize(cursor, false));
        len += sources_length_len;
      } catch (err) {
        err.message = '{sources_length}: ' + err.message;
        throw err;
      }
      if (sources_length < 0) {
        throw new Error('{sources_length} is less than zero');
      }
      cursor = cursor.slice(sources_length_len);

      // {source_i} is composed of {name}{values_length}{value_0}...{value_n}
      for (let i = 0; i < sources_length; i++) {
        // {name} is a String
        let name, name_len;
        try {
          ({ v: name, len: name_len } = this.ioc.stringSerializer.deserialize(cursor, false));
          len += name_len;
        } catch (err) {
          err.message = `{source_${i}} {name}: ` + err.message;
          throw err;
        }
        cursor = cursor.slice(name_len);

        // {values_length} is an Int describing the amount values
        let values_length, values_length_len;
        try {
          ({ v: values_length, len: values_length_len } = this.ioc.intSerializer.deserialize(cursor, false));
          len += values_length_len;
        } catch (err) {
          err.message = `{source_${i}} {values_length}: ` + err.message;
          throw err;
        }
        if (values_length < 0) {
          throw new Error(`{source_${i}} {values_length} is less than zero`);
        }
        cursor = cursor.slice(values_length_len);

        // {value_i} is a fully qualified typed value composed of {type_code}{type_info}{value_flag}{value}
        const values = [];
        let value, value_len;
        for (let j = 0; j < values_length; j++) {
          try {
            ({ v: value, len: value_len } = this.ioc.anySerializer.deserialize(cursor));
            len += value_len;
            values.push(value);
          } catch (err) {
            err.message = `{source_${i}} {value_${j}}: ` + err.message;
            throw err;
          }
          cursor = cursor.slice(value_len);
        }

        v.addSource(name, values);
      }

      return { v, len };
    } catch (err) {
      throw this.ioc.utils.des_error({ serializer: this, args: arguments, cursor, err });
    }
  }
};
