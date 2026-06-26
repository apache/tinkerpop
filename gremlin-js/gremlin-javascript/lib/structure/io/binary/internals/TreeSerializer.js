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
import { Tree } from '../../../graph.js';

/**
 * GraphBinary serializer for the Tree type (0x2b).
 *
 * Wire format (authoritative from Java TreeSerializer):
 *   value = {int length} then length x ( {fully-qualified key} {bare child Tree} )
 * A child Tree is written bare: just {int length}{entries...}, with no type-id
 * and no value-flag, recursively. A leaf is length 0. The child is never null,
 * but a key may be null.
 */
export default class TreeSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.TREE] = this;
  }

  canBeUsedFor(value) {
    return value instanceof Tree;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.TREE, 0x01]);
      }
      // bare null -> empty tree (length 0)
      return this.ioc.intSerializer.serialize(0, false);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.TREE, 0x00]));
    }
    bufs.push(this._serializeValue(item));
    return Buffer.concat(bufs);
  }

  /**
   * Serializes the bare value of a Tree: {int length} then, for each root key, a
   * fully-qualified key followed by a bare child Tree (recursive).
   *
   * Uses only the public read API ({@link Tree#rootNodes} / {@link Tree#childAt})
   * so it never touches the Tree's private backing. Keys returned by rootNodes()
   * are already deduped, so each is written exactly once.
   * @param {Tree} tree
   * @returns {Buffer}
   */
  _serializeValue(tree) {
    const keys = tree.rootNodes();
    let length = keys.length;
    if (length < 0) {
      length = 0;
    } else if (length > this.ioc.intSerializer.INT32_MAX) {
      length = this.ioc.intSerializer.INT32_MAX;
    }

    const bufs = [this.ioc.intSerializer.serialize(length, false)];
    for (let i = 0; i < length; i++) {
      const key = keys[i];
      // fully-qualified key
      bufs.push(this.ioc.anySerializer.serialize(key));
      // bare child tree (no type_code, no value_flag)
      bufs.push(this._serializeValue(tree.childAt(key)));
    }
    return Buffer.concat(bufs);
  }

  /**
   * Async deserialization of tree value bytes from a StreamReader.
   * @param {StreamReader} reader
   * @param {number} valueFlag
   * @param {number} typeCode
   * @returns {Promise<Tree>}
   */
  async deserializeValue(reader, valueFlag, typeCode) {
    return this._readTreeValue(reader);
  }

  /**
   * Reads a bare tree value: {int length} then length x ( fully-qualified key,
   * bare child tree ). The child recursion reads bare values (no type_code,
   * no value_flag).
   *
   * Duplicate sibling keys are collapsed via {@link Tree#getOrCreateChild} +
   * {@link Tree#addTree} (a server emitting
   * the same sibling key twice yields a single merged entry, not duplicates).
   * @param {StreamReader} reader
   * @returns {Promise<Tree>}
   */
  async _readTreeValue(reader) {
    const length = await this.ioc.intSerializer.deserializeBare(reader);

    const tree = new Tree();
    for (let i = 0; i < length; i++) {
      // {fully-qualified key} - read with type_code + value_flag.
      const key = await this.ioc.anySerializer.deserialize(reader);
      // {bare child tree} - never null, no type_code / value_flag
      const child = await this._readTreeValue(reader);
      // Merge into any existing entry for this key so duplicate sibling keys
      // collapse into a single entry.
      tree.getOrCreateChild(key).addTree(child);
    }
    return tree;
  }

  /**
   * Async fully-qualified deserialization from a StreamReader.
   * @param {StreamReader} reader
   * @returns {Promise<Tree|null>}
   */
  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (type_code !== this.ioc.DataType.TREE) {
      throw new Error(`TreeSerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00) {
      throw new Error(`TreeSerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag, type_code);
  }
}
