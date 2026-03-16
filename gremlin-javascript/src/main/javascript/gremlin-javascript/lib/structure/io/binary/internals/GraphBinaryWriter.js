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

import { Buffer } from 'buffer';

/**
 * GraphBinary writer.
 */
export default class GraphBinaryWriter {
  constructor(ioc) {
    this.ioc = ioc;
  }

  writeRequest(requestMessage) {
    const fields = new Map();

    // Extract fields from RequestMessage if it has getter methods
    if (typeof requestMessage.getLanguage === 'function') {
      const language = requestMessage.getLanguage();
      if (language) {
        fields.set('language', language);
      }
      const g = requestMessage.getG();
      if (g) {
        fields.set('g', g);
      }
      const bindings = requestMessage.getBindings();
      if (bindings && Object.keys(bindings).length > 0) {
        fields.set('bindings', bindings);
      }
      const timeoutMs = requestMessage.getTimeoutMs();
      if (timeoutMs !== undefined) {
        fields.set('timeoutMs', timeoutMs);
      }
      const materializeProperties = requestMessage.getMaterializeProperties();
      if (materializeProperties) {
        fields.set('materializeProperties', materializeProperties);
      }
      const bulkResults = requestMessage.getBulkResults();
      if (bulkResults !== undefined) {
        fields.set('bulkResults', bulkResults);
      }

      // Add any custom fields
      const customFields = requestMessage.getFields();
      if (customFields) {
        customFields.forEach((v, k) => fields.set(k, v));
      }

      const gremlin = requestMessage.getGremlin();
      const bufs = [
        Buffer.from([0x81]),
        this.ioc.mapSerializer.serialize(fields, false),
        this.ioc.stringSerializer.serialize(gremlin, false),
      ];
      return Buffer.concat(bufs);
    }

    // Legacy path: plain object with { gremlin, fields }
    const bufs = [
      Buffer.from([0x81]),
      this.ioc.mapSerializer.serialize(requestMessage.fields || new Map(), false),
      this.ioc.stringSerializer.serialize(requestMessage.gremlin, false),
    ];
    return Buffer.concat(bufs);
  }
}
