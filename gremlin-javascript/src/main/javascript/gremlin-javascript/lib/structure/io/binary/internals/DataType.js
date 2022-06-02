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

/**
 * Represents a GraphBinary data type.
 *
 * See org.apache.tinkerpop.gremlin.structure.io.binary.DataType Java class.
 */
const DataType = {
  INT: 0x01,
  LONG: 0x02,
  STRING: 0x03,
  DATE: 0x04,
  TIMESTAMP: 0x05,
  CLASS: 0x06,
  DOUBLE: 0x07,
  FLOAT: 0x08,
  LIST: 0x09,
  MAP: 0x0a,
  SET: 0x0b,
  UUID: 0x0c,
  EDGE: 0x0d,
  PATH: 0x0e,
  PROPERTY: 0x0f,
  GRAPH: 0x10,
  VERTEX: 0x11,
  VERTEXPROPERTY: 0x12,
  BARRIER: 0x13,
  BINDING: 0x14,
  BYTECODE: 0x15,
  CARDINALITY: 0x16,
  COLUMN: 0x17,
  DIRECTION: 0x18,
  OPERATOR: 0x19,
  ORDER: 0x1a,
  PICK: 0x1b,
  POP: 0x1c,
  LAMBDA: 0x1d,
  P: 0x1e,
  SCOPE: 0x1f,
  T: 0x20,
  TRAVERSER: 0x21,
  BIGDECIMAL: 0x22,
  BIGINTEGER: 0x23,
  BYTE: 0x24,
  BYTEBUFFER: 0x25,
  SHORT: 0x26,
  BOOLEAN: 0x27,
  TEXTP: 0x28,
  TRAVERSALSTRATEGY: 0x29,
  BULKSET: 0x2a,
  TREE: 0x2b,
  METRICS: 0x2c,
  TRAVERSALMETRICS: 0x2d,
  MERGE: 0x2e,

  CHAR: 0x80,
  DURATION: 0x81,
  INETADDRESS: 0x82,
  INSTANT: 0x83,
  LOCALDATE: 0x84,
  LOCALDATETIME: 0x85,
  LOCALTIME: 0x86,
  MONTHDAY: 0x87,
  OFFSETDATETIME: 0x88,
  OFFSETTIME: 0x89,
  PERIOD: 0x8a,
  YEAR: 0x8b,
  YEARMONTH: 0x8c,
  ZONEDATETIME: 0x8d,
  ZONEOFFSET: 0x8e,

  CUSTOM: 0,
  UNSPECIFIED_NULL: 0xfe,
};

module.exports = DataType;
