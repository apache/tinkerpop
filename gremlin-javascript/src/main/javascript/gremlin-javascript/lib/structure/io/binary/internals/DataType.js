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

/**
 * Represents a GraphBinary data type.
 *
 * See org.apache.tinkerpop.gremlin.structure.io.binary.DataType Java class.
 */
const DataType = {
  INT: 0x01,
  LONG: 0x02,
  STRING: 0x03,
  DATETIME: 0x04,
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
  DIRECTION: 0x18,
  T: 0x20,
  BIGDECIMAL: 0x22,
  BIGINTEGER: 0x23,
  BYTE: 0x24,
  BINARY: 0x25,
  SHORT: 0x26,
  BOOLEAN: 0x27,
  TREE: 0x2b,
  MERGE: 0x2e,

  CHAR: 0x80,
  DURATION: 0x81,

  COMPOSITEPDT: 0xf0,
  PRIMITIVEPDT: 0xf1,

  MARKER: 0xfd,
  UNSPECIFIED_NULL: 0xfe,
};

export default DataType;
