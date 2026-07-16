// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// GraphBinary v4 type codes.
enum DataType {
  custom(0x00),
  int_(0x01),
  long(0x02),
  string(0x03),
  dateTime(0x04),
  timestamp(0x05),
  clazz(0x06),
  double_(0x07),
  float_(0x08),
  list(0x09),
  map(0x0A),
  set_(0x0B),
  uuid(0x0C),
  edge(0x0D),
  path(0x0E),
  property(0x0F),
  graph(0x10),
  vertex(0x11),
  vertexProperty(0x12),
  direction(0x18),
  t(0x20),
  bigDecimal(0x22),
  bigInt(0x23),
  byte_(0x24),
  binary(0x25),
  short(0x26),
  boolean(0x27),
  bulkSet(0x2A),
  tree(0x2B),
  merge(0x2E),
  gType(0x30),
  char(0x80),
  duration(0x81),
  compositePdt(0xF0),
  primitivePdt(0xF1),
  marker(0xFD),
  unspecifiedNull(0xFE);

  final int code;
  const DataType(this.code);

  static DataType? fromCode(int code) {
    for (final dt in values) {
      if (dt.code == code) return dt;
    }
    return null;
  }
}
