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
 * GraphBinary 4.0 support implementation.
 *
 * The officially expected entrypoint is GraphBinaryReader/GraphBinaryWriter pair of classes,
 * examine lib/driver/** for use cases.
 *
 * See AnySerializer.serialize() for the mechanism of serializer selection for a given JavaScript value,
 * also consider AnySerializer.serialize() unit tests for real examples.
 * See NumberSerializationStrategy to understand how it deals with JavaScript numbers' serialization.
 *
 * TODO: it has the following open topics:
 * Core Data Types support:
 * - [] 0x22: BigDecimal
 * - [] 0x2b: Tree
 * Extended Types support:
 * - [] 0x80: Char
 * - [] 0x81: Duration
 *
 * @author Igor Ostapenko
 */
/*eslint-disable*/

import DataType from './internals/DataType.js';
import * as utils from './internals/utils.js';
import IntSerializer from './internals/IntSerializer.js';
import LongSerializer from './internals/LongSerializer.js';
import StringSerializer from './internals/StringSerializer.js';
import DateTimeSerializer from './internals/DateTimeSerializer.js';
import DoubleSerializer from './internals/DoubleSerializer.js';
import FloatSerializer from './internals/FloatSerializer.js';
import ArraySerializer from './internals/ArraySerializer.js';
import MapSerializer from './internals/MapSerializer.js';
import SetSerializer from './internals/SetSerializer.js';
import UuidSerializer from './internals/UuidSerializer.js';
import EdgeSerializer from './internals/EdgeSerializer.js';
import PathSerializer from './internals/PathSerializer.js';
import PropertySerializer from './internals/PropertySerializer.js';
import VertexSerializer from './internals/VertexSerializer.js';
import VertexPropertySerializer from './internals/VertexPropertySerializer.js';
import BigIntegerSerializer from './internals/BigIntegerSerializer.js';
import ByteSerializer from './internals/ByteSerializer.js';
import BinarySerializer from './internals/BinarySerializer.js';
import ShortSerializer from './internals/ShortSerializer.js';
import BooleanSerializer from './internals/BooleanSerializer.js';
import MarkerSerializer from './internals/MarkerSerializer.js';
import UnspecifiedNullSerializer from './internals/UnspecifiedNullSerializer.js';
import EnumSerializer from './internals/EnumSerializer.js';
import StubSerializer from './internals/StubSerializer.js';
import NumberSerializationStrategy from './internals/NumberSerializationStrategy.js';
import AnySerializer from './internals/AnySerializer.js';
import GraphBinaryReader from './internals/GraphBinaryReader.js';
import GraphBinaryWriter from './internals/GraphBinaryWriter.js';

const ioc = {};

ioc.DataType = DataType;
ioc.utils = utils;

ioc.serializers = {};

ioc.intSerializer = new IntSerializer(ioc);
ioc.longSerializer = new LongSerializer(ioc);
ioc.stringSerializer = new StringSerializer(ioc, ioc.DataType.STRING);
ioc.dateTimeSerializer = new DateTimeSerializer(ioc);
ioc.doubleSerializer = new DoubleSerializer(ioc);
ioc.floatSerializer = new FloatSerializer(ioc);
ioc.listSerializer = new ArraySerializer(ioc, ioc.DataType.LIST);
ioc.mapSerializer = new MapSerializer(ioc);
ioc.setSerializer = new SetSerializer(ioc, ioc.DataType.SET);
ioc.uuidSerializer = new UuidSerializer(ioc);
ioc.edgeSerializer = new EdgeSerializer(ioc);
ioc.pathSerializer = new PathSerializer(ioc);
ioc.propertySerializer = new PropertySerializer(ioc);
ioc.vertexSerializer = new VertexSerializer(ioc);
ioc.vertexPropertySerializer = new VertexPropertySerializer(ioc);
ioc.bigIntegerSerializer = new BigIntegerSerializer(ioc);
ioc.byteSerializer = new ByteSerializer(ioc);
ioc.binarySerializer = new BinarySerializer(ioc);
ioc.shortSerializer = new ShortSerializer(ioc);
ioc.booleanSerializer = new BooleanSerializer(ioc);
ioc.markerSerializer = new MarkerSerializer(ioc);
ioc.unspecifiedNullSerializer = new UnspecifiedNullSerializer(ioc);
ioc.enumSerializer = new EnumSerializer(ioc);

// Register stub serializers for unimplemented v4 types
new StubSerializer(ioc, ioc.DataType.TREE, 'Tree');
new StubSerializer(ioc, ioc.DataType.GRAPH, 'Graph');
new StubSerializer(ioc, ioc.DataType.COMPOSITEPDT, 'CompositePDT');
new StubSerializer(ioc, ioc.DataType.PRIMITIVEPDT, 'PrimitivePDT');

ioc.numberSerializationStrategy = new NumberSerializationStrategy(ioc);
ioc.anySerializer = new AnySerializer(ioc);

ioc.graphBinaryReader = new GraphBinaryReader(ioc);
ioc.graphBinaryWriter = new GraphBinaryWriter(ioc);

export { default as DataType } from './internals/DataType.js';

export const {
  serializers,
  intSerializer,
  longSerializer,
  stringSerializer,
  dateTimeSerializer,
  doubleSerializer,
  floatSerializer,
  listSerializer,
  mapSerializer,
  setSerializer,
  uuidSerializer,
  edgeSerializer,
  pathSerializer,
  propertySerializer,
  vertexSerializer,
  vertexPropertySerializer,
  bigIntegerSerializer,
  byteSerializer,
  binarySerializer,
  shortSerializer,
  booleanSerializer,
  markerSerializer,
  unspecifiedNullSerializer,
  enumSerializer,
  numberSerializationStrategy,
  anySerializer,
  graphBinaryReader,
  graphBinaryWriter,
} = ioc;

export default ioc;
