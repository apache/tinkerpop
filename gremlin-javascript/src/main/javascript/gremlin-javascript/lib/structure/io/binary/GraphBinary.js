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
 * GraphBinary 1.0 support implementation.
 *
 * The officially expected entrypoint is GraphBinaryReader/GraphBinaryWriter pair of classes,
 * examine lib/driver/** for use cases.
 *
 * See AnySerializer.serialize() for the mechanism of serializer selection for a given JavaScript value,
 * also consider AnySerializer.serialize() unit tests for real examples.
 * See NumberSerializationStrategy to understand how it deals with JavaScript numbers' serialization.
 *
 * Consider AnySerializer.serialize()/deserialize() unit tests to see what is not implemented,
 * what is ignored, what is not expected to be (de)serialized, etc.
 *
 * TODO: it has the following open topics:
 * - [] Should we do anything for application/vnd.graphbinary-v1.0-stringd mime type support?
 * Core Data Types support:
 * - [] 0x22: BigDecimal
 * - [] 0x2b: Tree
 * - [] 0x2c: Metrics
 * - [] 0x2d: TraversalMetrics
 * - [] 0x00: Custom
 * Extended Types support:
 * - [] 0x80: Char
 * - [] 0x81: Duration
 * - [] 0x82: InetAddress
 * - [] 0x83: Instant
 * - [] 0x84: LocalDate
 * - [] 0x85: LocalDateTime
 * - [] 0x86: LocalTime
 * - [] 0x87: MonthDay
 * - [] 0x88: OffsetDateTime
 * - [] 0x89: OffsetTime
 * - [] 0x8a: Period
 * - [] 0x8b: Year
 * - [] 0x8c: YearMonth
 * - [] 0x8d: ZonedDateTime
 * - [] 0x8e: ZoneOffset
 *
 * @author Igor Ostapenko
 */
/*eslint-disable*/
'use strict';

const ioc = {};

ioc.DataType = require('./internals/DataType');
ioc.utils = require('./internals/utils');

ioc.serializers = {};

ioc.intSerializer               = new (require('./internals/IntSerializer'))(ioc);
ioc.longSerializer              = new (require('./internals/LongSerializer'))(ioc);
ioc.longSerializerNg            = new (require('./internals/LongSerializerNg'))(ioc);
ioc.stringSerializer            = new (require('./internals/StringSerializer'))(ioc, ioc.DataType.STRING);
ioc.dateSerializer              = new (require('./internals/DateSerializer'))(ioc, ioc.DataType.DATE);
ioc.timestampSerializer         = new (require('./internals/DateSerializer'))(ioc, ioc.DataType.TIMESTAMP);
ioc.classSerializer             = new (require('./internals/StringSerializer'))(ioc, ioc.DataType.CLASS);
ioc.doubleSerializer            = new (require('./internals/DoubleSerializer'))(ioc);
ioc.floatSerializer             = new (require('./internals/FloatSerializer'))(ioc);
ioc.listSerializer              = new (require('./internals/ArraySerializer'))(ioc, ioc.DataType.LIST);
ioc.mapSerializer               = new (require('./internals/MapSerializer'))(ioc);
ioc.setSerializer               = new (require('./internals/ArraySerializer'))(ioc, ioc.DataType.SET);
ioc.uuidSerializer              = new (require('./internals/UuidSerializer'))(ioc);
ioc.edgeSerializer              = new (require('./internals/EdgeSerializer'))(ioc);
ioc.pathSerializer              = new (require('./internals/PathSerializer'))(ioc);
ioc.propertySerializer          = new (require('./internals/PropertySerializer'))(ioc);
ioc.vertexSerializer            = new (require('./internals/VertexSerializer'))(ioc);
ioc.vertexPropertySerializer    = new (require('./internals/VertexPropertySerializer'))(ioc);
ioc.bytecodeSerializer          = new (require('./internals/BytecodeSerializer'))(ioc);
ioc.pSerializer                 = new (require('./internals/PSerializer'))(ioc);
ioc.traverserSerializer         = new (require('./internals/TraverserSerializer'))(ioc);
ioc.enumSerializer              = new (require('./internals/EnumSerializer'))(ioc);
ioc.lambdaSerializer            = new (require('./internals/LambdaSerializer'))(ioc);
ioc.bigIntegerSerializer        = new (require('./internals/BigIntegerSerializer'))(ioc);
ioc.byteSerializer              = new (require('./internals/ByteSerializer'))(ioc);
ioc.byteBufferSerializer        = new (require('./internals/ByteBufferSerializer'))(ioc);
ioc.shortSerializer             = new (require('./internals/ShortSerializer'))(ioc);
ioc.booleanSerializer           = new (require('./internals/BooleanSerializer'))(ioc);
ioc.textPSerializer             = new (require('./internals/TextPSerializer'))(ioc);
ioc.traversalStrategySerializer = new (require('./internals/TraversalStrategySerializer'))(ioc);
ioc.bulkSetSerializer           = new (require('./internals/BulkSetSerializer'))(ioc);
ioc.unspecifiedNullSerializer   = new (require('./internals/UnspecifiedNullSerializer'))(ioc);

ioc.numberSerializationStrategy = new (require('./internals/NumberSerializationStrategy'))(ioc);
ioc.anySerializer               = new (require('./internals/AnySerializer'))(ioc);

ioc.graphBinaryReader           = new (require('./internals/GraphBinaryReader.js'))(ioc);
ioc.graphBinaryWriter           = new (require('./internals/GraphBinaryWriter'))(ioc);

module.exports = ioc;
