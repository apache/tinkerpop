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

import 'dart:convert';
import 'dart:typed_data';

import 'package:uuid/uuid_value.dart';

import '../../../driver/request_message.dart';
import '../../../process/traversal.dart';
import '../../../structure/graph.dart';
import 'data_type.dart';

class GraphBinaryWriter {
  static const String _mimeType = 'application/vnd.graphbinary-v4.0';
  static const int _version = 0x84;

  String get mimeType => _mimeType;

  Uint8List writeRequest(RequestMessage message) {
    final fields = <String, dynamic>{};
    if (message.language.isNotEmpty) fields['language'] = message.language;
    if (message.g != null) fields['g'] = message.g;
    if (message.bindings != null) fields['bindings'] = message.bindings;
    if (message.timeoutMs != null) fields['timeoutMs'] = message.timeoutMs;
    if (message.materializeProperties != null) {
      fields['materializeProperties'] = message.materializeProperties;
    }
    if (message.bulkResults != null) fields['bulkResults'] = message.bulkResults;
    fields.addAll(message.fields);

    final writer = _GraphBinaryValueWriter();
    writer.addByte(_version);
    writer.writeMap(fields, fullyQualified: false);
    writer.writeString(message.gremlin, fullyQualified: false);
    return writer.takeBytes();
  }
}

class _GraphBinaryValueWriter {
  static const int _int32Min = -2147483648;
  static const int _int32Max = 2147483647;

  final BytesBuilder _builder = BytesBuilder(copy: false);

  void addByte(int value) => _builder.addByte(value & 0xff);

  Uint8List takeBytes() => _builder.takeBytes();

  void writeAny(dynamic value) {
    if (value == null) {
      addByte(DataType.unspecifiedNull.code);
      addByte(0x01);
    } else if (value is GInt) {
      writeInt32(value.value);
    } else if (value is GLong) {
      writeInt64(value.value);
    } else if (value is GFloat) {
      writeFloat(value.value);
    } else if (value is GDouble) {
      writeDouble(value.value);
    } else if (value is GShort) {
      writeShort(value.value);
    } else if (value is GByte) {
      writeByte(value.value);
    } else if (value is int) {
      if (value >= _int32Min && value <= _int32Max) {
        writeInt32(value);
      } else {
        writeInt64(value);
      }
    } else if (value is double) {
      writeDouble(value);
    } else if (value is bool) {
      writeBoolean(value);
    } else if (value is String) {
      writeString(value);
    } else if (value is UuidValue) {
      writeUuid(value);
    } else if (value is DateTime) {
      writeDateTime(value);
    } else if (value is Uint8List) {
      writeBinary(value);
    } else if (value is List) {
      writeList(value);
    } else if (value is Set) {
      writeSet(value);
    } else if (value is Map) {
      writeMap(value);
    } else if (value is VertexProperty) {
      writeVertexProperty(value);
    } else if (value is Vertex) {
      writeVertex(value);
    } else if (value is Edge) {
      writeEdge(value);
    } else if (value is Property) {
      writeProperty(value);
    } else if (value is Path) {
      writePath(value);
    } else if (value is EnumValue) {
      writeEnum(value);
    } else {
      throw ArgumentError('Unsupported GraphBinary value: ${value.runtimeType}');
    }
  }

  void writeInt32(int value, {bool fullyQualified = true}) {
    _writeHeader(DataType.int_, fullyQualified);
    _writeInt32Bare(value);
  }

  void writeInt64(int value, {bool fullyQualified = true}) {
    _writeHeader(DataType.long, fullyQualified);
    _writeInt64Bare(value);
  }

  void writeFloat(double value, {bool fullyQualified = true}) {
    _writeHeader(DataType.float_, fullyQualified);
    final data = ByteData(4)..setFloat32(0, value, Endian.big);
    _builder.add(data.buffer.asUint8List());
  }

  void writeDouble(double value, {bool fullyQualified = true}) {
    _writeHeader(DataType.double_, fullyQualified);
    final data = ByteData(8)..setFloat64(0, value, Endian.big);
    _builder.add(data.buffer.asUint8List());
  }

  void writeShort(int value, {bool fullyQualified = true}) {
    _writeHeader(DataType.short, fullyQualified);
    final data = ByteData(2)..setInt16(0, value, Endian.big);
    _builder.add(data.buffer.asUint8List());
  }

  void writeByte(int value, {bool fullyQualified = true}) {
    _writeHeader(DataType.byte_, fullyQualified);
    addByte(value);
  }

  void writeBoolean(bool value, {bool fullyQualified = true}) {
    _writeHeader(DataType.boolean, fullyQualified);
    addByte(value ? 0x01 : 0x00);
  }

  void writeString(String value, {bool fullyQualified = true}) {
    _writeHeader(DataType.string, fullyQualified);
    final bytes = utf8.encode(value);
    _writeInt32Bare(bytes.length);
    _builder.add(bytes);
  }

  void writeUuid(UuidValue value, {bool fullyQualified = true}) {
    _writeHeader(DataType.uuid, fullyQualified);
    _builder.add(value.toBytes(validate: true));
  }

  void writeDateTime(DateTime value, {bool fullyQualified = true}) {
    _writeHeader(DataType.dateTime, fullyQualified);
    final utc = value.toUtc();
    _writeInt32Bare(utc.year);
    addByte(utc.month);
    addByte(utc.day);
    final nanos = BigInt.from(utc.hour * 3600 + utc.minute * 60 + utc.second) *
            BigInt.from(1000000000) +
        BigInt.from(utc.millisecond * 1000000 + utc.microsecond * 1000);
    _writeInt64BareBig(nanos);
    _writeInt32Bare(0);
  }

  void writeBinary(Uint8List value, {bool fullyQualified = true}) {
    _writeHeader(DataType.binary, fullyQualified);
    _writeInt32Bare(value.length);
    _builder.add(value);
  }

  void writeList(List<dynamic> value, {bool fullyQualified = true}) {
    _writeHeader(DataType.list, fullyQualified);
    _writeInt32Bare(value.length);
    for (final item in value) {
      writeAny(item);
    }
  }

  void writeSet(Set<dynamic> value, {bool fullyQualified = true}) {
    _writeHeader(DataType.set_, fullyQualified);
    _writeInt32Bare(value.length);
    for (final item in value) {
      writeAny(item);
    }
  }

  void writeMap(Map<dynamic, dynamic> value, {bool fullyQualified = true}) {
    _writeHeader(DataType.map, fullyQualified);
    _writeInt32Bare(value.length);
    for (final entry in value.entries) {
      writeAny(entry.key);
      writeAny(entry.value);
    }
  }

  void writeVertex(Vertex value, {bool fullyQualified = true}) {
    _writeHeader(DataType.vertex, fullyQualified);
    writeAny(value.id);
    writeList([value.label], fullyQualified: false);
    writeList(value.properties, fullyQualified: true);
  }

  void writeEdge(Edge value, {bool fullyQualified = true}) {
    _writeHeader(DataType.edge, fullyQualified);
    writeAny(value.id);
    writeList([value.label], fullyQualified: false);
    writeAny(value.inV.id);
    writeList([value.inV.label], fullyQualified: false);
    writeAny(value.outV.id);
    writeList([value.outV.label], fullyQualified: false);
    writeAny(null);
    writeList(value.properties, fullyQualified: true);
  }

  void writeVertexProperty(VertexProperty value, {bool fullyQualified = true}) {
    _writeHeader(DataType.vertexProperty, fullyQualified);
    writeAny(value.id);
    writeList([value.label], fullyQualified: false);
    writeAny(value.value);
    writeAny(null);
    writeList(value.properties, fullyQualified: true);
  }

  void writeProperty(Property value, {bool fullyQualified = true}) {
    _writeHeader(DataType.property, fullyQualified);
    writeString(value.key, fullyQualified: false);
    writeAny(value.value);
    writeAny(null);
  }

  void writePath(Path value, {bool fullyQualified = true}) {
    _writeHeader(DataType.path, fullyQualified);
    writeList(value.labels.map((labels) => labels.toSet()).toList());
    writeList(value.objects);
  }

  void writeEnum(EnumValue value, {bool fullyQualified = true}) {
    final type = switch (value.typeName) {
      'Direction' => DataType.direction,
      'Merge' => DataType.merge,
      'T' => DataType.t,
      _ => throw ArgumentError('Unsupported GraphBinary enum: ${value.typeName}'),
    };
    _writeHeader(type, fullyQualified);
    writeString(value.elementName, fullyQualified: true);
  }

  void _writeHeader(DataType type, bool fullyQualified) {
    if (fullyQualified) {
      addByte(type.code);
      addByte(0x00);
    }
  }

  void _writeInt32Bare(int value) {
    final data = ByteData(4)..setInt32(0, value, Endian.big);
    _builder.add(data.buffer.asUint8List());
  }

  void _writeInt64Bare(int value) => _writeInt64BareBig(BigInt.from(value));

  void _writeInt64BareBig(BigInt value) {
    var unsigned = value;
    if (value.isNegative) {
      unsigned += BigInt.one << 64;
    }
    final bytes = Uint8List(8);
    for (var i = 7; i >= 0; i--) {
      bytes[i] = (unsigned & BigInt.from(0xff)).toInt();
      unsigned >>= 8;
    }
    _builder.add(bytes);
  }
}
