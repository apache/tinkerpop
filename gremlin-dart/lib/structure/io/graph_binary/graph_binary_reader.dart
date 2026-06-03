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

import '../../../process/traversal.dart';
import '../../../structure/graph.dart';
import 'data_type.dart';

class GraphBinaryReader {
  static const String _mimeType = 'application/vnd.graphbinary-v4.0';
  static const int _version = 0x84;
  static const Object _endOfStream = Object();

  String get mimeType => _mimeType;

  Future<Map<String, dynamic>> readResponse(Uint8List bytes) async {
    if (bytes.isEmpty) {
      throw ArgumentError('GraphBinary response is empty.');
    }

    final reader = _GraphBinaryValueReader(bytes);
    final version = reader.readUint8();
    if (version != _version) {
      throw FormatException('Unsupported GraphBinary version: 0x${version.toRadixString(16)}');
    }

    final bulked = reader.readUint8() == 0x01;
    final data = <dynamic>[];
    while (true) {
      final value = reader.readAny();
      if (identical(value, _endOfStream)) break;

      if (bulked) {
        // bulk count is a fully-typed value (type + flag + int64), not raw bytes
        final bulk = reader.readAny();
        data.add({'v': value, 'bulk': bulk is int ? bulk : (bulk as num).toInt()});
      } else {
        data.add(value);
      }
    }

    final status = _readStatus(reader);
    return {
      'status': status,
      'result': {'data': data, 'bulked': bulked},
    };
  }

  Stream<dynamic> readResponseStream(Stream<Uint8List> source) async* {
    final chunks = <int>[];
    await for (final chunk in source) {
      chunks.addAll(chunk);
    }
    final response = await readResponse(Uint8List.fromList(chunks));
    final result = response['result'] as Map<String, dynamic>;
    final data = result['data'] as List;
    if (result['bulked'] == true) {
      for (final item in data.cast<Map>()) {
        yield Traverser(item['v'], item['bulk'] as int? ?? 1);
      }
    } else {
      for (final item in data) {
        yield item;
      }
    }
  }

  Map<String, dynamic> _readStatus(_GraphBinaryValueReader reader) {
    final code = reader.readInt32();
    final message = reader.readNullableBareString();
    final exception = reader.readNullableBareString();
    return {'code': code, 'message': message, 'exception': exception};
  }
}

class _GraphBinaryValueReader {
  static const int _bulkFlag = 0x02;

  final Uint8List _bytes;
  final ByteData _data;
  int _offset = 0;

  _GraphBinaryValueReader(this._bytes)
      : _data = ByteData.sublistView(_bytes);

  int readUint8() {
    _require(1);
    return _bytes[_offset++];
  }

  int readInt8() {
    _require(1);
    return _data.getInt8(_offset++);
  }

  int readInt16() {
    _require(2);
    final value = _data.getInt16(_offset, Endian.big);
    _offset += 2;
    return value;
  }

  int readInt32() {
    _require(4);
    final value = _data.getInt32(_offset, Endian.big);
    _offset += 4;
    return value;
  }

  int readInt64() {
    final value = readBigInt64();
    return value.toInt();
  }

  BigInt readBigInt64() {
    _require(8);
    BigInt value = BigInt.zero;
    for (var i = 0; i < 8; i++) {
      value = (value << 8) | BigInt.from(_bytes[_offset + i]);
    }
    _offset += 8;
    if ((_bytes[_offset - 8] & 0x80) != 0) {
      value -= BigInt.one << 64;
    }
    return value;
  }

  double readFloat32() {
    _require(4);
    final value = _data.getFloat32(_offset, Endian.big);
    _offset += 4;
    return value;
  }

  double readFloat64() {
    _require(8);
    final value = _data.getFloat64(_offset, Endian.big);
    _offset += 8;
    return value;
  }

  Uint8List readBytes(int length) {
    if (length < 0) {
      throw FormatException('Negative byte length: $length');
    }
    _require(length);
    final value = Uint8List.sublistView(_bytes, _offset, _offset + length);
    _offset += length;
    return value;
  }

  dynamic readAny() {
    final position = _offset;
    final typeCode = readUint8();
    final type = DataType.fromCode(typeCode);
    if (type == null) {
      throw FormatException('Unknown GraphBinary type 0x${typeCode.toRadixString(16)} at $position');
    }

    final valueFlag = readUint8();
    if (valueFlag == 0x01) return null;
    if (valueFlag != 0x00 && valueFlag != _bulkFlag) {
      throw FormatException('Unexpected value flag 0x${valueFlag.toRadixString(16)} at $position');
    }

    return _readValue(type, valueFlag);
  }

  String? readNullableBareString() {
    final flag = readUint8();
    if (flag == 0x01) return null;
    if (flag != 0x00) {
      throw FormatException('Unexpected nullable string flag 0x${flag.toRadixString(16)}');
    }
    return _readString();
  }

  dynamic _readValue(DataType type, int valueFlag) {
    switch (type) {
      case DataType.int_:
        return readInt32();
      case DataType.long:
        return readInt64();
      case DataType.float_:
        return readFloat32();
      case DataType.double_:
        return readFloat64();
      case DataType.string:
        return _readString();
      case DataType.boolean:
        return _readBoolean();
      case DataType.list:
        return _readList(valueFlag == _bulkFlag);
      case DataType.set_:
        return _readSet(valueFlag == _bulkFlag);
      case DataType.map:
        return _readMap();
      case DataType.uuid:
        return _readUuid();
      case DataType.vertex:
        return _readVertex();
      case DataType.edge:
        return _readEdge();
      case DataType.vertexProperty:
        return _readVertexProperty();
      case DataType.property:
        return _readProperty();
      case DataType.path:
        return _readPath();
      case DataType.bulkSet:
        return _readBulkSet();
      case DataType.dateTime:
        return _readDateTime();
      case DataType.binary:
        return readBytes(readInt32());
      case DataType.byte_:
        return readInt8();
      case DataType.short:
        return readInt16();
      case DataType.bigInt:
        return _readBigInt();
      case DataType.bigDecimal:
        return _readBigDecimal();
      case DataType.duration:
        return _readDuration();
      case DataType.marker:
        final marker = readUint8();
        if (marker != 0x00) {
          throw FormatException('Unexpected GraphBinary marker value: $marker');
        }
        return GraphBinaryReader._endOfStream;
      case DataType.t:
      case DataType.direction:
      case DataType.merge:
      case DataType.gType:
        return _readEnum(type);
      default:
        throw FormatException('Unsupported GraphBinary type: $type');
    }
  }

  String _readString() {
    final length = readInt32();
    final bytes = readBytes(length);
    return utf8.decode(bytes);
  }

  bool _readBoolean() {
    final value = readUint8();
    if (value != 0x00 && value != 0x01) {
      throw FormatException('Unexpected boolean value: $value');
    }
    return value == 0x01;
  }

  List<dynamic> _readList(bool bulked) {
    final length = readInt32();
    if (length < 0) throw FormatException('Negative list length: $length');
    final values = <dynamic>[];
    for (var i = 0; i < length; i++) {
      final value = readAny();
      if (bulked) {
        final bulk = readInt64();
        for (var j = 0; j < bulk; j++) {
          values.add(value);
        }
      } else {
        values.add(value);
      }
    }
    return values;
  }

  Set<dynamic> _readSet(bool bulked) {
    final length = readInt32();
    if (length < 0) throw FormatException('Negative set length: $length');
    final values = <dynamic>{};
    for (var i = 0; i < length; i++) {
      final value = readAny();
      if (bulked) readInt64();
      values.add(value);
    }
    return values;
  }

  Map<dynamic, dynamic> _readMap() {
    final length = readInt32();
    if (length < 0) throw FormatException('Negative map length: $length');
    final values = <dynamic, dynamic>{};
    for (var i = 0; i < length; i++) {
      final key = readAny();
      values[key] = readAny();
    }
    return values;
  }

  List<Traverser<dynamic>> _readBulkSet() {
    final length = readInt32();
    if (length < 0) throw FormatException('Negative bulk set length: $length');
    final values = <Traverser<dynamic>>[];
    for (var i = 0; i < length; i++) {
      values.add(Traverser(readAny(), readInt64()));
    }
    return values;
  }

  String _readUuid() {
    final bytes = readBytes(16);
    String hex(int start, int end) =>
        bytes.sublist(start, end).map((b) => b.toRadixString(16).padLeft(2, '0')).join();
    return '${hex(0, 4)}-${hex(4, 6)}-${hex(6, 8)}-${hex(8, 10)}-${hex(10, 16)}';
  }

  Vertex _readVertex() {
    final id = readAny();
    final label = _firstLabel(_readList(false));
    final properties = _asProperties(readAny());
    return Vertex(id, label, properties);
  }

  Edge _readEdge() {
    final id = readAny();
    final label = _firstLabel(_readList(false));
    final inVId = readAny();
    final inVLabel = _firstLabel(_readList(false));
    final outVId = readAny();
    final outVLabel = _firstLabel(_readList(false));
    readAny();
    final properties = _asProperties(readAny());
    return Edge(id, Vertex(outVId, outVLabel), label, Vertex(inVId, inVLabel), properties);
  }

  VertexProperty _readVertexProperty() {
    final id = readAny();
    final label = _firstLabel(_readList(false));
    final value = readAny();
    readAny();
    final properties = _asProperties(readAny());
    return VertexProperty(id, label, value, properties);
  }

  Property _readProperty() {
    final key = _readString();
    final value = readAny();
    readAny();
    return Property(key, value);
  }

  Path _readPath() {
    final labelsValue = readAny();
    final objectsValue = readAny();
    final labels = <List<String>>[];
    if (labelsValue is List) {
      for (final item in labelsValue) {
        if (item is Set) {
          labels.add(item.map((label) => label.toString()).toList());
        } else if (item is List) {
          labels.add(item.map((label) => label.toString()).toList());
        } else {
          labels.add([item.toString()]);
        }
      }
    }
    final objects = objectsValue is List ? objectsValue : <dynamic>[];
    return Path(labels, objects);
  }

  DateTime _readDateTime() {
    final year = readInt32();
    final month = readUint8();
    final day = readUint8();
    final nanos = readBigInt64();
    final offsetSeconds = readInt32();

    final hour = (nanos ~/ BigInt.from(3600000000000)).toInt();
    var remaining = nanos.remainder(BigInt.from(3600000000000));
    final minute = (remaining ~/ BigInt.from(60000000000)).toInt();
    remaining = remaining.remainder(BigInt.from(60000000000));
    final second = (remaining ~/ BigInt.from(1000000000)).toInt();
    remaining = remaining.remainder(BigInt.from(1000000000));
    final millisecond = (remaining ~/ BigInt.from(1000000)).toInt();
    remaining = remaining.remainder(BigInt.from(1000000));
    final microsecond = (remaining ~/ BigInt.from(1000)).toInt();

    return DateTime.utc(year, month, day, hour, minute, second, millisecond, microsecond)
        .subtract(Duration(seconds: offsetSeconds));
  }

  BigInt _readBigInt() {
    final bytes = readBytes(readInt32());
    if (bytes.isEmpty) return BigInt.zero;
    BigInt value = BigInt.zero;
    for (final byte in bytes) {
      value = (value << 8) | BigInt.from(byte);
    }
    if ((bytes.first & 0x80) != 0) {
      value -= BigInt.one << (bytes.length * 8);
    }
    return value;
  }

  double _readBigDecimal() {
    final scale = readInt32();
    final unscaled = _readBigInt();
    return unscaled.toDouble() / _pow10(scale);
  }

  Duration _readDuration() {
    final seconds = readInt64();
    final nanos = readInt32();
    return Duration(seconds: seconds, microseconds: nanos ~/ 1000);
  }

  EnumValue _readEnum(DataType type) {
    final elementName = readAny() as String;
    final typeName = switch (type) {
      DataType.direction => 'Direction',
      DataType.merge => 'Merge',
      DataType.t => 'T',
      DataType.gType => 'GType',
      _ => 'Enum',
    };
    return EnumValue(typeName, elementName);
  }

  String _firstLabel(List<dynamic> labels) {
    if (labels.isEmpty) return '';
    return labels.first.toString();
  }

  List<Property> _asProperties(dynamic value) {
    if (value == null) return const [];
    if (value is List) {
      return value.whereType<Property>().toList();
    }
    if (value is Map) {
      return value.entries.map((entry) => Property(entry.key.toString(), entry.value)).toList();
    }
    return const [];
  }

  double _pow10(int scale) {
    var value = 1.0;
    for (var i = 0; i < scale.abs(); i++) {
      value *= 10;
    }
    return scale >= 0 ? value : 1 / value;
  }

  void _require(int count) {
    if (_offset + count > _bytes.length) {
      throw FormatException('Unexpected end of GraphBinary buffer at $_offset');
    }
  }
}
