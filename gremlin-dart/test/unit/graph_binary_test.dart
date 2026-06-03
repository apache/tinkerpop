// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

import 'dart:convert';
import 'dart:typed_data';

import 'package:gremlin_dart/driver/request_message.dart';
import 'package:gremlin_dart/structure/graph.dart';
import 'package:gremlin_dart/structure/io/graph_binary/data_type.dart';
import 'package:gremlin_dart/structure/io/graph_binary/graph_binary_reader.dart';
import 'package:gremlin_dart/structure/io/graph_binary/graph_binary_writer.dart';
import 'package:test/test.dart';

void main() {
  group('GraphBinary v4', () {
    test('writer encodes a request as binary GraphBinary v4', () {
      final message = RequestMessage.build('g.V().has("name",x)')
          .addG('g')
          .addTimeoutMillis(1234)
          .addBulkResults(false)
          .addField('materializeProperties', 'tokens')
          .create();

      final bytes = GraphBinaryWriter().writeRequest(message);
      expect(bytes.first, 0x84);
      expect(utf8.decode(bytes, allowMalformed: true), isNot(startsWith('{')));

      final reader = _TestReader(bytes);
      expect(reader.readUint8(), 0x84);
      final fields = reader.readBareMap();
      final gremlin = reader.readBareString();

      expect(gremlin, 'g.V().has("name",x)');
      expect(fields['language'], 'gremlin-lang');
      expect(fields['g'], 'g');
      expect(fields['timeoutMs'], 1234);
      expect(fields['bulkResults'], false);
      expect(fields['materializeProperties'], 'tokens');
    });

    test('reader decodes primitive and collection values from a response', () async {
      final response = _response([
        _string('marko'),
        _int32(29),
        _int64(3000000000),
        _double(3.14),
        _boolean(true),
        _nullValue(),
        _list([_string('a'), _int32(1)]),
        _map({_string('name'): _string('lop'), _string('age'): _int32(5)}),
      ]);

      final decoded = await GraphBinaryReader().readResponse(response);
      final data = decoded['result']['data'] as List;

      expect(data[0], 'marko');
      expect(data[1], 29);
      expect(data[2], 3000000000);
      expect(data[3], closeTo(3.14, 0.0000001));
      expect(data[4], true);
      expect(data[5], isNull);
      expect(data[6], ['a', 1]);
      expect(data[7], {'name': 'lop', 'age': 5});
    });

    test('reader decodes vertex values from a response', () async {
      final response = _response([
        _vertex(1, 'person'),
      ]);

      final decoded = await GraphBinaryReader().readResponse(response);
      final vertex = (decoded['result']['data'] as List).single as Vertex;

      expect(vertex.id, 1);
      expect(vertex.label, 'person');
    });

    test('reader decodes a full response envelope', () async {
      final response = _response([
        _string('ok'),
      ], statusCode: 206, statusMessage: 'partial', exception: 'x');

      final decoded = await GraphBinaryReader().readResponse(response);

      expect(decoded['result']['bulked'], false);
      expect(decoded['result']['data'], ['ok']);
      expect(decoded['status'], {
        'code': 206,
        'message': 'partial',
        'exception': 'x',
      });
    });
  });
}

Uint8List _response(List<Uint8List> values,
    {int statusCode = 200, String? statusMessage = 'OK', String? exception}) {
  final b = _Bytes();
  b.u8(0x84);
  b.u8(0x00);
  for (final value in values) {
    b.bytes(value);
  }
  b.u8(DataType.marker.code);
  b.u8(0x00);
  b.u8(0x00);
  b.i32(statusCode);
  b.nullableBareString(statusMessage);
  b.nullableBareString(exception);
  return b.done();
}

Uint8List _string(String value) {
  final b = _Bytes()..header(DataType.string);
  b.bareString(value);
  return b.done();
}

Uint8List _int32(int value) {
  final b = _Bytes()..header(DataType.int_);
  b.i32(value);
  return b.done();
}

Uint8List _int64(int value) {
  final b = _Bytes()..header(DataType.long);
  b.i64(value);
  return b.done();
}

Uint8List _double(double value) {
  final b = _Bytes()..header(DataType.double_);
  b.f64(value);
  return b.done();
}

Uint8List _boolean(bool value) {
  final b = _Bytes()..header(DataType.boolean);
  b.u8(value ? 1 : 0);
  return b.done();
}

Uint8List _nullValue() => Uint8List.fromList([DataType.unspecifiedNull.code, 0x01]);

Uint8List _list(List<Uint8List> values, {bool fullyQualified = true}) {
  final b = _Bytes();
  if (fullyQualified) b.header(DataType.list);
  b.i32(values.length);
  values.forEach(b.bytes);
  return b.done();
}

Uint8List _map(Map<Uint8List, Uint8List> values) {
  final b = _Bytes()..header(DataType.map);
  b.i32(values.length);
  for (final entry in values.entries) {
    b.bytes(entry.key);
    b.bytes(entry.value);
  }
  return b.done();
}

Uint8List _vertex(int id, String label) {
  final b = _Bytes()..header(DataType.vertex);
  b.bytes(_int32(id));
  b.bytes(_list([_string(label)], fullyQualified: false));
  b.bytes(_list(const []));
  return b.done();
}

class _Bytes {
  final BytesBuilder _builder = BytesBuilder(copy: false);

  void header(DataType type) {
    u8(type.code);
    u8(0x00);
  }

  void u8(int value) => _builder.addByte(value & 0xff);

  void i32(int value) {
    final data = ByteData(4)..setInt32(0, value, Endian.big);
    _builder.add(data.buffer.asUint8List());
  }

  void i64(int value) {
    final data = ByteData(8)..setInt64(0, value, Endian.big);
    _builder.add(data.buffer.asUint8List());
  }

  void f64(double value) {
    final data = ByteData(8)..setFloat64(0, value, Endian.big);
    _builder.add(data.buffer.asUint8List());
  }

  void bareString(String value) {
    final encoded = utf8.encode(value);
    i32(encoded.length);
    _builder.add(encoded);
  }

  void nullableBareString(String? value) {
    if (value == null) {
      u8(0x01);
    } else {
      u8(0x00);
      bareString(value);
    }
  }

  void bytes(Uint8List value) => _builder.add(value);

  Uint8List done() => _builder.takeBytes();
}

class _TestReader {
  final Uint8List bytes;
  late final ByteData data = ByteData.sublistView(bytes);
  int offset = 0;

  _TestReader(this.bytes);

  int readUint8() => bytes[offset++];

  int readInt32() {
    final value = data.getInt32(offset, Endian.big);
    offset += 4;
    return value;
  }

  int readInt64() {
    final value = data.getInt64(offset, Endian.big);
    offset += 8;
    return value;
  }

  double readFloat64() {
    final value = data.getFloat64(offset, Endian.big);
    offset += 8;
    return value;
  }

  String readBareString() {
    final length = readInt32();
    final value = utf8.decode(bytes.sublist(offset, offset + length));
    offset += length;
    return value;
  }

  Map<dynamic, dynamic> readBareMap() {
    final length = readInt32();
    final map = <dynamic, dynamic>{};
    for (var i = 0; i < length; i++) {
      final key = readAny();
      map[key] = readAny();
    }
    return map;
  }

  dynamic readAny() {
    final type = DataType.fromCode(readUint8());
    final flag = readUint8();
    if (flag == 0x01) return null;
    switch (type) {
      case DataType.int_:
        return readInt32();
      case DataType.long:
        return readInt64();
      case DataType.double_:
        return readFloat64();
      case DataType.string:
        return readBareString();
      case DataType.boolean:
        return readUint8() == 1;
      case DataType.map:
        return readBareMap();
      default:
        throw StateError('Unsupported test type: $type');
    }
  }
}
