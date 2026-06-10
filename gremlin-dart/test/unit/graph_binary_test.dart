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
import 'package:gremlin_dart/process/traversal.dart';
import 'package:gremlin_dart/structure/graph.dart';
import 'package:gremlin_dart/structure/io/graph_binary/data_type.dart';
import 'package:gremlin_dart/structure/io/graph_binary/graph_binary_reader.dart';
import 'package:gremlin_dart/structure/io/graph_binary/graph_binary_writer.dart';
import 'package:test/test.dart';
import 'package:uuid/uuid_value.dart';

void main() {
  group('GraphBinary v4', () {
    test('writer encodes a request as binary GraphBinary v4', () {
      final message = RequestMessage.build('g.V().has("name",x)')
          .addG('g')
          .addTransactionId('tx-123')
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
      expect(fields['transactionId'], 'tx-123');
      expect(fields['evaluationTimeout'], 1234);
      expect(fields['bulkResults'], false);
      expect(fields['materializeProperties'], 'tokens');
    });

    test('writer encodes typed GraphBinary bindings', () {
      final uuid = UuidValue.fromString('00112233-4455-6677-8899-aabbccddeeff');
      final when = DateTime.utc(2024, 6, 1, 12, 34, 56, 789, 123);
      final message = RequestMessage.build('g.inject(x)')
          .addBinding('uuidValue', uuid)
          .addBinding('durationValue', const Duration(milliseconds: -500))
          .addBinding('bigIntValue', BigInt.parse('-9223372036854775809'))
          .addBinding('decimalValue', GDecimal(2, BigInt.from(12345)))
          .addBinding('when', when)
          .addBinding('bytes', Uint8List.fromList([1, 2, 3]))
          .create();

      final bytes = GraphBinaryWriter().writeRequest(message);
      final reader = _TestReader(bytes);
      reader.readUint8();
      final fields = reader.readBareMap();
      final bindings = fields['bindings'] as Map;

      expect(bindings['uuidValue'], uuid);
      expect(bindings['durationValue'], const Duration(milliseconds: -500));
      expect(bindings['bigIntValue'], BigInt.parse('-9223372036854775809'));
      final decimal = bindings['decimalValue'] as GDecimal;
      expect(decimal.scale, 2);
      expect(decimal.unscaled, BigInt.from(12345));
      expect(bindings['when'], when);
      expect(bindings['bytes'], Uint8List.fromList([1, 2, 3]));
    });

    test('reader decodes primitive and collection values from a response',
        () async {
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

    test('reader decodes extended scalar GraphBinary values', () async {
      final uuid = UuidValue.fromString('00112233-4455-6677-8899-aabbccddeeff');
      final timestamp = DateTime.utc(2024, 6, 1, 12, 34, 56, 789, 123);
      final response = _response([
        _uuid(uuid),
        _binary([1, 2, 3]),
        _bigInt(BigInt.parse('-9223372036854775809')),
        _bigDecimal(2, BigInt.from(12345)),
        _duration(const Duration(milliseconds: -500)),
        _dateTime(timestamp),
      ]);

      final decoded = await GraphBinaryReader().readResponse(response);
      final data = decoded['result']['data'] as List;

      expect(data[0], uuid);
      expect(data[1], Uint8List.fromList([1, 2, 3]));
      expect(data[2], BigInt.parse('-9223372036854775809'));
      final decimal = data[3] as GDecimal;
      expect(decimal.scale, 2);
      expect(decimal.unscaled, BigInt.from(12345));
      expect(data[4], const Duration(milliseconds: -500));
      expect(data[5], timestamp);
    });

    test('reader decodes bulked responses into traversers', () async {
      final response = _response([
        _string('marko'),
        _int64(2),
        _int32(29),
        _int64(3),
      ], bulked: true);

      final decoded = await GraphBinaryReader()
          .readResponseStream(Stream.value(response))
          .toList();

      expect(decoded, hasLength(2));
      expect(decoded[0], isA<Traverser<dynamic>>());
      expect((decoded[0] as Traverser).object, 'marko');
      expect((decoded[0] as Traverser).bulk, 2);
      expect((decoded[1] as Traverser).object, 29);
      expect((decoded[1] as Traverser).bulk, 3);
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
    {int statusCode = 200,
    String? statusMessage = 'OK',
    String? exception,
    bool bulked = false}) {
  final b = _Bytes();
  b.u8(0x84);
  b.u8(bulked ? 0x01 : 0x00);
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

Uint8List _uuid(UuidValue value) {
  final b = _Bytes()..header(DataType.uuid);
  b.bytes(value.toBytes(validate: true));
  return b.done();
}

Uint8List _binary(List<int> value) {
  final b = _Bytes()..header(DataType.binary);
  b.i32(value.length);
  b.bytes(Uint8List.fromList(value));
  return b.done();
}

Uint8List _bigInt(BigInt value) {
  final b = _Bytes()..header(DataType.bigInt);
  final encoded = _bigIntBytes(value);
  b.i32(encoded.length);
  b.bytes(encoded);
  return b.done();
}

Uint8List _bigDecimal(int scale, BigInt unscaled) {
  final b = _Bytes()..header(DataType.bigDecimal);
  b.i32(scale);
  final encoded = _bigIntBytes(unscaled);
  b.i32(encoded.length);
  b.bytes(encoded);
  return b.done();
}

Uint8List _duration(Duration value) {
  final b = _Bytes()..header(DataType.duration);
  final totalNanos = BigInt.from(value.inMicroseconds) * BigInt.from(1000);
  final nanosPerSecond = BigInt.from(1000000000);
  var seconds = totalNanos ~/ nanosPerSecond;
  var nanos = totalNanos.remainder(nanosPerSecond);
  if (nanos.isNegative) {
    seconds -= BigInt.one;
    nanos += nanosPerSecond;
  }
  b.i64Big(seconds);
  b.i32(nanos.toInt());
  return b.done();
}

Uint8List _dateTime(DateTime value) {
  final b = _Bytes()..header(DataType.dateTime);
  final utc = value.toUtc();
  final nanos = BigInt.from(utc.hour * 3600 + utc.minute * 60 + utc.second) *
          BigInt.from(1000000000) +
      BigInt.from(utc.millisecond * 1000000 + utc.microsecond * 1000);
  b.i32(utc.year);
  b.u8(utc.month);
  b.u8(utc.day);
  b.i64Big(nanos);
  b.i32(0);
  return b.done();
}

Uint8List _nullValue() =>
    Uint8List.fromList([DataType.unspecifiedNull.code, 0x01]);

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

Uint8List _bigIntBytes(BigInt value) {
  if (value == BigInt.zero) return Uint8List.fromList([0x00]);
  if (value > BigInt.zero) {
    final bytes = <int>[];
    var v = value;
    while (v > BigInt.zero) {
      bytes.add((v & BigInt.from(0xff)).toInt());
      v >>= 8;
    }
    final result = bytes.reversed.toList();
    if ((result.first & 0x80) != 0) result.insert(0, 0x00);
    return Uint8List.fromList(result);
  }

  int byteCount = 1;
  var limit = BigInt.from(0x80);
  while (-value > limit) {
    byteCount++;
    limit <<= 8;
  }

  var twos = (BigInt.one << (byteCount * 8)) + value;
  final bytes = <int>[];
  for (var i = 0; i < byteCount; i++) {
    bytes.add((twos & BigInt.from(0xff)).toInt());
    twos >>= 8;
  }
  return Uint8List.fromList(bytes.reversed.toList());
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

  void i64Big(BigInt value) {
    var unsigned = value;
    if (value.isNegative) unsigned += BigInt.one << 64;
    final bytes = Uint8List(8);
    for (var i = 7; i >= 0; i--) {
      bytes[i] = (unsigned & BigInt.from(0xff)).toInt();
      unsigned >>= 8;
    }
    _builder.add(bytes);
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

  Uint8List readBytes(int length) {
    final value = Uint8List.sublistView(bytes, offset, offset + length);
    offset += length;
    return value;
  }

  String readBareString() {
    final length = readInt32();
    return utf8.decode(readBytes(length));
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
      case DataType.uuid:
        return UuidValue.fromByteList(readBytes(16));
      case DataType.binary:
        return readBytes(readInt32());
      case DataType.bigInt:
        final length = readInt32();
        return _decodeBigInt(readBytes(length));
      case DataType.bigDecimal:
        final scale = readInt32();
        final length = readInt32();
        return GDecimal(scale, _decodeBigInt(readBytes(length)));
      case DataType.duration:
        final seconds = readInt64();
        final nanos = readInt32();
        return Duration(seconds: seconds, microseconds: nanos ~/ 1000);
      case DataType.dateTime:
        final year = readInt32();
        final month = readUint8();
        final day = readUint8();
        final nanos = _readBigInt64();
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
        return DateTime.utc(year, month, day, hour, minute, second, millisecond,
                microsecond)
            .subtract(Duration(seconds: offsetSeconds));
      case DataType.map:
        return readBareMap();
      default:
        throw StateError('Unsupported test type: $type');
    }
  }

  BigInt _readBigInt64() {
    BigInt value = BigInt.zero;
    for (var i = 0; i < 8; i++) {
      value = (value << 8) | BigInt.from(bytes[offset + i]);
    }
    offset += 8;
    if ((bytes[offset - 8] & 0x80) != 0) {
      value -= BigInt.one << 64;
    }
    return value;
  }

  BigInt _decodeBigInt(Uint8List valueBytes) {
    if (valueBytes.isEmpty) return BigInt.zero;
    BigInt value = BigInt.zero;
    for (final byte in valueBytes) {
      value = (value << 8) | BigInt.from(byte);
    }
    if ((valueBytes.first & 0x80) != 0) {
      value -= BigInt.one << (valueBytes.length * 8);
    }
    return value;
  }
}
