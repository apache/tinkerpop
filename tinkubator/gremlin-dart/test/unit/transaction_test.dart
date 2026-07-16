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
import 'dart:io';
import 'dart:async';
import 'dart:typed_data';

import 'package:gremlin_dart/driver/connection.dart';
import 'package:gremlin_dart/driver/driver_remote_connection.dart';
import 'package:gremlin_dart/driver/response_error.dart';
import 'package:gremlin_dart/driver/transaction.dart';
import 'package:gremlin_dart/process/anonymous_traversal.dart';
import 'package:gremlin_dart/process/graph_traversal.dart';
import 'package:gremlin_dart/process/traversal_strategy.dart';
import 'package:gremlin_dart/structure/graph.dart';
import 'package:gremlin_dart/structure/io/graph_binary/data_type.dart';
import 'package:test/test.dart';

void main() {
  group('Transaction', () {
    test('rejects commit before begin', () async {
      final tx = DriverRemoteConnection('http://localhost/gremlin').tx();
      await expectLater(tx.commit(), throwsStateError);
    });

    test('open starts transaction', () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(server.close);

      server.listen((request) async {
        final body = await request.fold<BytesBuilder>(
          BytesBuilder(copy: false),
          (builder, chunk) => builder..add(chunk),
        );
        final gremlin = _RequestReader(body.takeBytes()).readGremlin();
        expect(gremlin, 'g.tx().begin()');
        request.response.headers.contentType =
            ContentType('application', 'vnd.graphbinary-v4.0');
        request.response.headers
            .set(Connection.transactionIdHeader, 'tx-open');
        request.response.add(_response([_typedMap({'transactionId': 'tx-open'})]));
        await request.response.close();
      });

      final connection = DriverRemoteConnection(
        'http://${server.address.host}:${server.port}/gremlin',
      );
      final tx = connection.tx();
      await tx.open();

      expect(tx.isOpen, isTrue);
      expect(tx.transactionId, 'tx-open');
      await connection.close();
    });

    test('remote traversal source exposes tx', () {
      final connection = DriverRemoteConnection(
        'http://localhost/gremlin',
        const ConnectionOptions(traversalSource: 'gtx'),
      );
      final g = traversal().withRemote(connection);

      final tx = g.tx();

      expect(tx, isA<Transaction>());
    });

    test('local traversal source rejects tx', () {
      final g = GraphTraversalSource(Graph(), TraversalStrategies());

      expect(() => g.tx(), throwsStateError);
    });

    test('begin returns tx-bound traversal source and sends transactionId',
        () async {
      final seen = <_RecordedRequest>[];
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(server.close);

      server.listen((request) async {
        final body = await request.fold<BytesBuilder>(
          BytesBuilder(copy: false),
          (builder, chunk) => builder..add(chunk),
        );
        final recorded = _RecordedRequest(
          headers: request.headers,
          body: body.takeBytes(),
        );
        seen.add(recorded);

        final fields = _RequestReader(recorded.body).readFields();
        final gremlin = _RequestReader(recorded.body).readGremlin();
        final txId = fields['transactionId'] as String?;

        request.response.headers.contentType =
            ContentType('application', 'vnd.graphbinary-v4.0');
        if (gremlin == 'g.tx().begin()') {
          expect(fields['g'], 'gtx');
          expect(txId, isNull);
          expect(
              request.headers.value(Connection.transactionIdHeader), isNull);
          request.response.headers
              .set(Connection.transactionIdHeader, 'tx-123');
          request.response.add(_response([_typedMap({'transactionId': 'tx-123'})]));
        } else if (gremlin == 'g.V().count()') {
          expect(txId, 'tx-123');
          expect(
              request.headers.value(Connection.transactionIdHeader), 'tx-123');
          request.response.headers
              .set(Connection.transactionIdHeader, 'tx-123');
          request.response.add(_response([_int32(1)]));
        } else if (gremlin == 'g.tx().commit()') {
          expect(txId, 'tx-123');
          expect(
              request.headers.value(Connection.transactionIdHeader), 'tx-123');
          request.response.headers
              .set(Connection.transactionIdHeader, 'tx-123');
          request.response.add(_response(const []));
        } else {
          fail('Unexpected gremlin request: $gremlin');
        }
        await request.response.close();
      });

      final connection = DriverRemoteConnection(
        'http://${server.address.host}:${server.port}/gremlin',
        const ConnectionOptions(traversalSource: 'gtx'),
      );

      final tx = connection.tx();
      final gtx = await tx.begin();
      expect(identical(gtx.tx(), tx), isTrue);
      final count = await gtx.V().count().next<int>();
      await tx.commit();
      await connection.close();

      expect(count, 1);
      expect(tx.isOpen, isFalse);
      expect(seen, hasLength(3));
    });

    test('begin falls back to transactionId response header', () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(server.close);

      server.listen((request) async {
        final body = await request.fold<BytesBuilder>(
          BytesBuilder(copy: false),
          (builder, chunk) => builder..add(chunk),
        );
        final gremlin = _RequestReader(body.takeBytes()).readGremlin();

        request.response.headers.contentType =
            ContentType('application', 'vnd.graphbinary-v4.0');
        request.response.headers
            .set(Connection.transactionIdHeader, 'tx-header-only');

        if (gremlin == 'g.tx().begin()') {
          request.response.add(_response([_typedMap({'notTransactionId': 'x'})]));
        } else if (gremlin == 'g.tx().commit()') {
          request.response.add(_response(const []));
        } else {
          fail('Unexpected gremlin request: $gremlin');
        }

        await request.response.close();
      });

      final connection = DriverRemoteConnection(
        'http://${server.address.host}:${server.port}/gremlin',
      );

      final tx = connection.tx();
      await tx.begin();
      expect(tx.transactionId, 'tx-header-only');
      await tx.commit();
      await connection.close();
    });

    test('failed begin closes transaction permanently', () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(server.close);

      server.listen((request) async {
        await request.drain();
        request.response.statusCode = 400;
        request.response.headers.contentType =
            ContentType('application', 'vnd.graphbinary-v4.0');
        request.response.add(_response(const [],
            statusCode: 400,
            statusMessage: 'Graph does not support transactions',
            exception: 'TransactionException'));
        await request.response.close();
      });

      final connection = DriverRemoteConnection(
        'http://${server.address.host}:${server.port}/gremlin',
      );

      final tx = connection.tx();
      await expectLater(tx.begin(), throwsA(isA<ResponseError>()));
      expect(tx.isOpen, isFalse);
      expect(tx.transactionId, isNull);
      await expectLater(tx.begin(), throwsStateError);
      await connection.close();
    });

    test('close honors rollback behavior', () async {
      final seenGremlins = <String>[];
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(server.close);

      server.listen((request) async {
        final body = await request.fold<BytesBuilder>(
          BytesBuilder(copy: false),
          (builder, chunk) => builder..add(chunk),
        );
        final gremlin = _RequestReader(body.takeBytes()).readGremlin();
        seenGremlins.add(gremlin);
        request.response.headers.contentType =
            ContentType('application', 'vnd.graphbinary-v4.0');
        request.response.headers
            .set(Connection.transactionIdHeader, 'tx-close');
        if (gremlin == 'g.tx().begin()') {
          request.response.add(_response([_typedMap({'transactionId': 'tx-close'})]));
        } else if (gremlin == 'g.tx().rollback()') {
          request.response.add(_response(const []));
        } else {
          fail('Unexpected gremlin request: $gremlin');
        }
        await request.response.close();
      });

      final connection = DriverRemoteConnection(
        'http://${server.address.host}:${server.port}/gremlin',
      );
      final tx = connection.tx().onClose(TransactionCloseBehavior.rollback);
      await tx.begin();
      await tx.close();
      await connection.close();

      expect(seenGremlins, ['g.tx().begin()', 'g.tx().rollback()']);
    });

    test('close honors manual behavior', () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(server.close);

      server.listen((request) async {
        final body = await request.fold<BytesBuilder>(
          BytesBuilder(copy: false),
          (builder, chunk) => builder..add(chunk),
        );
        final gremlin = _RequestReader(body.takeBytes()).readGremlin();
        expect(gremlin, 'g.tx().begin()');
        request.response.headers.contentType =
            ContentType('application', 'vnd.graphbinary-v4.0');
        request.response.headers
            .set(Connection.transactionIdHeader, 'tx-manual');
        request.response
            .add(_response([_typedMap({'transactionId': 'tx-manual'})]));
        await request.response.close();
      });

      final connection = DriverRemoteConnection(
        'http://${server.address.host}:${server.port}/gremlin',
      );
      final tx = connection.tx().onClose(TransactionCloseBehavior.manual);
      await tx.begin();

      await expectLater(tx.close(), throwsStateError);
      await connection.close();
    });

    test('readWrite is unsupported', () {
      final tx = DriverRemoteConnection('http://localhost/gremlin').tx();

      expect(() => tx.readWrite(), throwsUnsupportedError);
      expect(() => tx.onReadWrite((_) {}), throwsUnsupportedError);
    });

    test('transaction listeners are unsupported', () {
      final tx = DriverRemoteConnection('http://localhost/gremlin').tx();

      expect(() => tx.addTransactionListener((_) {}), throwsUnsupportedError);
      expect(
          () => tx.removeTransactionListener((_) {}), throwsUnsupportedError);
      expect(() => tx.clearTransactionListeners(), throwsUnsupportedError);
    });

    test('serializes transactional submissions', () async {
      final seenGremlins = <String>[];
      final firstTraversalRelease = Completer<void>();
      var firstTraversalSeen = false;

      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(server.close);

      server.listen((request) async {
        final body = await request.fold<BytesBuilder>(
          BytesBuilder(copy: false),
          (builder, chunk) => builder..add(chunk),
        );
        final reader = _RequestReader(body.takeBytes());
        final gremlin = reader.readGremlin();
        final fields = reader.readFields();
        seenGremlins.add(gremlin);

        request.response.headers.contentType =
            ContentType('application', 'vnd.graphbinary-v4.0');
        request.response.headers
            .set(Connection.transactionIdHeader, 'tx-123');

        if (gremlin == 'g.tx().begin()') {
          request.response.add(_response([_typedMap({'transactionId': 'tx-123'})]));
        } else if (gremlin == 'g.V().count()') {
          firstTraversalSeen = true;
          await firstTraversalRelease.future;
          request.response.add(_response([_int32(1)]));
        } else if (gremlin == 'g.E().count()') {
          expect(fields['transactionId'], 'tx-123');
          request.response.add(_response([_int32(2)]));
        } else if (gremlin == 'g.tx().commit()') {
          request.response.add(_response(const []));
        } else {
          fail('Unexpected gremlin request: $gremlin');
        }

        await request.response.close();
      });

      final connection = DriverRemoteConnection(
        'http://${server.address.host}:${server.port}/gremlin',
      );

      final tx = connection.tx();
      final gtx = await tx.begin();
      final first = gtx.V().count().next<int>();
      final second = gtx.E().count().next<int>();

      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(firstTraversalSeen, isTrue);
      expect(seenGremlins, ['g.tx().begin()', 'g.V().count()']);

      firstTraversalRelease.complete();

      expect(await first, 1);
      expect(await second, 2);
      await tx.commit();
      await connection.close();

      expect(seenGremlins, [
        'g.tx().begin()',
        'g.V().count()',
        'g.E().count()',
        'g.tx().commit()',
      ]);
    });

    test('submit after commit fails deterministically', () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(server.close);

      server.listen((request) async {
        final body = await request.fold<BytesBuilder>(
          BytesBuilder(copy: false),
          (builder, chunk) => builder..add(chunk),
        );
        final gremlin = _RequestReader(body.takeBytes()).readGremlin();
        request.response.headers.contentType =
            ContentType('application', 'vnd.graphbinary-v4.0');
        request.response.headers
            .set(Connection.transactionIdHeader, 'tx-after-commit');

        if (gremlin == 'g.tx().begin()') {
          request.response.add(
              _response([_typedMap({'transactionId': 'tx-after-commit'})]));
        } else if (gremlin == 'g.tx().commit()') {
          request.response.add(_response(const []));
        } else {
          fail('Unexpected gremlin request: $gremlin');
        }
        await request.response.close();
      });

      final connection = DriverRemoteConnection(
        'http://${server.address.host}:${server.port}/gremlin',
      );

      final tx = connection.tx();
      final gtx = await tx.begin();
      await tx.commit();

      await expectLater(gtx.V().count().next<int>(), throwsStateError);
      await connection.close();
    });

    test('submit after rollback fails deterministically', () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(server.close);

      server.listen((request) async {
        final body = await request.fold<BytesBuilder>(
          BytesBuilder(copy: false),
          (builder, chunk) => builder..add(chunk),
        );
        final gremlin = _RequestReader(body.takeBytes()).readGremlin();
        request.response.headers.contentType =
            ContentType('application', 'vnd.graphbinary-v4.0');
        request.response.headers
            .set(Connection.transactionIdHeader, 'tx-after-rollback');

        if (gremlin == 'g.tx().begin()') {
          request.response.add(
              _response([_typedMap({'transactionId': 'tx-after-rollback'})]));
        } else if (gremlin == 'g.tx().rollback()') {
          request.response.add(_response(const []));
        } else {
          fail('Unexpected gremlin request: $gremlin');
        }
        await request.response.close();
      });

      final connection = DriverRemoteConnection(
        'http://${server.address.host}:${server.port}/gremlin',
      );

      final tx = connection.tx();
      final gtx = await tx.begin();
      await tx.rollback();

      await expectLater(gtx.V().count().next<int>(), throwsStateError);
      await connection.close();
    });

    test('transaction remains usable after parent connection closes', () async {
      final seenGremlins = <String>[];
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(server.close);

      server.listen((request) async {
        final body = await request.fold<BytesBuilder>(
          BytesBuilder(copy: false),
          (builder, chunk) => builder..add(chunk),
        );
        final reader = _RequestReader(body.takeBytes());
        final gremlin = reader.readGremlin();
        final fields = reader.readFields();
        seenGremlins.add(gremlin);

        request.response.headers.contentType =
            ContentType('application', 'vnd.graphbinary-v4.0');
        request.response.headers
            .set(Connection.transactionIdHeader, 'tx-dedicated');

        if (gremlin == 'g.tx().begin()') {
          request.response
              .add(_response([_typedMap({'transactionId': 'tx-dedicated'})]));
        } else if (gremlin == 'g.V().count()') {
          expect(fields['transactionId'], 'tx-dedicated');
          request.response.add(_response([_int32(1)]));
        } else if (gremlin == 'g.tx().commit()') {
          expect(fields['transactionId'], 'tx-dedicated');
          request.response.add(_response(const []));
        } else {
          fail('Unexpected gremlin request: $gremlin');
        }

        await request.response.close();
      });

      final connection = DriverRemoteConnection(
        'http://${server.address.host}:${server.port}/gremlin',
      );

      final tx = connection.tx();
      final gtx = await tx.begin();
      await connection.close();

      final count = await gtx.V().count().next<int>();
      await tx.commit();

      expect(count, 1);
      expect(seenGremlins, [
        'g.tx().begin()',
        'g.V().count()',
        'g.tx().commit()',
      ]);
    });
  });
}

class _RecordedRequest {
  final HttpHeaders headers;
  final Uint8List body;

  _RecordedRequest({required this.headers, required this.body});
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

Uint8List _int32(int value) {
  final b = _Bytes()..header(DataType.int_);
  b.i32(value);
  return b.done();
}

Uint8List _typedMap(Map<String, String> value) {
  final b = _Bytes()..header(DataType.map);
  b.i32(value.length);
  for (final entry in value.entries) {
    b.bytes(_string(entry.key));
    b.bytes(_string(entry.value));
  }
  return b.done();
}

Uint8List _string(String value) {
  final b = _Bytes()..header(DataType.string);
  b.bareString(value);
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

class _RequestReader {
  final ByteData _data;
  final Uint8List _bytes;

  _RequestReader(Uint8List bytes)
      : _bytes = bytes,
        _data = ByteData.sublistView(bytes);

  Map<String, dynamic> readFields() {
    var offset = 0;
    expect(_bytes[offset++], 0x84);
    final entryCount = _data.getInt32(offset, Endian.big);
    offset += 4;
    final result = <String, dynamic>{};
    for (var i = 0; i < entryCount; i++) {
      final key = _readValue(offset) as String;
      offset = _nextOffset;
      final value = _readValue(offset);
      offset = _nextOffset;
      result[key] = value;
    }
    return result;
  }

  String readGremlin() {
    var offset = 0;
    expect(_bytes[offset++], 0x84);
    final entryCount = _data.getInt32(offset, Endian.big);
    offset += 4;
    for (var i = 0; i < entryCount; i++) {
      _readValue(offset);
      offset = _nextOffset;
      _readValue(offset);
      offset = _nextOffset;
    }
    final length = _data.getInt32(offset, Endian.big);
    offset += 4;
    return utf8.decode(_bytes.sublist(offset, offset + length));
  }

  int _nextOffset = 0;

  dynamic _readValue(int offset) {
    final typeCode = _bytes[offset++];
    final valueFlag = _bytes[offset++];
    expect(valueFlag, 0x00);
    if (typeCode == DataType.string.code) {
      final length = _data.getInt32(offset, Endian.big);
      offset += 4;
      final value = utf8.decode(_bytes.sublist(offset, offset + length));
      _nextOffset = offset + length;
      return value;
    }
    if (typeCode == DataType.boolean.code) {
      final value = _bytes[offset] == 0x01;
      _nextOffset = offset + 1;
      return value;
    }
    if (typeCode == DataType.int_.code) {
      final value = _data.getInt32(offset, Endian.big);
      _nextOffset = offset + 4;
      return value;
    }
    if (typeCode == DataType.map.code) {
      final length = _data.getInt32(offset, Endian.big);
      offset += 4;
      final value = <dynamic, dynamic>{};
      for (var i = 0; i < length; i++) {
        final key = _readValue(offset);
        offset = _nextOffset;
        final entryValue = _readValue(offset);
        offset = _nextOffset;
        value[key] = entryValue;
      }
      _nextOffset = offset;
      return value;
    }
    throw UnsupportedError(
        'Unsupported request field type 0x${typeCode.toRadixString(16)}');
  }
}
