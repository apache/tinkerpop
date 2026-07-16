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

import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:test/test.dart';

import '../../lib/driver/cluster.dart';
import '../../lib/driver/connection.dart';
import '../../lib/driver/transaction.dart';
import '../../lib/process/gremlin_lang.dart';

void main() {
  // -------------------------------------------------------------------------
  // ClusterBuilder
  // -------------------------------------------------------------------------
  group('ClusterBuilder', () {
    test('defaults to localhost:8182/gremlin', () {
      final cluster = Cluster.build().create();
      expect(cluster.toString(), contains('localhost:8182/gremlin'));
      cluster.close();
    });

    test('single contact point constructs correct URL', () {
      final cluster = Cluster.build()
          .addContactPoint('myserver')
          .port(9999)
          .path('/ws')
          .create();
      expect(cluster.toString(), contains('myserver:9999/ws'));
      cluster.close();
    });

    test('multiple contact points produce multiple hosts', () {
      final cluster =
          Cluster.build().addContactPoints(['h1', 'h2', 'h3']).create();
      final str = cluster.toString();
      expect(str, contains('h1'));
      expect(str, contains('h2'));
      expect(str, contains('h3'));
      cluster.close();
    });

    test('enableSsl switches to https scheme', () {
      final cluster = Cluster.build()
          .addContactPoint('secure-host')
          .enableSsl(true)
          .create();
      expect(cluster.toString(), contains('https://secure-host'));
      cluster.close();
    });

    test('buildFrom shorthand sets single contact point', () {
      final cluster = Cluster.buildFrom('shorthand-host').create();
      expect(cluster.toString(), contains('shorthand-host'));
      cluster.close();
    });

    test('hosts list has correct count', () {
      final cluster =
          Cluster.build().addContactPoints(['a', 'b', 'c']).create();
      expect(cluster.hosts.length, 3);
      cluster.close();
    });
  });

  // -------------------------------------------------------------------------
  // RoundRobin
  // -------------------------------------------------------------------------
  group('RoundRobin', () {
    test('selects from all available hosts', () {
      final hosts = [HostEntry('a'), HostEntry('b'), HostEntry('c')];
      final lb = RoundRobin();
      lb.initialize(hosts);

      final selected = {for (var i = 0; i < 9; i++) lb.select()!.url};
      expect(selected, equals({'a', 'b', 'c'}));
    });

    test('skips unavailable hosts', () {
      final hosts = [HostEntry('a'), HostEntry('b'), HostEntry('c')];
      final lb = RoundRobin();
      lb.initialize(hosts);
      lb.onUnavailable(hosts[1]);

      for (var i = 0; i < 10; i++) {
        expect(lb.select()?.url, isNot('b'));
      }
    });

    test('returns null when all hosts are unavailable', () {
      final hosts = [HostEntry('a'), HostEntry('b')];
      final lb = RoundRobin();
      lb.initialize(hosts);
      lb.onUnavailable(hosts[0]);
      lb.onUnavailable(hosts[1]);
      expect(lb.select(), isNull);
    });

    test('exclude set prevents selecting those hosts', () {
      final hosts = [HostEntry('only')];
      final lb = RoundRobin();
      lb.initialize(hosts);
      expect(lb.select(exclude: {hosts[0]}), isNull);
    });

    test('onAvailable re-adds host to rotation', () {
      final hosts = [HostEntry('a'), HostEntry('b')];
      final lb = RoundRobin();
      lb.initialize(hosts);
      lb.onUnavailable(hosts[0]);
      lb.onUnavailable(hosts[1]);
      expect(lb.select(), isNull);

      lb.onAvailable(hosts[0]);
      expect(lb.select()?.url, 'a');
    });

    test('does not add duplicate on repeated onAvailable', () {
      final hosts = [HostEntry('a')];
      final lb = RoundRobin();
      lb.initialize(hosts);
      lb.onUnavailable(hosts[0]);
      lb.onAvailable(hosts[0]);
      lb.onAvailable(hosts[0]); // second call should be idempotent

      // Should still select exactly one host per call
      int count = 0;
      for (var i = 0; i < 6; i++) {
        if (lb.select()?.url == 'a') count++;
      }
      expect(count, 6);
    });
  });

  // -------------------------------------------------------------------------
  // HostEntry
  // -------------------------------------------------------------------------
  group('HostEntry', () {
    test('creates Connection lazily and reuses it', () {
      final host = HostEntry('http://example.com');
      const opts = ConnectionOptions();
      final c1 = host.open(opts);
      final c2 = host.open(opts);
      expect(identical(c1, c2), isTrue);
      host.dispose();
    });

    test('reset closes connection and creates a fresh one on next open', () {
      final host = HostEntry('http://example.com');
      const opts = ConnectionOptions();
      final c1 = host.open(opts);
      host.reset();
      final c2 = host.open(opts);
      expect(identical(c1, c2), isFalse);
      host.dispose();
    });
  });

  // -------------------------------------------------------------------------
  // Cluster routing (uses real local HTTP servers)
  // -------------------------------------------------------------------------
  group('Cluster routing', () {
    late HttpServer goodServer;

    setUpAll(() async {
      goodServer = await HttpServer.bind('127.0.0.1', 0);
      unawaited(goodServer.forEach((req) async {
        final body = _emptyGraphBinaryResponse();
        req.response
          ..statusCode = 200
          ..headers.set('Content-Type', 'application/vnd.graphbinary-v4.0')
          ..headers.contentLength = body.length
          ..add(body);
        await req.response.close();
      }).catchError((_) {}));
    });

    tearDownAll(() => goodServer.close(force: true));

    test('connect() returns ClusterRemoteConnection', () {
      final cluster = Cluster.build()
          .addContactPoint('127.0.0.1')
          .port(goodServer.port)
          .path('/')
          .create();
      expect(cluster.connect(), isA<ClusterRemoteConnection>());
      cluster.close();
    });

    test('isOpen is true when at least one host is available', () {
      final cluster = Cluster.build()
          .addContactPoints(['h1', 'h2'])
          .create();
      expect(cluster.connect().isOpen, isTrue);
      cluster.close();
    });

    test('isOpen is false when all hosts are unavailable', () {
      final cluster = Cluster.build()
          .addContactPoints(['h1', 'h2'])
          .create();
      for (final h in cluster.hosts) {
        h.isAvailable = false;
      }
      expect(cluster.connect().isOpen, isFalse);
      cluster.close();
    });

    test('all hosts unavailable throws NoHostAvailableException', () async {
      final cluster = Cluster.build()
          .addContactPoints(['h1', 'h2'])
          .reconnectInterval(const Duration(hours: 1))
          .create();
      for (final h in cluster.hosts) {
        h.isAvailable = false;
      }

      final conn = cluster.connect();
      // _stream should throw immediately since no hosts are available
      await expectLater(
        conn
            .submit(_simpleLang('g.inject(1)'))
            .then((t) => t.toList()),
        throwsA(isA<NoHostAvailableException>()),
      );
      await cluster.close();
    });

    test('markUnavailable schedules reconnect timer', () async {
      final cluster = Cluster.build()
          .addContactPoint('127.0.0.1')
          .port(1) // unreachable port
          .path('/')
          .reconnectInterval(const Duration(milliseconds: 50))
          .create();

      // Force the single host unavailable
      final host = cluster.hosts.first;
      host.isAvailable = false;

      // Wait briefly — no crash should occur
      await Future<void>.delayed(const Duration(milliseconds: 120));
      await cluster.close();
    });
  });

  // -------------------------------------------------------------------------
  // ClusterRemoteConnection
  // -------------------------------------------------------------------------
  group('ClusterRemoteConnection', () {
    test('tx() pins to a single host and returns Transaction', () {
      final cluster = Cluster.build()
          .addContactPoints(['h1', 'h2'])
          .options(const ConnectionOptions(traversalSource: 'gtx'))
          .create();
      final tx = cluster.connect().tx();
      expect(tx, isA<Transaction>());
      cluster.close();
    });

    test('tx() throws NoHostAvailableException when no hosts available', () {
      final cluster = Cluster.build()
          .addContactPoint('h1')
          .create();
      for (final h in cluster.hosts) {
        h.isAvailable = false;
      }
      expect(
        () => cluster.connect().tx(),
        throwsA(isA<NoHostAvailableException>()),
      );
      cluster.close();
    });

    test('traversalSource override is applied', () {
      final cluster = Cluster.build()
          .addContactPoint('h1')
          .options(const ConnectionOptions(traversalSource: 'g'))
          .create();
      final conn = cluster.connect('my_source');
      expect(conn.effectiveTraversalSource, 'my_source');
      cluster.close();
    });

    test('traversalSource falls back to options when not overridden', () {
      final cluster = Cluster.build()
          .addContactPoint('h1')
          .options(const ConnectionOptions(traversalSource: 'default_g'))
          .create();
      final conn = cluster.connect();
      expect(conn.effectiveTraversalSource, 'default_g');
      cluster.close();
    });
  });

  // -------------------------------------------------------------------------
  // ConnectionOptions.copyWith
  // -------------------------------------------------------------------------
  group('ConnectionOptions.copyWith', () {
    test('overrides only specified fields', () {
      const base = ConnectionOptions(
        traversalSource: 'g',
        maxConnectionsPerHost: 4,
      );
      final copy = base.copyWith(traversalSource: 'gx');
      expect(copy.traversalSource, 'gx');
      expect(copy.maxConnectionsPerHost, 4);
    });

    test('all fields can be overridden', () {
      const base = ConnectionOptions();
      final copy = base.copyWith(
        traversalSource: 'tx',
        maxConnectionsPerHost: 2,
        connectTimeout: Duration(seconds: 5),
      );
      expect(copy.traversalSource, 'tx');
      expect(copy.maxConnectionsPerHost, 2);
      expect(copy.connectTimeout, const Duration(seconds: 5));
    });
  });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A minimal valid GraphBinary v4 response body encoding an empty result list.
Uint8List _emptyGraphBinaryResponse() => Uint8List.fromList([
      0x81, // version
      0x00, 0xC8, // status code 200
      0x00, 0x00, // status message: empty (length=0)
      0x00, // no exception class
      0x09, 0x00, // data type = LIST, non-null flag
      0x00, 0x00, 0x00, 0x00, // list length = 0
      0x27, 0x00, 0x00, // bulked = BOOLEAN false
    ]);

/// Builds a minimal [GremlinLang] that serialises to [gremlin].
GremlinLang _simpleLang(String gremlin) =>
    GremlinLang()..addStep(gremlin);
