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
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import 'dart:io';

import 'package:test/test.dart';

import 'package:gremlin_dart/driver/connection.dart';
import 'package:gremlin_dart/driver/client.dart';
import 'package:gremlin_dart/driver/cluster.dart';
import 'package:gremlin_dart/driver/driver_remote_connection.dart';
import 'package:gremlin_dart/driver/request_message.dart';
import 'package:gremlin_dart/driver/response_error.dart';
import 'package:gremlin_dart/process/anonymous_traversal.dart';

// Tests in this file require a live Gremlin Server.
// Set GREMLIN_SERVER_URL (e.g. http://localhost:45940/gremlin) to enable.
// When unset the entire suite is skipped — safe to run in unit-test-only CI.

String? get _serverUrl => Platform.environment['GREMLIN_SERVER_URL'];

void main() {
  final url = _serverUrl;
  if (url == null) {
    test('integration tests skipped — GREMLIN_SERVER_URL not set', () {
      markTestSkipped('Set GREMLIN_SERVER_URL to run integration tests');
    });
    return;
  }

  // ---------------------------------------------------------------------------
  // Connection — low-level submit
  // ---------------------------------------------------------------------------
  group('Connection', () {
    late Connection conn;

    setUp(() {
      conn =
          Connection(url, const ConnectionOptions(traversalSource: 'gmodern'));
    });

    tearDown(() => conn.close());

    test('submit inject returns expected count', () async {
      final result = await conn.submit(
        RequestMessage.build('g.inject(1,2,3)')
            .addG('gmodern')
            .addBulkResults(false)
            .create(),
      );
      expect(result.items.length, 3);
    });

    test('submit V() on modern graph returns 6 vertices', () async {
      final result = await conn.submit(
        RequestMessage.build('g.V()')
            .addG('gmodern')
            .addBulkResults(false)
            .create(),
      );
      expect(result.items.length, 6);
    });

    test('submit with bindings resolves variable', () async {
      final result = await conn.submit(
        RequestMessage.build('g.V(x).values("name")')
            .addG('gmodern')
            .addBindings({'x': 1})
            .addBulkResults(false)
            .create(),
      );
      expect(result.items, ['marko']);
    });

    test('server error surfaces as ResponseError', () async {
      await expectLater(
        conn.submit(
          RequestMessage.build('g.V().not_a_real_step()')
              .addG('gmodern')
              .create(),
        ),
        throwsA(isA<ResponseError>()),
      );
    });

    test('result contains correct types after GraphBinary deserialization',
        () async {
      final result = await conn.submit(
        RequestMessage.build('g.V(1).values("age")')
            .addG('gmodern')
            .addBulkResults(false)
            .create(),
      );
      expect(result.items.length, 1);
      final age = result.items.first;
      // age comes back as a Dart int (GraphBinary int32 decoded)
      expect(age, isA<int>());
      expect(age, 29);
    });

    test('Client submits a batchSize request option', () async {
      final client = Client(
        url,
        const ConnectionOptions(traversalSource: 'gmodern'),
      );
      addTearDown(client.close);

      final result = await client.submit(
        'g.V().values("name")',
        requestOptions: const RequestOptions(
          batchSize: 1,
          bulkResults: false,
        ),
      );
      expect(result.items, hasLength(6));
    });

    test('Client overrides its traversal source per request', () async {
      final client = Client(url);
      addTearDown(client.close);

      final result = await client.submit(
        'g.V().count()',
        requestOptions: const RequestOptions(
          traversalSource: 'gmodern',
          bulkResults: false,
        ),
      );
      expect(result.items, [6]);
    });
  });

  // ---------------------------------------------------------------------------
  // DriverRemoteConnection — DSL traversal execution
  // ---------------------------------------------------------------------------
  group('DriverRemoteConnection', () {
    late DriverRemoteConnection remote;

    setUp(() {
      remote = DriverRemoteConnection(
        url,
        const ConnectionOptions(traversalSource: 'gmodern'),
      );
    });

    tearDown(() => remote.close());

    test('g.V().count() returns 6 on modern graph', () async {
      final g = traversal().withRemote(remote);
      final count = await g.V().count().next();
      expect(count, 6);
    });

    test('g.V().values("name") returns all vertex names', () async {
      final g = traversal().withRemote(remote);
      final names = await g.V().values(['name']).toList();
      expect(names.length, 6);
      expect(names,
          containsAll(['marko', 'vadas', 'josh', 'peter', 'lop', 'ripple']));
    });

    test('g.V().has("name","marko").values("age") returns 29', () async {
      final g = traversal().withRemote(remote);
      final ages = await g.V().has('name', 'marko').values(['age']).toList();
      expect(ages.length, 1);
      expect(ages.first, 29);
    });

    test('g.V().hasLabel("person") returns 4 people', () async {
      final g = traversal().withRemote(remote);
      final people = await g.V().hasLabel('person').toList();
      expect(people.length, 4);
    });

    test('g.V().out("knows").values("name") returns knows edges', () async {
      final g = traversal().withRemote(remote);
      final names = await g
          .V()
          .has('name', 'marko')
          .out(['knows']).values(['name']).toList();
      expect(names.length, 2);
      expect(names, containsAll(['vadas', 'josh']));
    });

    test('g.E().count() returns 6 edges on modern graph', () async {
      final g = traversal().withRemote(remote);
      final count = await g.E().count().next();
      expect(count, 6);
    });

    test('chained steps: select, project, by', () async {
      final g = traversal().withRemote(remote);
      final results = await g
          .V()
          .hasLabel('person')
          .has('name', 'marko')
          .project('name', 'age')
          .by('name')
          .by('age')
          .toList();
      expect(results.length, 1);
      final row = results.first as Map;
      expect(row['name'], 'marko');
      expect(row['age'], 29);
    });

    test('g.inject() with varargs returns elements', () async {
      final g = traversal().withRemote(remote);
      final results = await g.inject(1, 2, 3).toList();
      expect(results.length, 3);
    });

    test('toList on empty traversal returns empty list', () async {
      final g = traversal().withRemote(remote);
      final results = await g.V().has('name', 'nobody').toList();
      expect(results, isEmpty);
    });
  });

  // ---------------------------------------------------------------------------
  // Cluster — load balancing and connection pooling
  // ---------------------------------------------------------------------------
  group('Cluster', () {
    test('single host connect and query', () async {
      final cluster = Cluster.build()
          .addContactPoint(Uri.parse(url).host)
          .port(Uri.parse(url).port)
          .path(Uri.parse(url).path)
          .create();
      try {
        final conn = cluster.connect('gmodern');
        final g = traversal().withRemote(conn);
        final count = await g.V().count().next();
        expect(count, 6);
      } finally {
        await cluster.close();
      }
    });

    test('cluster survives multiple sequential queries', () async {
      final cluster = Cluster.build()
          .addContactPoint(Uri.parse(url).host)
          .port(Uri.parse(url).port)
          .path(Uri.parse(url).path)
          .create();
      try {
        final conn = cluster.connect('gmodern');
        final g = traversal().withRemote(conn);
        for (var i = 0; i < 5; i++) {
          final count = await g.V().count().next();
          expect(count, 6);
        }
      } finally {
        await cluster.close();
      }
    });

    test('cluster default batch size is applied to traversal requests',
        () async {
      final cluster = Cluster.build()
          .addContactPoint(Uri.parse(url).host)
          .port(Uri.parse(url).port)
          .path(Uri.parse(url).path)
          .resultIterationBatchSize(2)
          .create();
      try {
        final g = traversal().withRemote(cluster.connect('gmodern'));
        final names = await g.V().values('name').toList();
        expect(names, hasLength(6));
      } finally {
        await cluster.close();
      }
    });
  });

  // ---------------------------------------------------------------------------
  // Transaction (session-based)
  // ---------------------------------------------------------------------------
  group('Transaction', () {
    test('writes in a tx are visible after commit', () async {
      final remote = DriverRemoteConnection(
        url,
        const ConnectionOptions(traversalSource: 'gtx'),
      );
      try {
        final g = traversal().withRemote(remote);
        final name = 'alice-${DateTime.now().microsecondsSinceEpoch}';
        final startCount =
            await g.V().has('name', name).count().next<int>() ?? 0;

        final tx = remote.tx();
        final gtx = await tx.begin();
        await gtx.addV('person').property('name', name).iterate();
        await tx.commit();

        final count = await g.V().has('name', name).count().next();
        expect(count, startCount + 1);

        // cleanup
        await g.V().has('name', name).drop().iterate();
      } finally {
        await remote.close();
      }
    });

    test('rollback discards writes', () async {
      final remote = DriverRemoteConnection(
        url,
        const ConnectionOptions(traversalSource: 'gtx'),
      );
      try {
        final g = traversal().withRemote(remote);
        final name = 'bob-${DateTime.now().microsecondsSinceEpoch}';
        final startCount =
            await g.V().has('name', name).count().next<int>() ?? 0;

        final tx = remote.tx();
        final gtx = await tx.begin();
        await gtx.addV('person').property('name', name).iterate();
        await tx.rollback();

        final count = await g.V().has('name', name).count().next();
        expect(count, startCount);
      } finally {
        await remote.close();
      }
    });
  });

  // ---------------------------------------------------------------------------
  // Retry
  // ---------------------------------------------------------------------------
  group('RetryInterceptor', () {
    test('query succeeds without retry on stable server', () async {
      final conn = Connection(
        url,
        ConnectionOptions(
          traversalSource: 'gmodern',
          retryOptions: RetryOptions(maxAttempts: 3, delay: Duration.zero),
        ),
      );
      try {
        final result = await conn.submit(
          RequestMessage.build('g.inject(42)')
              .addG('gmodern')
              .addBulkResults(false)
              .create(),
        );
        expect(result.items.length, 1);
      } finally {
        await conn.close();
      }
    });
  });
}
