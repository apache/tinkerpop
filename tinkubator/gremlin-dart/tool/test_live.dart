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

import 'dart:typed_data';

import 'package:uuid/uuid_value.dart';

import '../lib/driver/connection.dart';
import '../lib/process/traversal.dart';
import '../lib/driver/driver_remote_connection.dart';
import '../lib/driver/request_message.dart';
import '../lib/process/anonymous_traversal.dart';

/// Manual smoke test — requires a running Gremlin Server (docker compose up).
/// Run with: dart run tool/test_live.dart
void main() async {
  // Bindings
  await _test('bind int', 'g.V(vid)', 'gmodern', bindings: {'vid': 1});
  await _test('bind string', 'g.V().has("name", n)', 'gmodern',
      bindings: {'n': 'marko'});
  await _test('bind uuid property roundtrip',
      'g.addV("probe").property("uuid", v).values("uuid")', 'ggraph',
      bindings: {
        'v': UuidValue.fromString('47af10b8-58cc-4372-a567-0e02b2c3d479'),
      });
  await _test('bind binary property roundtrip',
      'g.addV("probe").property("blob", v).values("blob")', 'ggraph',
      bindings: {
        'v': Uint8List.fromList([1, 2, 3]),
      });
  await _test('bind duration property roundtrip',
      'g.addV("probe").property("length", v).values("length")', 'ggraph',
      bindings: {
        'v': const Duration(seconds: 9),
      });

  // Sack with different numeric literals
  await _test('sack 0.5f',
      'g.withSack(2147483647i).inject(0.5f).sack(div).sack()', 'gmodern');
  await _test('sack 0.5D',
      'g.withSack(2147483647i).inject(0.5D).sack(div).sack()', 'gmodern');

  // Conjoin edge cases
  await _test(
      'conjoin [null,null]', 'g.inject([null,null]).conjoin("+")', 'ggraph');
  await _test(
      'conjoin null,null', 'g.inject(null, null).conjoin("+")', 'ggraph');

  // BigInt / BigDecimal binding round-trips
  await _test('bind BigInt roundtrip',
      'g.addV("probe").property("n", v).values("n")', 'ggraph',
      bindings: {'v': BigInt.parse('123456789012345678901234567890')});
  await _test('bind GDecimal roundtrip',
      'g.addV("probe").property("d", v).values("d")', 'ggraph',
      bindings: {'v': GDecimal(27, BigInt.parse('3141592653589793238462643383'))});

  // Transactions
  await _testTransactionCommit();
  await _testTransactionRollback();
  await _testTransactionClosedAfterCommit();
  await _testTransactionInvalidCommit();
  await _testTransactionInvalidRollback();
  await _testTransactionTraversalAfterCommitWithStaleId();
  await _testBeginWithUserProvidedTransactionId();
}

Future<void> _test(String name, String gremlin, String g,
    {Map<String, dynamic>? bindings}) async {
  final builder = RequestMessage.build(gremlin).addG(g).addBulkResults(true);
  if (bindings != null) builder.addBindings(bindings);
  final c = Connection('http://localhost:45940/gremlin');
  try {
    final rs = await c.submit(builder.create());
    print('[$name] → ${rs.items}');
  } catch (e) {
    print('[$name] ERROR: $e');
  } finally {
    await c.close();
  }
}

Future<void> _testTransactionCommit() async {
  final label = 'tx_commit_probe_${DateTime.now().microsecondsSinceEpoch}';
  final connection = DriverRemoteConnection(
    'http://localhost:45940/gremlin',
    const ConnectionOptions(traversalSource: 'gtx'),
  );
  final g = traversal().withRemote(connection);
  final tx = g.tx();

  try {
    final gtx = await tx.begin();
    await gtx.addV(label).iterate();

    final outsideBefore = await g.V().hasLabel(label).count().next<dynamic>();
    print('[tx commit outside before] -> $outsideBefore');

    await tx.commit();

    final outsideAfter = await g.V().hasLabel(label).count().next<dynamic>();
    print('[tx commit outside after] -> $outsideAfter');
  } catch (e) {
    print('[tx commit] ERROR: $e');
  } finally {
    await connection.close();
  }
}

Future<void> _testTransactionRollback() async {
  final label = 'tx_rollback_probe_${DateTime.now().microsecondsSinceEpoch}';
  final connection = DriverRemoteConnection(
    'http://localhost:45940/gremlin',
    const ConnectionOptions(traversalSource: 'gtx'),
  );
  final g = traversal().withRemote(connection);
  final tx = g.tx();

  try {
    final gtx = await tx.begin();
    await gtx.addV(label).iterate();
    await tx.rollback();

    final outsideAfter = await g.V().hasLabel(label).count().next<dynamic>();
    print('[tx rollback outside after] -> $outsideAfter');
  } catch (e) {
    print('[tx rollback] ERROR: $e');
  } finally {
    await connection.close();
  }
}

Future<void> _testTransactionClosedAfterCommit() async {
  final label = 'tx_closed_probe_${DateTime.now().microsecondsSinceEpoch}';
  final connection = DriverRemoteConnection(
    'http://localhost:45940/gremlin',
    const ConnectionOptions(traversalSource: 'gtx'),
  );
  final g = traversal().withRemote(connection);
  final tx = g.tx();

  try {
    final gtx = await tx.begin();
    await gtx.addV(label).iterate();
    await tx.commit();

    try {
      await gtx.V().count().next<dynamic>();
      print('[tx closed after commit] UNEXPECTED SUCCESS');
    } catch (e) {
      print('[tx closed after commit] ERROR: $e');
    }
  } catch (e) {
    print('[tx closed after commit setup] ERROR: $e');
  } finally {
    await connection.close();
  }
}

Future<void> _testTransactionInvalidCommit() async {
  final connection = Connection('http://localhost:45940/gremlin');
  try {
    final rs = await connection.submit(
      RequestMessage.build('g.tx().commit()')
          .addG('gtx')
          .addTransactionId('fake-id')
          .create(),
    );
    print('[tx invalid commit] UNEXPECTED SUCCESS -> ${rs.items}');
  } catch (e) {
    print('[tx invalid commit] ERROR: $e');
  } finally {
    await connection.close();
  }
}

Future<void> _testTransactionInvalidRollback() async {
  final connection = Connection('http://localhost:45940/gremlin');
  try {
    final rs = await connection.submit(
      RequestMessage.build('g.tx().rollback()')
          .addG('gtx')
          .addTransactionId('fake-id')
          .create(),
    );
    print('[tx invalid rollback] UNEXPECTED SUCCESS -> ${rs.items}');
  } catch (e) {
    print('[tx invalid rollback] ERROR: $e');
  } finally {
    await connection.close();
  }
}

Future<void> _testTransactionTraversalAfterCommitWithStaleId() async {
  final label = 'tx_stale_id_probe_${DateTime.now().microsecondsSinceEpoch}';
  final connection = DriverRemoteConnection(
    'http://localhost:45940/gremlin',
    const ConnectionOptions(traversalSource: 'gtx'),
  );
  final raw = Connection('http://localhost:45940/gremlin');

  try {
    final tx = connection.tx();
    final gtx = await tx.begin();
    final txId = tx.transactionId;
    await gtx.addV(label).iterate();
    await tx.commit();

    if (txId == null) {
      print('[tx stale id after commit] ERROR: transactionId missing');
      return;
    }

    final rs = await raw.submit(
      RequestMessage.build('g.V().count()')
          .addG('gtx')
          .addTransactionId(txId)
          .create(),
    );
    print('[tx stale id after commit] UNEXPECTED SUCCESS -> ${rs.items}');
  } catch (e) {
    print('[tx stale id after commit] ERROR: $e');
  } finally {
    await raw.close();
    await connection.close();
  }
}

Future<void> _testBeginWithUserProvidedTransactionId() async {
  final connection = Connection('http://localhost:45940/gremlin');
  try {
    final rs = await connection.submit(
      RequestMessage.build('g.tx().begin()')
          .addG('gtx')
          .addTransactionId('fake-begin-id')
          .create(),
    );
    print('[tx begin with user id] UNEXPECTED SUCCESS -> ${rs.items}');
  } catch (e) {
    print('[tx begin with user id] ERROR: $e');
  } finally {
    await connection.close();
  }
}
