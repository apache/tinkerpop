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

import '../lib/driver/connection.dart';
import '../lib/driver/request_message.dart';

/// Manual smoke test — requires a running Gremlin Server (docker compose up).
/// Run with: dart run tool/test_live.dart
void main() async {
  // Bindings
  await _test('bind int', 'g.V(vid)', 'gmodern', bindings: {'vid': 1});
  await _test('bind string', 'g.V().has("name", n)', 'gmodern',
      bindings: {'n': 'marko'});

  // Sack with different numeric literals
  await _test('sack 0.5f', 'g.withSack(2147483647i).inject(0.5f).sack(div).sack()', 'gmodern');
  await _test('sack 0.5D', 'g.withSack(2147483647i).inject(0.5D).sack(div).sack()', 'gmodern');

  // Conjoin edge cases
  await _test('conjoin [null,null]', 'g.inject([null,null]).conjoin("+")', 'ggraph');
  await _test('conjoin null,null', 'g.inject(null, null).conjoin("+")', 'ggraph');
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
