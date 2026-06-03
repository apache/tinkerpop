// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

import 'package:gremlin_dart/gremlin_dart.dart';

Future<void> main() async {
  final conn = DriverRemoteConnection('http://localhost:8182/gremlin');

  // g is the graph traversal source — all queries start here
  final g = traversal().withRemote(conn);

  // Buffered query: returns Future<List<T>>
  final vertices = await g.V().hasLabel('person').toList();
  print('People: $vertices');

  // Chained property access
  final names = await g.V().hasLabel('person').values(['name']).toList<String>();
  print('Names: $names');

  // Anonymous traversal inside repeat
  final reachable = await g
      .V()
      .has('name', P.eq('marko'))
      .repeat(Anon.out(['knows']))
      .times(2)
      .values(['name'])
      .toList<String>();
  print('Reachable from marko in 2 hops: $reachable');

  // Streaming query
  await for (final item in g.V().hasLabel('software').stream()) {
    print('  software: $item');
  }

  await conn.close();
}

extension StreamingTraversal on GraphTraversal {
  // Convenience extension so callers can do g.V()...stream()
  Stream<dynamic> stream() {
    return Stream.fromFuture(applyStrategies()).asyncExpand((_) {
      return resultsStream ?? const Stream.empty();
    });
  }
}
