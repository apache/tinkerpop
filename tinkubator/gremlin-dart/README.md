<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# Dart Gremlin Language Variant

> **Experimental.** Gremlin-Dart is a [Tinkubator](../README.md) project at Stage 1: there is no release, the API
> may change at any time, and it is not officially supported by Apache TinkerPop. Use it for experimentation only.

[Apache TinkerPop™][tk] is a graph computing framework for both graph databases (OLTP) and graph analytic systems
(OLAP). [Gremlin][gremlin] is the graph traversal language of TinkerPop. It can be described as a functional,
data-flow language that enables users to succinctly express complex traversals on (or queries of) their application's
property graph.

Gremlin-Dart implements Gremlin within the Dart language and can be used with Dart 3.x, including in Flutter
applications. It targets TinkerPop 4.x and connects to a [Gremlin Server][gs] or other TinkerPop-enabled graph system
over HTTP, using GraphBinary 4 (`application/vnd.graphbinary-v4.0`) as its serialization format.

Traversals are written with a native Dart DSL and sent to the server as `gremlin-lang` strings. The driver does not
include a parser for Gremlin strings, but `Client` can still submit a Gremlin string to the server as-is.

## Getting Started

Gremlin-Dart is not published to [pub.dev](https://pub.dev). To try it, add it to your `pubspec.yaml` as a path or
git dependency:

```yaml
dependencies:
  gremlin_dart:
    path: /path/to/tinkerpop/tinkubator/gremlin-dart
```

Then import the library, create a connection and a traversal source `g`, and write traversals:

```dart
import 'package:gremlin_dart/gremlin_dart.dart';

Future<void> main() async {
  final conn = DriverRemoteConnection('http://localhost:8182/gremlin');
  final g = traversal().withRemote(conn);

  await g.addV('person').property('name', 'stephen').iterate();

  final names = await g.V().hasLabel('person').values('name').toList<String>();
  print(names);

  await conn.close();
}
```

A longer example is in [example/basic.dart](example/basic.dart).

## Differences from Java Gremlin

Gremlin-Dart follows canonical Gremlin closely, with a few changes forced by the Dart language:

* **Anonymous traversals** use `Anon` instead of `__`, because identifiers beginning with `_` are private in Dart:
  `g.V().repeat(Anon.out('knows')).times(2)`.
* **Steps that collide with Dart keywords or core members** take a trailing underscore: `as_()`, `in_()`, `is_()`,
  `not_()`, `and_()`, `or_()`, `from_()`, `with_()`, `map_()`, `filter_()` and others.
* **Terminal steps are asynchronous.** `toList()`, `next()` and `iterate()` return a `Future`. `toList<T>()` and
  `next<T>()` take a type argument for the result type.
* **Numeric types.** Dart has a single `int` and `double`, so use `GInt`, `GLong`, `GShort`, `GByte`, `GFloat`,
  `GDouble`, `GDecimal` and `GChar` when the exact Gremlin type matters.

## Connecting

`DriverRemoteConnection` connects to a single server. `ConnectionOptions` sets the traversal source name, headers,
timeouts, connection pool size, TLS, authentication and retries. `SslOptions` and `RetryOptions` are not exported
from `gremlin_dart.dart` yet, so import them from `driver/connection.dart`:

```dart
import 'package:gremlin_dart/driver/connection.dart' show SslOptions, RetryOptions;

final conn = DriverRemoteConnection(
  'https://localhost:8182/gremlin',
  ConnectionOptions(
    traversalSource: 'g',
    auth: BasicAuth(username: 'stephen', password: 'password'),
    ssl: SslOptions(trustedCertificatesPath: 'ca.pem'),
    retryOptions: RetryOptions(maxAttempts: 5, useExponentialBackoff: true),
  ),
);
```

`Cluster` spreads requests over several hosts with round-robin load balancing. It stops sending to a host after a
connection failure and starts again once the host responds:

```dart
final cluster = Cluster.build()
    .addContactPoints(['host1', 'host2'])
    .port(8182)
    .create();

final g = traversal().withRemote(cluster.connect());
final count = await g.V().count().next<int>();
await cluster.close();
```

You may use `SigV4Auth` to sign requests with AWS Signature Version 4 for systems secured that way. Give it the
region and the service name that the system expects. Credentials come from a `StaticCredentialsProvider` or from
the standard AWS environment variables with `EnvironmentCredentialsProvider`:

```dart
final conn = DriverRemoteConnection(
  'http://localhost:8182/gremlin',
  ConnectionOptions(
    auth: SigV4Auth(
      credentials: EnvironmentCredentialsProvider(),
      region: 'service-region',
      service: 'service-name',
    ),
  ),
);
```

To send a Gremlin string instead of using the DSL, use `Client`:

```dart
final client = Client('http://localhost:8182/gremlin');
final results = await client.submit('g.V().has("name", n)', bindings: {'n': 'marko'});
await client.close();
```

## Transactions

On a graph that supports transactions, start one from the connection and use the traversal source it returns:

```dart
final tx = conn.tx();
final gtx = await tx.begin();
await gtx.addV('person').property('name', 'pat').iterate();
await tx.commit();
```

## Building and Testing

Gremlin-Dart is not part of the main TinkerPop Maven build, so its Maven commands are run with `-f` from the root of
the repository. The module needs a local `4.0.0-SNAPSHOT` build of TinkerPop. Building with `mvn clean install` from
the repository root provides the JVM modules and the Gremlin Server Docker image that the integration tests use.

The unit tests need only the Dart SDK:

```bash
cd tinkubator/gremlin-dart
dart pub get
dart test test/unit/
```

The feature tests run the scenarios from `gremlin-test` against a running Gremlin Server. They use the traversals in
`test/feature/gremlin.dart`, which is generated from the `.feature` files by `build/generate.groovy` and the
`DartTranslateVisitor` in `src/main/java`. After changing the feature files or the translator, regenerate it:

```bash
mvn generate-test-sources -f tinkubator/gremlin-dart/pom.xml
```

To run all of the tests, including the feature tests and the example, against Gremlin Server in Docker:

```bash
mvn verify -f tinkubator/gremlin-dart/pom.xml
```

This starts a test server on port 45940 with `docker compose` and runs the unit tests, the feature tests and
`example/basic.dart` in a `dart:stable` container. Do not run it at the same time as another GLV's integration tests,
because they all use the same ports and the tests change the server's graphs.

To run the feature tests against a server you started yourself, set `GREMLIN_SERVER_URL`:

```bash
GREMLIN_SERVER_URL=http://localhost:45940/gremlin dart test test/feature/ --timeout=120s
```

If `GREMLIN_SERVER_URL` is not set and no server is listening on `http://localhost:45940/gremlin`, the feature tests
are reported as skipped.

[tk]: https://tinkerpop.apache.org
[gremlin]: https://tinkerpop.apache.org/gremlin.html
[gs]: https://tinkerpop.apache.org/docs/current/reference/#gremlin-server
