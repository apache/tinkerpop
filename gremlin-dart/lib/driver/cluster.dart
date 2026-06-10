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
import 'dart:math';

import '../process/gremlin_lang.dart';
import '../process/traversal.dart';
import '../process/traversal_strategy.dart';
import 'connection.dart';
import 'driver_remote_connection.dart';
import 'remote_connection.dart';
import 'request_message.dart';
import 'transaction.dart';

// ---------------------------------------------------------------------------
// Exception
// ---------------------------------------------------------------------------

class NoHostAvailableException implements Exception {
  final String message;

  NoHostAvailableException([
    this.message =
        'All hosts are considered unavailable due to previous errors.',
  ]);

  @override
  String toString() => 'NoHostAvailableException: $message';
}

// ---------------------------------------------------------------------------
// HostEntry — one server node in the cluster
// ---------------------------------------------------------------------------

class HostEntry {
  final String url;
  bool isAvailable = true;
  Connection? _connection;
  Timer? _reconnectTimer;

  HostEntry(this.url);

  Connection open(ConnectionOptions opts) =>
      _connection ??= Connection(url, opts);

  void reset() {
    _connection?.close();
    _connection = null;
  }

  void dispose() {
    _reconnectTimer?.cancel();
    _reconnectTimer = null;
    _connection?.close();
    _connection = null;
  }

  @override
  String toString() => 'HostEntry($url, available=$isAvailable)';
}

// ---------------------------------------------------------------------------
// LoadBalancingStrategy
// ---------------------------------------------------------------------------

abstract class LoadBalancingStrategy {
  void initialize(List<HostEntry> hosts);

  /// Returns the next host to try, skipping any in [exclude].
  HostEntry? select({Set<HostEntry>? exclude});

  void onAvailable(HostEntry host);
  void onUnavailable(HostEntry host);
}

/// Simple round-robin over available hosts, with a random start index so
/// multiple clients in the same process don't all start at host 0.
class RoundRobin implements LoadBalancingStrategy {
  final _hosts = <HostEntry>[];
  int _index = 0;

  @override
  void initialize(List<HostEntry> hosts) {
    _hosts.addAll(hosts);
    _index = hosts.isEmpty ? 0 : Random().nextInt(hosts.length);
  }

  @override
  HostEntry? select({Set<HostEntry>? exclude}) {
    final available = _hosts
        .where((h) => h.isAvailable && (exclude == null || !exclude.contains(h)))
        .toList();
    if (available.isEmpty) return null;
    final i = _index % available.length;
    _index = (_index + 1) & 0x7FFFFFFF;
    return available[i];
  }

  @override
  void onAvailable(HostEntry host) {
    host.isAvailable = true;
    if (!_hosts.contains(host)) _hosts.add(host);
  }

  @override
  void onUnavailable(HostEntry host) {
    host.isAvailable = false;
  }
}

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

/// Manages a pool of Gremlin Server hosts with load balancing and automatic
/// reconnection.  Equivalent to Java's {@code Cluster} class.
///
/// ```dart
/// final cluster = Cluster.build()
///     .addContactPoints(['host1', 'host2'])
///     .port(8182)
///     .create();
///
/// final g = traversal().withRemote(cluster.connect());
/// final names = await g.V().values('name').toList<String>();
/// await cluster.close();
/// ```
class Cluster {
  final List<HostEntry> _hosts;
  final LoadBalancingStrategy _lb;
  final ConnectionOptions _baseOptions;
  final Duration _reconnectInterval;

  Cluster._({
    required List<HostEntry> hosts,
    required LoadBalancingStrategy lb,
    required ConnectionOptions baseOptions,
    required Duration reconnectInterval,
  })  : _hosts = hosts,
        _lb = lb,
        _baseOptions = baseOptions,
        _reconnectInterval = reconnectInterval {
    _lb.initialize(hosts);
  }

  static ClusterBuilder build() => ClusterBuilder();

  /// Shorthand: single contact point.
  static ClusterBuilder buildFrom(String address) =>
      ClusterBuilder()..addContactPoint(address);

  /// Returns a [ClusterRemoteConnection] that load-balances across all hosts.
  ClusterRemoteConnection connect([String? traversalSource]) =>
      ClusterRemoteConnection(this, traversalSource);

  // ---- internal submission helpers ----------------------------------------

  Stream<dynamic> _stream(RequestMessage request) async* {
    final tried = <HostEntry>{};
    Object? lastError;
    while (tried.length < _hosts.length) {
      final host = _lb.select(exclude: tried);
      if (host == null) break;
      tried.add(host);
      try {
        yield* host.open(_baseOptions).stream(request);
        return;
      } catch (e) {
        lastError = e;
        _markUnavailable(host);
      }
    }
    throw NoHostAvailableException(
        lastError?.toString() ?? 'All hosts are unavailable');
  }

  void _markUnavailable(HostEntry host) {
    _lb.onUnavailable(host);
    host.reset();
    // Cancel any existing reconnect attempt before scheduling a new one.
    host._reconnectTimer?.cancel();
    host._reconnectTimer = Timer.periodic(_reconnectInterval, (_) async {
      try {
        final probe = Connection(host.url, _baseOptions);
        await probe.submit(
          RequestMessage.build('g.inject(0)')
              .addG(_baseOptions.traversalSource)
              .addBulkResults(false)
              .create(),
        );
        await probe.close();
        host._reconnectTimer?.cancel();
        host._reconnectTimer = null;
        _lb.onAvailable(host);
      } catch (_) {
        // Host still unreachable; timer keeps firing.
      }
    });
  }

  /// Read-only view of all hosts (available and unavailable).
  List<HostEntry> get hosts => List.unmodifiable(_hosts);

  Future<void> close() async {
    for (final host in _hosts) {
      host.dispose();
    }
  }

  @override
  String toString() =>
      'Cluster(${_hosts.map((h) => h.url).join(', ')})';
}

// ---------------------------------------------------------------------------
// ClusterRemoteConnection
// ---------------------------------------------------------------------------

/// A [RemoteConnection] backed by a [Cluster].  Returned by
/// [Cluster.connect]; use it with [AnonymousTraversalSource.withRemote].
class ClusterRemoteConnection extends RemoteConnection
    implements TransactionCapableRemoteConnectionBase {
  final Cluster _cluster;
  final String? _traversalSource;

  ClusterRemoteConnection(this._cluster, [this._traversalSource])
      : super(_cluster._hosts.map((h) => h.url).join(','));

  String get _source =>
      _traversalSource ?? _cluster._baseOptions.traversalSource;

  /// The traversal source that will be used for requests from this connection.
  String get effectiveTraversalSource => _source;

  @override
  Future<void> open() async {}

  @override
  bool get isOpen => _cluster._hosts.any((h) => h.isAvailable);

  @override
  Future<RemoteTraversal> submit(GremlinLang gremlinLang) {
    final request = _buildRequest(gremlinLang, _source);
    return Future.value(RemoteTraversal(_cluster._stream(request)));
  }

  /// Pins this transaction to a single host for its entire lifetime.
  @override
  Transaction tx([String? traversalSource]) {
    final host = _cluster._lb.select();
    if (host == null) throw NoHostAvailableException();
    return Transaction(DriverRemoteConnection(
      host.url,
      _cluster._baseOptions.copyWith(
        traversalSource: traversalSource ?? _source,
      ),
    ));
  }

  @override
  Future<void> commit() async {}

  @override
  Future<void> rollback() async {}

  // The cluster manages its own connection lifecycle.
  @override
  Future<void> close() async {}
}

// ---------------------------------------------------------------------------
// ClusterBuilder
// ---------------------------------------------------------------------------

class ClusterBuilder {
  final _contactPoints = <String>[];
  int _port = 8182;
  String _path = '/gremlin';
  bool _enableSsl = false;
  Duration _reconnectInterval = const Duration(seconds: 1);
  LoadBalancingStrategy _lb = RoundRobin();
  ConnectionOptions _options = const ConnectionOptions();

  ClusterBuilder addContactPoint(String address) {
    _contactPoints.add(address);
    return this;
  }

  ClusterBuilder addContactPoints(List<String> addresses) {
    _contactPoints.addAll(addresses);
    return this;
  }

  ClusterBuilder port(int port) {
    _port = port;
    return this;
  }

  ClusterBuilder path(String path) {
    _path = path;
    return this;
  }

  /// Enable HTTPS.  For custom cert validation supply an [HttpClientAdapter]
  /// via [options] instead of (or in addition to) this flag.
  ClusterBuilder enableSsl(bool enable) {
    _enableSsl = enable;
    return this;
  }

  ClusterBuilder reconnectInterval(Duration interval) {
    _reconnectInterval = interval;
    return this;
  }

  ClusterBuilder loadBalancingStrategy(LoadBalancingStrategy strategy) {
    _lb = strategy;
    return this;
  }

  /// Base [ConnectionOptions] applied to every host (auth, timeouts,
  /// interceptors, SSL adapter, etc.).
  ClusterBuilder options(ConnectionOptions options) {
    _options = options;
    return this;
  }

  Cluster create() {
    final points = _contactPoints.isEmpty ? ['localhost'] : _contactPoints;
    final scheme = _enableSsl ? 'https' : 'http';
    final hosts =
        points.map((addr) => HostEntry('$scheme://$addr:$_port$_path')).toList();
    return Cluster._(
      hosts: hosts,
      lb: _lb,
      baseOptions: _options,
      reconnectInterval: _reconnectInterval,
    );
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Translates a [GremlinLang] traversal into a [RequestMessage], honouring
/// any OptionsStrategy hints embedded in the traversal (evaluationTimeout,
/// bulkResults, materializeProperties).
RequestMessage _buildRequest(GremlinLang gremlinLang, String traversalSource) {
  final strategies = gremlinLang.getOptionsStrategies();
  int? evalTimeout;
  bool bulkResults = true;
  String? materializeProperties;

  for (final s in strategies) {
    for (final entry in s.configuration.entries) {
      switch (entry.key) {
        case 'evaluationTimeout':
          evalTimeout = entry.value as int?;
        case 'bulkResults':
          bulkResults = (entry.value as bool?) ?? true;
        case 'materializeProperties':
          materializeProperties = entry.value as String?;
      }
    }
  }

  final builder = RequestMessage.build(gremlinLang.getGremlin())
      .addG(traversalSource)
      .addBulkResults(bulkResults);

  if (evalTimeout != null) builder.addTimeoutMillis(evalTimeout);
  if (materializeProperties != null) {
    builder.addMaterializeProperties(materializeProperties);
  }

  return builder.create();
}
