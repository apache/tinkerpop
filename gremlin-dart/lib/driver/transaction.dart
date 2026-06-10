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

import '../process/graph_traversal.dart';
import '../process/gremlin_lang.dart';
import '../process/traversal.dart';
import '../process/traversal_strategy.dart';
import '../structure/graph.dart';
import 'dart:async';

import 'client.dart';
import 'driver_remote_connection.dart';
import 'result_set.dart';

enum _TransactionState { notStarted, open, closed }

enum TransactionStatus { commit, rollback }

enum TransactionCloseBehavior {
  commit,
  rollback,
  manual,
}

class Transaction {
  final DriverRemoteConnection _transactionConnection;
  _TransactionState _state = _TransactionState.notStarted;
  String? _transactionId;
  Future<void> _pending = Future.value();
  TransactionCloseBehavior _closeBehavior = TransactionCloseBehavior.commit;

  Transaction(this._transactionConnection);

  bool get isOpen => _state == _TransactionState.open;

  String? get transactionId => _transactionId;

  Future<void> open() async {
    await begin();
  }

  Future<GraphTraversalSource> begin() async {
    return _enqueue(() async {
      if (_state != _TransactionState.notStarted) {
        throw StateError('Transaction already started');
      }

      try {
        final rs = await _transactionConnection.submitAsync('g.tx().begin()');
        _transactionId = _extractTransactionId(rs);
        _state = _TransactionState.open;
      } catch (_) {
        _state = _TransactionState.closed;
        _transactionId = null;
        await _transactionConnection.close();
        rethrow;
      }

      final strategies = TraversalStrategies();
      final txConnection = _TransactionRemoteConnection(this);
      strategies.addStrategy(RemoteStrategy(txConnection));
      return GraphTraversalSource(
        Graph(),
        strategies,
        GremlinLang(),
        txConnection,
        _transactionConnection.options.traversalSource,
      );
    });
  }

  Future<void> commit() async {
    await _enqueue(() async {
      final transactionId = _requireTransactionId();
      await _transactionConnection.submitAsync(
        'g.tx().commit()',
        requestOptions: RequestOptions(transactionId: transactionId),
      );
      await _closeState();
    });
  }

  Future<void> rollback() async {
    await _enqueue(() async {
      final transactionId = _requireTransactionId();
      await _transactionConnection.submitAsync(
        'g.tx().rollback()',
        requestOptions: RequestOptions(transactionId: transactionId),
      );
      await _closeState();
    });
  }

  Future<void> close() async {
    switch (_closeBehavior) {
      case TransactionCloseBehavior.commit:
        if (isOpen) await commit();
      case TransactionCloseBehavior.rollback:
        if (isOpen) await rollback();
      case TransactionCloseBehavior.manual:
        if (isOpen) {
          throw StateError(
              'Commit or rollback all outstanding transactions before closing the transaction');
        }
    }
  }

  void readWrite() {
    throw UnsupportedError(
        'Remote transaction behaviors are not configurable - they are always manually controlled');
  }

  Transaction onReadWrite(void Function(Transaction) consumer) {
    throw UnsupportedError(
        'Remote transaction behaviors are not configurable - they are always manually controlled');
  }

  Transaction onClose(TransactionCloseBehavior behavior) {
    _closeBehavior = behavior;
    return this;
  }

  void addTransactionListener(void Function(TransactionStatus) listener) {
    throw UnsupportedError(
        'Remote transactions cannot have listeners attached');
  }

  void removeTransactionListener(void Function(TransactionStatus) listener) {
    throw UnsupportedError(
        'Remote transactions cannot have listeners attached');
  }

  void clearTransactionListeners() {
    throw UnsupportedError(
        'Remote transactions cannot have listeners attached');
  }

  Future<RemoteTraversal> submit(GremlinLang gremlinLang) async {
    return _enqueue(() async {
      final transactionId = _requireTransactionId();
      final rs =
          await _transactionConnection.submitInTransactionBuffered(
              gremlinLang, transactionId);
      return RemoteTraversal(Stream<dynamic>.fromIterable(rs.items));
    });
  }

  String _extractTransactionId(ResultSet<dynamic> rs) {
    final headerTransactionId = rs.attributes['transactionId'];
    if (headerTransactionId is String && headerTransactionId.isNotEmpty) {
      return headerTransactionId;
    }

    if (rs.isEmpty) {
      throw StateError('Server did not return transactionId');
    }

    final first = rs[0];
    if (first is! Map) {
      throw StateError('Server did not return transactionId');
    }

    final transactionId = first['transactionId'];
    if (transactionId is! String || transactionId.isEmpty) {
      throw StateError('Server did not return transactionId');
    }

    return transactionId;
  }

  String _requireTransactionId() {
    if (!isOpen || _transactionId == null) {
      throw StateError('Transaction is not open');
    }
    return _transactionId!;
  }

  Future<void> _closeState() async {
    _state = _TransactionState.closed;
    _transactionId = null;
    await _transactionConnection.close();
  }

  Future<T> _enqueue<T>(Future<T> Function() action) {
    final completer = Completer<T>();
    final previous = _pending;
    _pending = () async {
      try {
        await previous;
      } catch (_) {
        // Preserve sequential submission even after a failed operation.
      }

      try {
        completer.complete(await action());
      } catch (e, st) {
        completer.completeError(e, st);
      }
    }();
    return completer.future;
  }
}

class _TransactionRemoteConnection
    implements TransactionCapableRemoteConnectionBase {
  final Transaction _transaction;

  _TransactionRemoteConnection(this._transaction);

  @override
  Future<RemoteTraversal> submit(GremlinLang gremlinLang) =>
      _transaction.submit(gremlinLang);

  Transaction tx([String? traversalSource]) => _transaction;
}
