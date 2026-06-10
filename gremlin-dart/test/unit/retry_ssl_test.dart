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

import 'dart:io';
import 'dart:typed_data';

import 'package:dio/dio.dart';
import 'package:test/test.dart';

import '../../lib/driver/connection.dart';
import '../../lib/driver/cluster.dart';

void main() {
  // -------------------------------------------------------------------------
  // RetryOptions
  // -------------------------------------------------------------------------
  group('RetryOptions defaults', () {
    test('maxAttempts=3, delay=500ms, no exponential backoff', () {
      const opts = RetryOptions();
      expect(opts.maxAttempts, 3);
      expect(opts.delay, const Duration(milliseconds: 500));
      expect(opts.useExponentialBackoff, isFalse);
      expect(opts.retryWhen, isNull);
    });

    test('custom values are stored', () {
      final opts = RetryOptions(
        maxAttempts: 5,
        delay: const Duration(seconds: 2),
        useExponentialBackoff: true,
        retryWhen: (e) => e.type == DioExceptionType.connectionError,
      );
      expect(opts.maxAttempts, 5);
      expect(opts.delay, const Duration(seconds: 2));
      expect(opts.useExponentialBackoff, isTrue);
      expect(opts.retryWhen, isNotNull);
    });
  });

  // -------------------------------------------------------------------------
  // RetryInterceptor behaviour  (uses a fake HttpClientAdapter)
  // -------------------------------------------------------------------------
  group('RetryInterceptor', () {
    // Builds a Dio instance with a fake adapter and RetryInterceptor wired.
    // The fake adapter fails [failCount] times, then succeeds.
    ({Dio dio, _FakeAdapter adapter}) _build(
      int failCount,
      RetryOptions opts,
    ) {
      final dio = Dio(BaseOptions(validateStatus: (_) => true));
      final adapter = _FakeAdapter(failCount);
      dio.httpClientAdapter = adapter;
      dio.interceptors.add(RetryInterceptor(dio, opts));
      return (dio: dio, adapter: adapter);
    }

    test('retries on connection error and succeeds on third attempt', () async {
      final (:dio, :adapter) = _build(
        2,
        RetryOptions(maxAttempts: 3, delay: Duration.zero),
      );
      await dio.get('http://x/');
      expect(adapter.calls, 3);
      dio.close(force: true);
    });

    test('stops retrying after maxAttempts', () async {
      final (:dio, :adapter) = _build(
        10,
        RetryOptions(maxAttempts: 2, delay: Duration.zero),
      );
      try {
        await dio.get('http://x/');
      } catch (_) {}
      expect(adapter.calls, 2);
      dio.close(force: true);
    });

    test('no retry when maxAttempts=1', () async {
      final (:dio, :adapter) = _build(
        10,
        RetryOptions(maxAttempts: 1, delay: Duration.zero),
      );
      try {
        await dio.get('http://x/');
      } catch (_) {}
      expect(adapter.calls, 1);
      dio.close(force: true);
    });

    test('custom retryWhen predicate controls which errors are retried', () {
      final opts = RetryOptions(
        retryWhen: (e) => e.type == DioExceptionType.connectionTimeout,
      );
      final dio = Dio();
      final interceptor = RetryInterceptor(dio, opts);

      // Connection errors should NOT be retried by this custom predicate
      final connErr = DioException(
        requestOptions: RequestOptions(path: '/'),
        type: DioExceptionType.connectionError,
      );
      expect(interceptor.shouldRetry(connErr), isFalse);

      // Timeouts SHOULD be retried
      final timeoutErr = DioException(
        requestOptions: RequestOptions(path: '/'),
        type: DioExceptionType.connectionTimeout,
      );
      expect(interceptor.shouldRetry(timeoutErr), isTrue);
      dio.close(force: true);
    });

    test('default policy retries connection errors and timeouts', () {
      const opts = RetryOptions();
      final dio = Dio();
      final interceptor = RetryInterceptor(dio, opts);

      for (final type in [
        DioExceptionType.connectionError,
        DioExceptionType.connectionTimeout,
        DioExceptionType.receiveTimeout,
        DioExceptionType.sendTimeout,
      ]) {
        final err = DioException(
            requestOptions: RequestOptions(path: '/'), type: type);
        expect(interceptor.shouldRetry(err), isTrue,
            reason: '$type should be retried');
      }

      // 4xx errors should NOT be retried
      final clientErr = DioException(
        requestOptions: RequestOptions(path: '/'),
        type: DioExceptionType.badResponse,
        response: Response(
          requestOptions: RequestOptions(path: '/'),
          statusCode: 400,
        ),
      );
      expect(interceptor.shouldRetry(clientErr), isFalse);
      dio.close(force: true);
    });

    test('503 is retried by default policy', () {
      const opts = RetryOptions();
      final dio = Dio();
      final interceptor = RetryInterceptor(dio, opts);

      final err = DioException(
        requestOptions: RequestOptions(path: '/'),
        type: DioExceptionType.badResponse,
        response: Response(
          requestOptions: RequestOptions(path: '/'),
          statusCode: 503,
        ),
      );
      expect(interceptor.shouldRetry(err), isTrue);
      dio.close(force: true);
    });
  });

  // -------------------------------------------------------------------------
  // SslOptions
  // -------------------------------------------------------------------------
  group('SslOptions', () {
    test('default: no verification skip, no certs', () {
      const opts = SslOptions();
      expect(opts.skipCertificateVerification, isFalse);
      expect(opts.trustedCertificates, isNull);
      expect(opts.clientCertificate, isNull);
      expect(opts.privateKey, isNull);
    });

    test('skipCertificateVerification flag is stored', () {
      const opts = SslOptions(skipCertificateVerification: true);
      expect(opts.skipCertificateVerification, isTrue);
    });

    test('custom CA bytes are stored', () {
      final pem = Uint8List.fromList([0x2D, 0x2D, 0x2D]); // "---"
      final opts = SslOptions(trustedCertificates: pem);
      expect(opts.trustedCertificates, equals(pem));
    });

    test('path alternatives are stored', () {
      const opts = SslOptions(
        trustedCertificatesPath: '/etc/ssl/ca.pem',
        clientCertificatePath: '/etc/ssl/client.pem',
        privateKeyPath: '/etc/ssl/client.key',
        privateKeyPassword: 's3cret',
      );
      expect(opts.trustedCertificatesPath, '/etc/ssl/ca.pem');
      expect(opts.clientCertificatePath, '/etc/ssl/client.pem');
      expect(opts.privateKeyPath, '/etc/ssl/client.key');
      expect(opts.privateKeyPassword, 's3cret');
    });

    test('ConnectionOptions carries ssl field', () {
      const opts = ConnectionOptions(ssl: SslOptions(skipCertificateVerification: true));
      expect(opts.ssl?.skipCertificateVerification, isTrue);
    });

    test('copyWith propagates ssl field', () {
      const base = ConnectionOptions();
      final copy = base.copyWith(ssl: const SslOptions(skipCertificateVerification: true));
      expect(copy.ssl?.skipCertificateVerification, isTrue);
      expect(copy.traversalSource, 'g');
    });
  });

  // -------------------------------------------------------------------------
  // ClusterBuilder ssl/retry helpers
  // -------------------------------------------------------------------------
  group('ClusterBuilder ssl/retry', () {
    test('ssl() sets options.ssl on cluster', () {
      final cluster = Cluster.build()
          .ssl(const SslOptions(skipCertificateVerification: true))
          .create();
      expect(cluster.hosts, isNotEmpty);
      cluster.close();
    });

    test('retry() sets options.retryOptions on cluster', () {
      final cluster = Cluster.build()
          .retry(const RetryOptions(maxAttempts: 5))
          .create();
      expect(cluster.hosts, isNotEmpty);
      cluster.close();
    });

    test('ssl and retry can be chained together', () {
      final cluster = Cluster.build()
          .addContactPoint('myhost')
          .ssl(const SslOptions(skipCertificateVerification: true))
          .retry(const RetryOptions(maxAttempts: 2, delay: Duration(seconds: 1)))
          .enableSsl(true)
          .create();
      expect(cluster.toString(), contains('https://myhost'));
      cluster.close();
    });
  });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Fake [HttpClientAdapter] that throws [DioExceptionType.connectionError]
/// for the first [failCount] calls, then returns an empty 200 response.
class _FakeAdapter implements HttpClientAdapter {
  final int failCount;
  int _calls = 0;

  _FakeAdapter(this.failCount);

  int get calls => _calls;

  @override
  Future<ResponseBody> fetch(
    RequestOptions options,
    Stream<Uint8List>? requestStream,
    Future<dynamic>? cancelFuture,
  ) async {
    _calls++;
    if (_calls <= failCount) {
      throw DioException(
        requestOptions: options,
        type: DioExceptionType.connectionError,
        error: const SocketException('Connection refused (fake)'),
      );
    }
    return ResponseBody.fromBytes(Uint8List(0), 200);
  }

  @override
  void close({bool force = false}) {}
}
