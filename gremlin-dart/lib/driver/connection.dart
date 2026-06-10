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
import 'dart:io' as io;
import 'dart:typed_data';

import 'package:dio/dio.dart';

import '../structure/io/graph_binary/graph_binary_reader.dart';
import '../structure/io/graph_binary/graph_binary_writer.dart';
import 'auth.dart';
import 'request_message.dart';
import 'sigv4.dart';
import 'response_error.dart';
import 'result_set.dart';

// ---------------------------------------------------------------------------
// SslOptions
// ---------------------------------------------------------------------------

/// TLS/SSL configuration for a [Connection].
///
/// Pass bytes (in-memory PEM) or a file path for each credential.  Bytes take
/// precedence when both are supplied for the same field.
///
/// ```dart
/// // Self-signed server cert — skip verification (dev only!)
/// SslOptions(skipCertificateVerification: true)
///
/// // Trust a private CA
/// SslOptions(trustedCertificates: File('ca.pem').readAsBytesSync())
///
/// // Mutual TLS
/// SslOptions(
///   clientCertificate: File('client.pem').readAsBytesSync(),
///   privateKey:        File('client.key').readAsBytesSync(),
///   privateKeyPassword: 'secret',
/// )
/// ```
class SslOptions {
  /// Skip TLS certificate verification entirely.  **For testing only.**
  final bool skipCertificateVerification;

  /// PEM-encoded trusted CA certificate(s) — bytes.
  final Uint8List? trustedCertificates;

  /// Path to PEM-encoded trusted CA certificate(s) file.
  final String? trustedCertificatesPath;

  /// PEM-encoded client certificate chain (bytes) — for mutual TLS.
  final Uint8List? clientCertificate;

  /// Path to PEM-encoded client certificate chain file.
  final String? clientCertificatePath;

  /// PEM-encoded private key (bytes) — for mutual TLS.
  final Uint8List? privateKey;

  /// Path to PEM-encoded private key file.
  final String? privateKeyPath;

  /// Password for the private key (if encrypted).
  final String? privateKeyPassword;

  const SslOptions({
    this.skipCertificateVerification = false,
    this.trustedCertificates,
    this.trustedCertificatesPath,
    this.clientCertificate,
    this.clientCertificatePath,
    this.privateKey,
    this.privateKeyPath,
    this.privateKeyPassword,
  });
}

// ---------------------------------------------------------------------------
// RetryOptions  (configured per-Connection; wires a RetryInterceptor into Dio)
// ---------------------------------------------------------------------------

/// Controls automatic retry behaviour on transient errors.
///
/// By default retries connection errors, timeouts, and 503 responses up to
/// 3 times with a 500 ms fixed delay.
///
/// ```dart
/// ConnectionOptions(
///   retryOptions: RetryOptions(
///     maxAttempts: 5,
///     delay: Duration(seconds: 1),
///     useExponentialBackoff: true,
///   ),
/// )
/// ```
class RetryOptions {
  /// Total number of attempts (first try + retries).  Must be ≥ 1.
  final int maxAttempts;

  /// Base delay between attempts.  Doubles on each attempt when
  /// [useExponentialBackoff] is true.
  final Duration delay;

  /// Double the delay on each successive retry.
  final bool useExponentialBackoff;

  /// Custom predicate — return true to retry the given error.  When null the
  /// default policy applies (connection errors, timeouts, HTTP 503).
  final bool Function(DioException)? retryWhen;

  const RetryOptions({
    this.maxAttempts = 3,
    this.delay = const Duration(milliseconds: 500),
    this.useExponentialBackoff = false,
    this.retryWhen,
  });
}

class ConnectionOptions {
  final bool enableUserAgentOnConnect;
  final Map<String, String> headers;
  final String traversalSource;
  final AuthOptions? auth;
  final List<Interceptor> interceptors;
  final Duration connectTimeout;
  final Duration receiveTimeout;
  final Duration idleTimeout;
  final int maxConnectionsPerHost;
  // Provide a custom adapter to override SSL, proxy, or transport behaviour.
  final HttpClientAdapter? httpClientAdapter;
  final SslOptions? ssl;
  final RetryOptions? retryOptions;

  const ConnectionOptions({
    this.enableUserAgentOnConnect = true,
    this.headers = const {},
    this.traversalSource = 'g',
    this.auth,
    this.interceptors = const [],
    this.connectTimeout = const Duration(seconds: 30),
    this.receiveTimeout = const Duration(seconds: 30),
    this.idleTimeout = const Duration(seconds: 30),
    this.maxConnectionsPerHost = 8,
    this.httpClientAdapter,
    this.ssl,
    this.retryOptions,
  });

  ConnectionOptions copyWith({
    bool? enableUserAgentOnConnect,
    Map<String, String>? headers,
    String? traversalSource,
    AuthOptions? auth,
    List<Interceptor>? interceptors,
    Duration? connectTimeout,
    Duration? receiveTimeout,
    Duration? idleTimeout,
    int? maxConnectionsPerHost,
    HttpClientAdapter? httpClientAdapter,
    SslOptions? ssl,
    RetryOptions? retryOptions,
  }) =>
      ConnectionOptions(
        enableUserAgentOnConnect:
            enableUserAgentOnConnect ?? this.enableUserAgentOnConnect,
        headers: headers ?? this.headers,
        traversalSource: traversalSource ?? this.traversalSource,
        auth: auth ?? this.auth,
        interceptors: interceptors ?? this.interceptors,
        connectTimeout: connectTimeout ?? this.connectTimeout,
        receiveTimeout: receiveTimeout ?? this.receiveTimeout,
        idleTimeout: idleTimeout ?? this.idleTimeout,
        maxConnectionsPerHost:
            maxConnectionsPerHost ?? this.maxConnectionsPerHost,
        httpClientAdapter: httpClientAdapter ?? this.httpClientAdapter,
        ssl: ssl ?? this.ssl,
        retryOptions: retryOptions ?? this.retryOptions,
      );
}

class _RawResponse {
  final int statusCode;
  final String? contentType;
  final String? transactionId;
  final Uint8List bodyBytes;
  const _RawResponse(
      this.statusCode, this.contentType, this.transactionId, this.bodyBytes);
}

class Connection {
  static const String transactionIdHeader = 'X-Transaction-Id';
  static const String _transactionIdHeaderLower = 'x-transaction-id';

  final String url;
  final ConnectionOptions options;
  final GraphBinaryReader _reader;
  final GraphBinaryWriter _writer;
  late final Dio _dio;

  bool isOpen = true;

  Connection(this.url, [ConnectionOptions? options])
      : options = options ?? const ConnectionOptions(),
        _reader = GraphBinaryReader(),
        _writer = GraphBinaryWriter() {
    _dio = Dio(BaseOptions(
      connectTimeout: this.options.connectTimeout,
      receiveTimeout: this.options.receiveTimeout,
      // Let our own _handleResponse deal with non-2xx status codes.
      validateStatus: (_) => true,
    ));

    _dio.httpClientAdapter = this.options.httpClientAdapter ??
        _TrailerTolerantAdapter(
          idleTimeout: this.options.idleTimeout,
          connectTimeout: this.options.connectTimeout,
          maxConnectionsPerHost: this.options.maxConnectionsPerHost,
          ssl: this.options.ssl,
        );

    for (final interceptor in this.options.interceptors) {
      _dio.interceptors.add(interceptor);
    }

    // SigV4 runs after user interceptors but before Retry so each attempt
    // (including retries) gets a fresh signature and timestamp.
    if (this.options.auth is SigV4Auth) {
      _dio.interceptors.add(SigV4Interceptor(this.options.auth as SigV4Auth));
    }

    // Retry interceptor is added last so it wraps the full request pipeline.
    if (this.options.retryOptions != null) {
      _dio.interceptors.add(RetryInterceptor(_dio, this.options.retryOptions!));
    }
  }

  Future<void> open() async {}

  Future<ResultSet<dynamic>> submit(RequestMessage request) async {
    final body = _writer.writeRequest(request);
    final response = await _makeHttpRequest(request, body);
    return _handleResponse(response);
  }

  Stream<dynamic> stream(RequestMessage request) async* {
    final body = _writer.writeRequest(request);
    final response = await _makeHttpRequest(request, body);
    yield* _streamResponse(response);
  }

  Future<_RawResponse> _makeHttpRequest(
      RequestMessage request, Uint8List body) async {
    final reqHeaders = <String, String>{
      'Content-Type': _writer.mimeType,
      'Accept': _reader.mimeType,
    };

    if (options.enableUserAgentOnConnect) {
      reqHeaders['x-gremlin-useragent'] = _userAgent();
    }
    reqHeaders.addAll(options.headers);
    if (options.auth is BasicAuth) {
      reqHeaders['Authorization'] = (options.auth as BasicAuth).headerValue;
    }
    if (request.transactionId != null) {
      reqHeaders[transactionIdHeader] = request.transactionId!;
    }

    final response = await _dio.post<Uint8List>(
      url,
      data: body,
      options: Options(
        headers: reqHeaders,
        responseType: ResponseType.bytes,
        // sendTimeout per-request if needed in future
      ),
    );

    final statusCode = response.statusCode ?? 0;
    final contentType = response.headers['content-type']?.firstOrNull;
    final transactionId =
        response.headers.value(_transactionIdHeaderLower);
    final bytes = response.data ?? Uint8List(0);

    return _RawResponse(statusCode, contentType, transactionId, bytes);
  }

  Future<ResultSet<dynamic>> _handleResponse(_RawResponse response) async {
    if (response.statusCode < 200 || response.statusCode >= 300) {
      await _throwResponseError(
        response.statusCode,
        response.contentType,
        response.bodyBytes,
        'HTTP ${response.statusCode}',
      );
    }

    if (response.bodyBytes.isEmpty) return ResultSet<dynamic>([]);

    final deserialized = await _reader.readResponse(response.bodyBytes);

    if (deserialized['status'] != null) {
      final code = deserialized['status']['code'] as int?;
      if (code != null && code != 200 && code != 204 && code != 206) {
        throw ResponseError(
          'Server error (code $code)',
          statusCode: code,
          serverMessage: deserialized['status']['message'] as String?,
          exception: deserialized['status']['exception'] as String?,
        );
      }
    }

    final result = deserialized['result'];
    final bulked = result['bulked'] as bool? ?? false;
    final data = result['data'] as List? ?? [];

    final items = bulked
        ? data.expand((item) {
            final bulk = (item['bulk'] as int?) ?? 1;
            return List.filled(bulk, item['v']);
          }).toList()
        : data;

    return ResultSet<dynamic>(items, {
      if (response.transactionId != null)
        'transactionId': response.transactionId,
    });
  }

  Stream<dynamic> _streamResponse(_RawResponse response) async* {
    if (response.statusCode < 200 || response.statusCode >= 300) {
      await _throwResponseError(
        response.statusCode,
        response.contentType,
        response.bodyBytes,
        'HTTP ${response.statusCode}',
      );
    }
    if (response.bodyBytes.isEmpty) return;
    yield* _reader.readResponseStream(Stream.value(response.bodyBytes));
  }

  Future<void> _throwResponseError(int statusCode, String? contentType,
      Uint8List body, String reasonPhrase) async {
    final message = 'Server returned HTTP $statusCode: $reasonPhrase';
    try {
      if (contentType != null && contentType.startsWith(_reader.mimeType)) {
        final decoded = await _reader.readResponse(body);
        final status = decoded['status'] as Map<String, dynamic>?;
        throw ResponseError(
          message,
          statusCode: statusCode,
          serverMessage: status?['message'] as String? ?? reasonPhrase,
          exception: status?['exception'] as String?,
        );
      }
      final decoded = jsonDecode(utf8.decode(body)) as Map<String, dynamic>;
      final status = decoded['status'] as Map<String, dynamic>?;
      throw ResponseError(
        message,
        statusCode: statusCode,
        serverMessage: status?['message'] as String? ??
            decoded['message'] as String? ??
            decoded['error'] as String? ??
            reasonPhrase,
      );
    } catch (e) {
      if (e is ResponseError) rethrow;
      throw ResponseError(message, statusCode: statusCode);
    }
  }

  Future<void> close() async {
    isOpen = false;
    _dio.close(force: true);
  }

  static String _userAgent() => 'gremlin-dart/0.1.0 Dart/unknown';
}

// ---------------------------------------------------------------------------
// Custom HTTP adapter — wraps dart:io so we can tolerate the non-standard
// HTTP trailers that TinkerPop's Netty server appends after the final 0\r\n
// chunk.  Dart's built-in HTTP parser throws HttpException when it sees those
// trailer bytes; we catch it (after the body is already fully buffered) and
// fall back to manual chunked-encoding decoding when needed.
// ---------------------------------------------------------------------------
class _TrailerTolerantAdapter implements HttpClientAdapter {
  final io.HttpClient _client;

  _TrailerTolerantAdapter({
    required Duration idleTimeout,
    required Duration connectTimeout,
    required int maxConnectionsPerHost,
    SslOptions? ssl,
  }) : _client = _buildClient(idleTimeout, connectTimeout,
            maxConnectionsPerHost, ssl);

  static io.HttpClient _buildClient(
    Duration idleTimeout,
    Duration connectTimeout,
    int maxConnectionsPerHost,
    SslOptions? ssl,
  ) {
    io.SecurityContext? ctx;

    if (ssl != null) {
      final needsCtx = ssl.trustedCertificates != null ||
          ssl.trustedCertificatesPath != null ||
          ssl.clientCertificate != null ||
          ssl.clientCertificatePath != null ||
          ssl.privateKey != null ||
          ssl.privateKeyPath != null;

      if (needsCtx) {
        ctx = io.SecurityContext(withTrustedRoots: true);

        if (ssl.trustedCertificates != null) {
          ctx.setTrustedCertificatesBytes(ssl.trustedCertificates!);
        } else if (ssl.trustedCertificatesPath != null) {
          ctx.setTrustedCertificates(ssl.trustedCertificatesPath!);
        }

        if (ssl.clientCertificate != null) {
          ctx.useCertificateChainBytes(ssl.clientCertificate!);
        } else if (ssl.clientCertificatePath != null) {
          ctx.useCertificateChain(ssl.clientCertificatePath!);
        }

        if (ssl.privateKey != null) {
          ctx.usePrivateKeyBytes(ssl.privateKey!,
              password: ssl.privateKeyPassword);
        } else if (ssl.privateKeyPath != null) {
          ctx.usePrivateKey(ssl.privateKeyPath!,
              password: ssl.privateKeyPassword);
        }
      }
    }

    final client =
        ctx != null ? io.HttpClient(context: ctx) : io.HttpClient();

    client
      ..idleTimeout = idleTimeout
      ..connectionTimeout = connectTimeout
      ..maxConnectionsPerHost = maxConnectionsPerHost;

    if (ssl?.skipCertificateVerification == true) {
      client.badCertificateCallback = (_, __, ___) => true;
    }

    return client;
  }

  @override
  Future<ResponseBody> fetch(
    RequestOptions options,
    Stream<Uint8List>? requestStream,
    Future<dynamic>? cancelFuture,
  ) async {
    final ioReq = await _client.openUrl(options.method, options.uri);
    options.headers.forEach((name, value) {
      if (value != null) ioReq.headers.set(name, value.toString());
    });

    if (requestStream != null) {
      await requestStream.forEach(ioReq.add);
    }
    final ioResp = await ioReq.close();

    final bodyBytes = BytesBuilder(copy: false);
    bool trailerException = false;
    try {
      await for (final chunk in ioResp) {
        bodyBytes.add(chunk);
      }
    } on io.HttpException catch (_) {
      if (bodyBytes.isEmpty) rethrow;
      trailerException = true;
    } on StateError catch (_) {
      if (bodyBytes.isEmpty) rethrow;
      trailerException = true;
    }

    final raw = bodyBytes.takeBytes();
    final decoded = trailerException ? _decodeChunked(raw) : raw;

    final headersMap = <String, List<String>>{};
    ioResp.headers.forEach((name, values) => headersMap[name] = values);

    return ResponseBody.fromBytes(
      decoded,
      ioResp.statusCode,
      headers: headersMap,
    );
  }

  @override
  void close({bool force = false}) => _client.close(force: force);

  // Decodes HTTP chunked transfer encoding manually. When dart:io throws an
  // HttpException due to trailing headers, the stream may yield raw wire bytes
  // (chunk-size CRLF chunk-data CRLF ... 0 CRLF) instead of decoded payload.
  // If the buffer doesn't look like chunked encoding, return it unchanged.
  static Uint8List _decodeChunked(Uint8List raw) {
    if (raw.isEmpty) return raw;
    final first = raw[0];
    final isHex = (first >= 0x30 && first <= 0x39) ||
        (first >= 0x41 && first <= 0x46) ||
        (first >= 0x61 && first <= 0x66);
    if (!isHex) return raw;

    final out = BytesBuilder();
    int pos = 0;
    while (pos < raw.length) {
      int crPos = pos;
      while (crPos < raw.length - 1 &&
          !(raw[crPos] == 0x0D && raw[crPos + 1] == 0x0A)) {
        crPos++;
      }
      if (crPos >= raw.length - 1) break;

      final sizeHex = String.fromCharCodes(raw.sublist(pos, crPos));
      final chunkSize = int.tryParse(sizeHex.trim(), radix: 16);
      if (chunkSize == null) return raw;
      if (chunkSize == 0) break;

      pos = crPos + 2;
      if (pos + chunkSize > raw.length) {
        out.add(raw.sublist(pos));
        break;
      }
      out.add(raw.sublist(pos, pos + chunkSize));
      pos += chunkSize + 2;
    }

    final result = out.takeBytes();
    return result.isEmpty ? raw : result;
  }
}

// ---------------------------------------------------------------------------
// RetryInterceptor
// ---------------------------------------------------------------------------

/// Dio interceptor that transparently retries requests on transient errors.
/// Added automatically by [Connection] when [ConnectionOptions.retryOptions]
/// is non-null.
class RetryInterceptor extends Interceptor {
  final Dio _dio;
  final RetryOptions options;

  RetryInterceptor(this._dio, this.options);

  @override
  Future<void> onError(
      DioException err, ErrorInterceptorHandler handler) async {
    final attempt = (err.requestOptions.extra['_attempt'] as int?) ?? 0;
    if (attempt < options.maxAttempts - 1 && _shouldRetry(err)) {
      final wait = options.useExponentialBackoff
          ? options.delay * (1 << attempt)
          : options.delay;
      await Future<void>.delayed(wait);
      try {
        final cloned = err.requestOptions.copyWith(
          extra: {...err.requestOptions.extra, '_attempt': attempt + 1},
        );
        handler.resolve(await _dio.fetch<dynamic>(cloned));
      } catch (e) {
        handler.next(e is DioException ? e : err);
      }
      return;
    }
    handler.next(err);
  }

  bool shouldRetry(DioException err) => _shouldRetry(err);

  bool _shouldRetry(DioException err) {
    if (options.retryWhen != null) return options.retryWhen!(err);
    return err.type == DioExceptionType.connectionError ||
        err.type == DioExceptionType.connectionTimeout ||
        err.type == DioExceptionType.receiveTimeout ||
        err.type == DioExceptionType.sendTimeout ||
        (err.response?.statusCode == 503);
  }
}
