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
import 'response_error.dart';
import 'result_set.dart';

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
  });
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
        );

    for (final interceptor in this.options.interceptors) {
      _dio.interceptors.add(interceptor);
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
  }) : _client = io.HttpClient()
          ..idleTimeout = idleTimeout
          ..connectionTimeout = connectTimeout
          ..maxConnectionsPerHost = maxConnectionsPerHost;

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
