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

import '../structure/io/graph_binary/graph_binary_reader.dart';
import '../structure/io/graph_binary/graph_binary_writer.dart';
import 'auth.dart';
import 'request_message.dart';
import 'response_error.dart';
import 'result_set.dart';

typedef RequestInterceptor = Future<Map<String, dynamic>> Function(
    Map<String, dynamic> request);

class ConnectionOptions {
  final bool enableUserAgentOnConnect;
  final Map<String, String> headers;
  final String traversalSource;
  final AuthOptions? auth;
  final List<RequestInterceptor> interceptors;
  final Duration idleTimeout;
  final int maxConnectionsPerHost;

  const ConnectionOptions({
    this.enableUserAgentOnConnect = true,
    this.headers = const {},
    this.traversalSource = 'g',
    this.auth,
    this.interceptors = const [],
    this.idleTimeout = const Duration(seconds: 30),
    this.maxConnectionsPerHost = 8,
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

  final String url;
  final ConnectionOptions options;
  final GraphBinaryReader _reader;
  final GraphBinaryWriter _writer;
  late final io.HttpClient _httpClient;

  bool isOpen = true;

  Connection(this.url, [ConnectionOptions? options])
      : options = options ?? const ConnectionOptions(),
        _reader = GraphBinaryReader(),
        _writer = GraphBinaryWriter() {
    _httpClient = io.HttpClient()
      ..idleTimeout = this.options.idleTimeout
      ..maxConnectionsPerHost = this.options.maxConnectionsPerHost;
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

  // ---------------------------------------------------------------------------
  // HTTP transport — uses dart:io directly so we can handle HTTP trailers that
  // TinkerPop's Netty server appends after the final 0\r\n chunk. Dart's built-in
  // HTTP parser throws HttpException when it sees trailer bytes; we swallow that
  // error after the body has been fully buffered.
  // ---------------------------------------------------------------------------

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

    Map<String, dynamic> req = {
      'url': url,
      'method': 'POST',
      'headers': reqHeaders,
      'body': body,
    };

    for (final interceptor in options.interceptors) {
      req = await interceptor(req);
    }

    final uri = Uri.parse(req['url'] as String);
    final finalHeaders = Map<String, String>.from(req['headers'] as Map);
    final finalBody = req['body'] as Uint8List;

    final ioReq = await _httpClient.postUrl(uri);
    finalHeaders.forEach((k, v) => ioReq.headers.set(k, v));
    ioReq.add(finalBody);

    final ioResp = await ioReq.close();
    final statusCode = ioResp.statusCode;
    final contentType = ioResp.headers.contentType?.toString();
    final transactionId = ioResp.headers.value(transactionIdHeader);

    final bodyBytes = BytesBuilder(copy: false);
    bool trailerException = false;
    try {
      await for (final chunk in ioResp) {
        bodyBytes.add(chunk);
      }
    } on io.HttpException catch (_) {
      // TinkerPop Netty sends HTTP trailers (e.g. "code: 200") after the
      // final 0\r\n chunk. Dart's parser throws here. The body is already
      // complete, so we can safely swallow this error.
      if (bodyBytes.isEmpty) rethrow;
      trailerException = true;
    } on StateError catch (_) {
      if (bodyBytes.isEmpty) rethrow;
      trailerException = true;
    }

    final raw = bodyBytes.takeBytes();
    // When the HttpException fires, dart:io may have handed us raw chunked-
    // encoding bytes instead of the decoded payload (the stream yields wire
    // bytes before the codec finishes). Detect and decode manually.
    final decoded = trailerException ? _maybeDecodeChunked(raw) : raw;
    return _RawResponse(statusCode, contentType, transactionId, decoded);
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
      if (response.transactionId != null) 'transactionId': response.transactionId,
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
    _httpClient.close(force: true);
  }

  // Decodes HTTP chunked transfer encoding manually. When dart:io throws an
  // HttpException due to trailing headers, the stream may yield raw wire bytes
  // (chunk-size CRLF chunk-data CRLF ... 0 CRLF) instead of decoded payload.
  // If the buffer doesn't look like chunked encoding, return it unchanged.
  static Uint8List _maybeDecodeChunked(Uint8List raw) {
    // Chunked encoding starts with a hex size followed by \r\n.
    // If the first byte is not a hex digit, it's already decoded.
    if (raw.isEmpty) return raw;
    final first = raw[0];
    final isHex = (first >= 0x30 && first <= 0x39) || // 0-9
        (first >= 0x41 && first <= 0x46) || // A-F
        (first >= 0x61 && first <= 0x66); // a-f
    if (!isHex) return raw;

    final out = BytesBuilder();
    int pos = 0;
    while (pos < raw.length) {
      // Find the \r\n after the chunk size
      int crPos = pos;
      while (crPos < raw.length - 1 &&
          !(raw[crPos] == 0x0D && raw[crPos + 1] == 0x0A)) {
        crPos++;
      }
      if (crPos >= raw.length - 1) break;

      final sizeHex = String.fromCharCodes(raw.sublist(pos, crPos));
      final chunkSize = int.tryParse(sizeHex.trim(), radix: 16);
      if (chunkSize == null) return raw; // not chunked after all
      if (chunkSize == 0) break; // final chunk

      pos = crPos + 2; // skip \r\n
      if (pos + chunkSize > raw.length) {
        // Partial last chunk — take what we have
        out.add(raw.sublist(pos));
        break;
      }
      out.add(raw.sublist(pos, pos + chunkSize));
      pos += chunkSize + 2; // skip chunk data + trailing \r\n
    }

    final result = out.takeBytes();
    return result.isEmpty ? raw : result;
  }

  static String _userAgent() => 'gremlin-dart/0.1.0 Dart/unknown';
}
