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
import 'dart:typed_data';

import 'package:crypto/crypto.dart';
import 'package:dio/dio.dart';

import 'auth.dart';

// ---------------------------------------------------------------------------
// SigV4Signer — pure computation, no I/O
// ---------------------------------------------------------------------------

/// Computes AWS Signature Version 4 signing headers for an HTTP request.
///
/// All methods are pure (no side-effects) so the signer is safe to use from
/// multiple isolates.  Pass [now] to [sign] to fix the signing timestamp in
/// tests.
class SigV4Signer {
  final String region;
  final String service;

  const SigV4Signer({required this.region, required this.service});

  /// Returns the signing headers to add to the request.
  ///
  /// The returned map always contains `Authorization` and `x-amz-date`.
  /// `x-amz-security-token` is present only when [sessionToken] is non-null.
  ///
  /// [headers] must contain the `Content-Type` header (case-insensitive lookup
  /// is performed).
  Map<String, String> sign({
    required String method,
    required Uri uri,
    required Map<String, dynamic> headers,
    required Uint8List body,
    required String accessKeyId,
    required String secretAccessKey,
    String? sessionToken,
    DateTime? now,
  }) {
    final dt = (now ?? DateTime.now()).toUtc();
    final amzDate = formatDateTime(dt);
    final dateStamp = formatDate(dt);

    final host = _hostHeader(uri);
    final bodyHash = hexSha256(body);

    // Canonical headers — always sign content-type, host, x-amz-date, and
    // x-amz-security-token when a session token is present.
    final canonMap = <String, String>{
      'content-type': _findContentType(headers),
      'host': host,
      'x-amz-date': amzDate,
    };
    if (sessionToken != null) {
      canonMap['x-amz-security-token'] = sessionToken;
    }

    final sortedKeys = canonMap.keys.toList()..sort();
    final canonicalHeaders = sortedKeys.map((k) => '$k:${canonMap[k]}\n').join();
    final signedHeaders = sortedKeys.join(';');

    final canonUri = _canonUri(uri);
    final canonQuery = _canonQuery(uri.queryParametersAll);

    final canonicalRequest =
        '${method.toUpperCase()}\n$canonUri\n$canonQuery\n'
        '$canonicalHeaders\n$signedHeaders\n$bodyHash';

    final credScope = '$dateStamp/$region/$service/aws4_request';
    final stringToSign = 'AWS4-HMAC-SHA256\n$amzDate\n$credScope\n'
        '${hexSha256(utf8.encode(canonicalRequest))}';

    final signingKey = deriveSigningKey(secretAccessKey, dateStamp, region, service);
    final signature =
        _hexEncode(Hmac(sha256, signingKey).convert(utf8.encode(stringToSign)).bytes);

    final result = <String, String>{
      'x-amz-date': amzDate,
      'Authorization': 'AWS4-HMAC-SHA256 '
          'Credential=$accessKeyId/$credScope,'
          'SignedHeaders=$signedHeaders,'
          'Signature=$signature',
    };
    if (sessionToken != null) {
      result['x-amz-security-token'] = sessionToken;
    }
    return result;
  }

  // ---------------------------------------------------------------------------
  // Public helpers (accessible from tests)
  // ---------------------------------------------------------------------------

  /// URI-encodes [s] using the SigV4 unreserved character set: A-Za-z0-9 - _ . ~
  static String encode(String s) {
    final buf = StringBuffer();
    for (final byte in utf8.encode(s)) {
      if ((byte >= 0x41 && byte <= 0x5A) || // A-Z
          (byte >= 0x61 && byte <= 0x7A) || // a-z
          (byte >= 0x30 && byte <= 0x39) || // 0-9
          byte == 0x2D || // -
          byte == 0x5F || // _
          byte == 0x2E || // .
          byte == 0x7E) { // ~
        buf.writeCharCode(byte);
      } else {
        buf.write('%${byte.toRadixString(16).padLeft(2, '0').toUpperCase()}');
      }
    }
    return buf.toString();
  }

  /// Derives the SigV4 signing key from a secret key, date, region and service.
  static List<int> deriveSigningKey(
      String secretKey, String date, String region, String service) {
    List<int> hmac(List<int> key, List<int> data) =>
        Hmac(sha256, key).convert(data).bytes;
    final kDate = hmac(utf8.encode('AWS4$secretKey'), utf8.encode(date));
    final kRegion = hmac(kDate, utf8.encode(region));
    final kService = hmac(kRegion, utf8.encode(service));
    return hmac(kService, utf8.encode('aws4_request'));
  }

  /// Returns the lowercase hex SHA-256 of [data].
  static String hexSha256(List<int> data) => sha256.convert(data).toString();

  /// Formats [dt] as `yyyyMMdd` (UTC).  Converts to UTC if not already.
  static String formatDate(DateTime dt) {
    final u = dt.toUtc();
    return '${u.year.toString().padLeft(4, '0')}'
        '${u.month.toString().padLeft(2, '0')}'
        '${u.day.toString().padLeft(2, '0')}';
  }

  /// Formats [dt] as `yyyyMMddTHHmmssZ` (UTC).  Converts to UTC if not already.
  static String formatDateTime(DateTime dt) {
    final u = dt.toUtc();
    return '${formatDate(u)}T'
        '${u.hour.toString().padLeft(2, '0')}'
        '${u.minute.toString().padLeft(2, '0')}'
        '${u.second.toString().padLeft(2, '0')}Z';
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  static String _hostHeader(Uri uri) {
    final isDefault = (uri.scheme == 'https' && uri.port == 443) ||
        (uri.scheme == 'http' && uri.port == 80);
    return isDefault ? uri.host : '${uri.host}:${uri.port}';
  }

  // Use pathSegments (decoded) rather than path (percent-encoded) to avoid
  // double-encoding sequences like %20 → %2520.
  static String _canonUri(Uri uri) {
    final segments = uri.pathSegments;
    if (segments.isEmpty) return '/';
    return '/${segments.map(encode).join('/')}';
  }

  // Accepts queryParametersAll so duplicate keys are preserved and sorted
  // correctly per SigV4 §3.4 (each (key, value) pair is a separate entry).
  static String _canonQuery(Map<String, List<String>> params) {
    if (params.isEmpty) return '';
    final pairs = <MapEntry<String, String>>[];
    for (final e in params.entries) {
      for (final v in e.value) {
        pairs.add(MapEntry(e.key, v));
      }
    }
    pairs.sort((a, b) {
      final c = a.key.compareTo(b.key);
      return c != 0 ? c : a.value.compareTo(b.value);
    });
    return pairs.map((e) => '${encode(e.key)}=${encode(e.value)}').join('&');
  }

  static String _findContentType(Map<String, dynamic> headers) {
    for (final entry in headers.entries) {
      if (entry.key.toLowerCase() == 'content-type') {
        return entry.value?.toString().trim() ?? '';
      }
    }
    return '';
  }

  static String _hexEncode(List<int> bytes) =>
      bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).join();
}

// ---------------------------------------------------------------------------
// SigV4Interceptor — Dio interceptor
// ---------------------------------------------------------------------------

/// Dio [Interceptor] that signs every outbound request with AWS SigV4.
///
/// Added automatically by [Connection] when [ConnectionOptions.auth] is a
/// [SigV4Auth].  It runs before [RetryInterceptor] so every retry attempt
/// receives a fresh timestamp and signature.
class SigV4Interceptor extends Interceptor {
  final SigV4Auth _auth;
  final SigV4Signer _signer;

  SigV4Interceptor(this._auth)
      : _signer = SigV4Signer(region: _auth.region, service: _auth.service);

  @override
  Future<void> onRequest(
      RequestOptions options, RequestInterceptorHandler handler) async {
    try {
      final creds = await _auth.credentials.resolve();

      Uint8List bodyBytes;
      final data = options.data;
      if (data == null) {
        bodyBytes = Uint8List(0);
      } else if (data is Uint8List) {
        bodyBytes = data;
      } else if (data is String) {
        bodyBytes = Uint8List.fromList(utf8.encode(data));
      } else {
        // Dio serialises Map/FormData after interceptors run, so the actual
        // wire bytes are unknowable here.  Throw rather than sign an empty body
        // hash that will produce a 403 SignatureDoesNotMatch from the server.
        throw StateError(
            'SigV4Interceptor cannot compute body hash for '
            '${data.runtimeType}. Pre-serialise the body to Uint8List or '
            'String before adding SigV4Interceptor.');
      }

      final signed = _signer.sign(
        method: options.method,
        uri: options.uri,
        headers: Map<String, dynamic>.from(options.headers),
        body: bodyBytes,
        accessKeyId: creds.accessKeyId,
        secretAccessKey: creds.secretAccessKey,
        sessionToken: creds.sessionToken,
      );

      signed.forEach((k, v) => options.headers[k] = v);
      handler.next(options);
    } catch (e) {
      handler.reject(
        e is DioException
            ? e
            : DioException(requestOptions: options, error: e),
        true,
      );
    }
  }
}
