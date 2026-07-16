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
import 'dart:typed_data';

import 'package:dio/dio.dart';
import 'package:test/test.dart';

import '../../lib/driver/auth.dart';
import '../../lib/driver/sigv4.dart';

// Captures RequestOptions just before the network call, returns an empty 200.
class _CapturingAdapter implements HttpClientAdapter {
  RequestOptions? captured;

  @override
  Future<ResponseBody> fetch(RequestOptions options,
      Stream<Uint8List>? requestStream, Future<dynamic>? cancelFuture) async {
    captured = options;
    if (requestStream != null) await requestStream.drain<void>();
    return ResponseBody.fromBytes(Uint8List(0), 200);
  }

  @override
  void close({bool force = false}) {}
}

void main() {
  // -------------------------------------------------------------------------
  // AwsCredentials
  // -------------------------------------------------------------------------
  group('AwsCredentials', () {
    test('stores all fields', () {
      const creds = AwsCredentials(
        accessKeyId: 'AKID',
        secretAccessKey: 'SECRET',
        sessionToken: 'TOKEN',
      );
      expect(creds.accessKeyId, 'AKID');
      expect(creds.secretAccessKey, 'SECRET');
      expect(creds.sessionToken, 'TOKEN');
    });

    test('sessionToken defaults to null', () {
      const creds = AwsCredentials(accessKeyId: 'K', secretAccessKey: 'S');
      expect(creds.sessionToken, isNull);
    });
  });

  // -------------------------------------------------------------------------
  // StaticCredentialsProvider
  // -------------------------------------------------------------------------
  group('StaticCredentialsProvider', () {
    test('resolves to the stored credentials instance', () async {
      const creds = AwsCredentials(accessKeyId: 'K', secretAccessKey: 'S');
      expect(await StaticCredentialsProvider(creds).resolve(), same(creds));
    });
  });

  // -------------------------------------------------------------------------
  // EnvironmentCredentialsProvider
  // -------------------------------------------------------------------------
  group('EnvironmentCredentialsProvider', () {
    test('resolves all three vars from the provided env map', () async {
      final p = EnvironmentCredentialsProvider({
        'AWS_ACCESS_KEY_ID': 'ENVKID',
        'AWS_SECRET_ACCESS_KEY': 'ENVSECRET',
        'AWS_SESSION_TOKEN': 'ENVTOKEN',
      });
      final c = await p.resolve();
      expect(c.accessKeyId, 'ENVKID');
      expect(c.secretAccessKey, 'ENVSECRET');
      expect(c.sessionToken, 'ENVTOKEN');
    });

    test('sessionToken is null when AWS_SESSION_TOKEN absent', () async {
      final p = EnvironmentCredentialsProvider({
        'AWS_ACCESS_KEY_ID': 'K',
        'AWS_SECRET_ACCESS_KEY': 'S',
      });
      expect((await p.resolve()).sessionToken, isNull);
    });

    test('throws StateError when access key id missing', () {
      final p = EnvironmentCredentialsProvider({'AWS_SECRET_ACCESS_KEY': 'S'});
      expect(p.resolve(), throwsA(isA<StateError>()));
    });

    test('throws StateError when secret access key missing', () {
      final p = EnvironmentCredentialsProvider({'AWS_ACCESS_KEY_ID': 'K'});
      expect(p.resolve(), throwsA(isA<StateError>()));
    });

    test('throws StateError when both keys missing', () {
      expect(EnvironmentCredentialsProvider({}).resolve(),
          throwsA(isA<StateError>()));
    });
  });

  // -------------------------------------------------------------------------
  // SigV4Signer.encode
  // -------------------------------------------------------------------------
  group('SigV4Signer.encode', () {
    test('leaves unreserved chars unchanged', () {
      expect(SigV4Signer.encode('ABCxyz012-_.~'), 'ABCxyz012-_.~');
    });

    test('encodes space as %20', () {
      expect(SigV4Signer.encode('hello world'), 'hello%20world');
    });

    test('encodes forward slash', () {
      expect(SigV4Signer.encode('/a/b'), '%2Fa%2Fb');
    });

    test('encodes plus sign', () {
      expect(SigV4Signer.encode('a+b'), 'a%2Bb');
    });

    test('encodes exclamation mark (not SigV4 unreserved)', () {
      expect(SigV4Signer.encode('a!b'), 'a%21b');
    });

    test('encodes multibyte UTF-8 correctly', () {
      // 'é' = UTF-8 bytes 0xC3 0xA9
      expect(SigV4Signer.encode('é'), '%C3%A9');
    });
  });

  // -------------------------------------------------------------------------
  // SigV4Signer date formatting
  // -------------------------------------------------------------------------
  group('SigV4Signer date formatting', () {
    test('formatDate produces yyyyMMdd', () {
      expect(SigV4Signer.formatDate(DateTime.utc(2024, 1, 5)), '20240105');
    });

    test('formatDate zero-pads single-digit month and day', () {
      expect(SigV4Signer.formatDate(DateTime.utc(2024, 3, 9)), '20240309');
    });

    test('formatDateTime produces yyyyMMddTHHmmssZ', () {
      expect(
        SigV4Signer.formatDateTime(DateTime.utc(2024, 1, 5, 8, 3, 7)),
        '20240105T080307Z',
      );
    });
  });

  // -------------------------------------------------------------------------
  // SigV4Signer.hexSha256
  // -------------------------------------------------------------------------
  group('SigV4Signer.hexSha256', () {
    test('empty input produces well-known SHA-256', () {
      expect(
        SigV4Signer.hexSha256([]),
        'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
      );
    });

    test('returns 64 lowercase hex chars', () {
      final h = SigV4Signer.hexSha256([1, 2, 3]);
      expect(h.length, 64);
      expect(h, matches(r'^[a-f0-9]+$'));
    });
  });

  // -------------------------------------------------------------------------
  // SigV4Signer.deriveSigningKey
  // -------------------------------------------------------------------------
  group('SigV4Signer.deriveSigningKey', () {
    test('produces exactly 32 bytes (256 bits)', () {
      final k = SigV4Signer.deriveSigningKey(
          'wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY', '20150830', 'us-east-1', 'iam');
      expect(k.length, 32);
    });

    test('is deterministic for same inputs', () {
      final k1 = SigV4Signer.deriveSigningKey('S', '20240101', 'us-east-1', 'neptune-db');
      final k2 = SigV4Signer.deriveSigningKey('S', '20240101', 'us-east-1', 'neptune-db');
      expect(k1, equals(k2));
    });

    test('differs when date changes', () {
      final k1 = SigV4Signer.deriveSigningKey('S', '20240101', 'us-east-1', 'neptune-db');
      final k2 = SigV4Signer.deriveSigningKey('S', '20240102', 'us-east-1', 'neptune-db');
      expect(k1, isNot(equals(k2)));
    });

    test('differs when region changes', () {
      final k1 = SigV4Signer.deriveSigningKey('S', '20240101', 'us-east-1', 'neptune-db');
      final k2 = SigV4Signer.deriveSigningKey('S', '20240101', 'eu-west-1', 'neptune-db');
      expect(k1, isNot(equals(k2)));
    });
  });

  // -------------------------------------------------------------------------
  // SigV4Signer.sign
  // -------------------------------------------------------------------------
  group('SigV4Signer.sign', () {
    final signer = SigV4Signer(region: 'us-east-1', service: 'neptune-db');
    final uri = Uri.parse(
        'https://db.cluster-xyz.us-east-1.neptune.amazonaws.com:8182/gremlin');
    final fixedNow = DateTime.utc(2024, 1, 15, 12, 0, 0);
    final headers = <String, dynamic>{
      'Content-Type': 'application/vnd.graphbinary-v4.0'
    };

    test('x-amz-date matches the fixed timestamp', () {
      final r = signer.sign(
        method: 'POST',
        uri: uri,
        headers: headers,
        body: Uint8List(0),
        accessKeyId: 'AKID',
        secretAccessKey: 'SECRET',
        now: fixedNow,
      );
      expect(r['x-amz-date'], '20240115T120000Z');
    });

    test('Authorization starts with AWS4-HMAC-SHA256', () {
      final r = signer.sign(
        method: 'POST',
        uri: uri,
        headers: headers,
        body: Uint8List(0),
        accessKeyId: 'AKIDEXAMPLE',
        secretAccessKey: 'wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY',
        now: fixedNow,
      );
      expect(r['Authorization'], startsWith('AWS4-HMAC-SHA256 '));
    });

    test('Authorization Credential scope is correct', () {
      final r = signer.sign(
        method: 'POST',
        uri: uri,
        headers: headers,
        body: Uint8List(0),
        accessKeyId: 'AKIDEXAMPLE',
        secretAccessKey: 'SECRET',
        now: fixedNow,
      );
      expect(r['Authorization'],
          contains('Credential=AKIDEXAMPLE/20240115/us-east-1/neptune-db/aws4_request,'));
    });

    test('Authorization SignedHeaders includes content-type;host;x-amz-date', () {
      final r = signer.sign(
        method: 'POST',
        uri: uri,
        headers: headers,
        body: Uint8List(0),
        accessKeyId: 'AKID',
        secretAccessKey: 'SECRET',
        now: fixedNow,
      );
      expect(r['Authorization'],
          contains('SignedHeaders=content-type;host;x-amz-date,'));
    });

    test('Signature component is 64 lowercase hex chars', () {
      final r = signer.sign(
        method: 'POST',
        uri: uri,
        headers: headers,
        body: Uint8List(0),
        accessKeyId: 'AKID',
        secretAccessKey: 'SECRET',
        now: fixedNow,
      );
      final m = RegExp(r'Signature=([a-f0-9]+)$').firstMatch(r['Authorization']!);
      expect(m, isNotNull);
      expect(m!.group(1)!.length, 64);
    });

    test('signing is deterministic for same inputs', () {
      final r1 = signer.sign(
        method: 'POST',
        uri: uri,
        headers: headers,
        body: Uint8List(0),
        accessKeyId: 'AKID',
        secretAccessKey: 'SECRET',
        now: fixedNow,
      );
      final r2 = signer.sign(
        method: 'POST',
        uri: uri,
        headers: headers,
        body: Uint8List(0),
        accessKeyId: 'AKID',
        secretAccessKey: 'SECRET',
        now: fixedNow,
      );
      expect(r1['Authorization'], equals(r2['Authorization']));
    });

    test('different bodies produce different signatures', () {
      final r1 = signer.sign(
        method: 'POST',
        uri: uri,
        headers: headers,
        body: Uint8List.fromList([1, 2, 3]),
        accessKeyId: 'AKID',
        secretAccessKey: 'SECRET',
        now: fixedNow,
      );
      final r2 = signer.sign(
        method: 'POST',
        uri: uri,
        headers: headers,
        body: Uint8List.fromList([4, 5, 6]),
        accessKeyId: 'AKID',
        secretAccessKey: 'SECRET',
        now: fixedNow,
      );
      expect(r1['Authorization'], isNot(equals(r2['Authorization'])));
    });

    test('no session token: x-amz-security-token absent', () {
      final r = signer.sign(
        method: 'POST',
        uri: uri,
        headers: headers,
        body: Uint8List(0),
        accessKeyId: 'AKID',
        secretAccessKey: 'SECRET',
        now: fixedNow,
      );
      expect(r.containsKey('x-amz-security-token'), isFalse);
      expect(r['Authorization'], isNot(contains('x-amz-security-token')));
    });

    test('with session token: x-amz-security-token present and in SignedHeaders', () {
      final r = signer.sign(
        method: 'POST',
        uri: uri,
        headers: headers,
        body: Uint8List(0),
        accessKeyId: 'AKID',
        secretAccessKey: 'SECRET',
        sessionToken: 'MYTOKEN',
        now: fixedNow,
      );
      expect(r['x-amz-security-token'], 'MYTOKEN');
      expect(r['Authorization'],
          contains('content-type;host;x-amz-date;x-amz-security-token'));
    });

    test('content-type lookup is case-insensitive', () {
      final r = signer.sign(
        method: 'POST',
        uri: uri,
        headers: {'content-type': 'application/vnd.graphbinary-v4.0'},
        body: Uint8List(0),
        accessKeyId: 'AKID',
        secretAccessKey: 'SECRET',
        now: fixedNow,
      );
      expect(r['Authorization'], contains('content-type'));
    });
  });

  // -------------------------------------------------------------------------
  // SigV4Interceptor (via _CapturingAdapter)
  // -------------------------------------------------------------------------
  group('SigV4Interceptor', () {
    Dio _buildDio(SigV4Auth auth, _CapturingAdapter adapter) {
      final dio = Dio(BaseOptions(validateStatus: (_) => true));
      dio.httpClientAdapter = adapter;
      dio.interceptors.add(SigV4Interceptor(auth));
      return dio;
    }

    final auth = SigV4Auth(
      credentials: StaticCredentialsProvider(const AwsCredentials(
        accessKeyId: 'AKIDEXAMPLE',
        secretAccessKey: 'wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY',
      )),
      region: 'us-east-1',
      service: 'neptune-db',
    );

    test('adds Authorization header', () async {
      final adapter = _CapturingAdapter();
      final dio = _buildDio(auth, adapter);
      await dio.post<void>(
        'https://cluster.us-east-1.neptune.amazonaws.com:8182/gremlin',
        data: Uint8List(0),
        options: Options(
          headers: {'Content-Type': 'application/vnd.graphbinary-v4.0'},
          responseType: ResponseType.bytes,
        ),
      );
      final captured = adapter.captured!.headers;
      expect(captured.containsKey('Authorization'), isTrue);
      expect(captured['Authorization'].toString(), startsWith('AWS4-HMAC-SHA256 '));
    });

    test('adds x-amz-date header with correct format', () async {
      final adapter = _CapturingAdapter();
      final dio = _buildDio(auth, adapter);
      await dio.post<void>(
        'https://cluster.us-east-1.neptune.amazonaws.com:8182/gremlin',
        data: Uint8List(0),
        options: Options(
          headers: {'Content-Type': 'application/vnd.graphbinary-v4.0'},
          responseType: ResponseType.bytes,
        ),
      );
      final date = adapter.captured!.headers['x-amz-date'].toString();
      expect(date, matches(r'^\d{8}T\d{6}Z$'));
    });

    test('no session token: x-amz-security-token absent', () async {
      final adapter = _CapturingAdapter();
      final dio = _buildDio(auth, adapter);
      await dio.post<void>(
        'https://cluster.us-east-1.neptune.amazonaws.com:8182/gremlin',
        data: Uint8List(0),
        options: Options(
          headers: {'Content-Type': 'application/vnd.graphbinary-v4.0'},
          responseType: ResponseType.bytes,
        ),
      );
      expect(
          adapter.captured!.headers.containsKey('x-amz-security-token'), isFalse);
    });

    test('with session token: x-amz-security-token forwarded', () async {
      final authWithToken = SigV4Auth(
        credentials: StaticCredentialsProvider(const AwsCredentials(
          accessKeyId: 'AKID',
          secretAccessKey: 'SECRET',
          sessionToken: 'SESS_TOKEN_XYZ',
        )),
        region: 'us-east-1',
        service: 'neptune-db',
      );
      final adapter = _CapturingAdapter();
      final dio = _buildDio(authWithToken, adapter);
      await dio.post<void>(
        'https://cluster.us-east-1.neptune.amazonaws.com:8182/gremlin',
        data: Uint8List(0),
        options: Options(
          headers: {'Content-Type': 'application/vnd.graphbinary-v4.0'},
          responseType: ResponseType.bytes,
        ),
      );
      expect(
          adapter.captured!.headers['x-amz-security-token'], 'SESS_TOKEN_XYZ');
    });

    test('rejects request when credentials provider throws', () async {
      final badAuth = SigV4Auth(
        credentials: EnvironmentCredentialsProvider({}),
        region: 'us-east-1',
      );
      final adapter = _CapturingAdapter();
      final dio = _buildDio(badAuth, adapter);
      await expectLater(
        dio.post<void>(
          'https://cluster.us-east-1.neptune.amazonaws.com:8182/gremlin',
          data: Uint8List(0),
        ),
        throwsA(isA<DioException>()),
      );
    });
  });

  // -------------------------------------------------------------------------
  // SigV4Auth defaults
  // -------------------------------------------------------------------------
  group('SigV4Auth', () {
    test('default service is neptune-db', () {
      final auth = SigV4Auth(
        credentials: StaticCredentialsProvider(const AwsCredentials(
          accessKeyId: 'K',
          secretAccessKey: 'S',
        )),
        region: 'us-east-1',
      );
      expect(auth.service, 'neptune-db');
    });

    test('custom service is stored', () {
      final auth = SigV4Auth(
        credentials: StaticCredentialsProvider(const AwsCredentials(
          accessKeyId: 'K',
          secretAccessKey: 'S',
        )),
        region: 'us-east-1',
        service: 'execute-api',
      );
      expect(auth.service, 'execute-api');
    });
  });

  // -------------------------------------------------------------------------
  // SigV4Signer URI signing: host header and path/query coverage
  // -------------------------------------------------------------------------
  group('SigV4Signer URI signing', () {
    final signer = SigV4Signer(region: 'us-east-1', service: 'neptune-db');
    final now = DateTime.utc(2024, 1, 15, 12, 0, 0);
    final headers = <String, dynamic>{'Content-Type': 'application/json'};

    Map<String, String> _sign(Uri uri) => signer.sign(
          method: 'GET',
          uri: uri,
          headers: headers,
          body: Uint8List(0),
          accessKeyId: 'AKID',
          secretAccessKey: 'SECRET',
          now: now,
        );

    test('HTTPS default port 443 gives same signature as explicit :443', () {
      final r1 = _sign(Uri.parse('https://host.example.com/path'));
      final r2 = _sign(Uri.parse('https://host.example.com:443/path'));
      expect(r1['Authorization'], equals(r2['Authorization']));
    });

    test('non-default port 8182 gives different signature than default port', () {
      final rDefault = _sign(Uri.parse('https://host.example.com/path'));
      final rCustom = _sign(Uri.parse('https://host.example.com:8182/path'));
      expect(rDefault['Authorization'], isNot(equals(rCustom['Authorization'])));
    });

    test('different path segments produce different signatures', () {
      final r1 = _sign(Uri.parse('https://host.example.com/a/b/c'));
      final r2 = _sign(Uri.parse('https://host.example.com/a/b/d'));
      expect(r1['Authorization'], isNot(equals(r2['Authorization'])));
    });

    test('query parameters are included in the signature', () {
      final r1 = _sign(Uri.parse('https://host.example.com/path?foo=bar'));
      final r2 = _sign(Uri.parse('https://host.example.com/path?foo=baz'));
      expect(r1['Authorization'], isNot(equals(r2['Authorization'])));
    });

    test('URI with query differs from URI without query', () {
      final r1 = _sign(Uri.parse('https://host.example.com/path'));
      final r2 = _sign(Uri.parse('https://host.example.com/path?x=1'));
      expect(r1['Authorization'], isNot(equals(r2['Authorization'])));
    });
  });
}
