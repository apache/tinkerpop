// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

import 'dart:convert';
import 'dart:io' as io;

abstract class AuthOptions {}

class BasicAuth extends AuthOptions {
  final String username;
  final String password;

  BasicAuth({required this.username, required this.password});

  String get headerValue {
    final encoded = base64Encode(utf8.encode('$username:$password'));
    return 'Basic $encoded';
  }
}

// ---------------------------------------------------------------------------
// AWS SigV4 auth
// ---------------------------------------------------------------------------

/// Immutable AWS credentials used for SigV4 request signing.
class AwsCredentials {
  final String accessKeyId;
  final String secretAccessKey;

  /// Temporary session token (from STS AssumeRole, EC2 instance profile, etc.).
  final String? sessionToken;

  const AwsCredentials({
    required this.accessKeyId,
    required this.secretAccessKey,
    this.sessionToken,
  });
}

/// Source of [AwsCredentials].  Implement to add custom providers (e.g.
/// instance metadata service, credential file, secrets manager).
abstract class AwsCredentialsProvider {
  Future<AwsCredentials> resolve();
}

/// Returns a fixed set of credentials.
class StaticCredentialsProvider implements AwsCredentialsProvider {
  final AwsCredentials _creds;

  const StaticCredentialsProvider(this._creds);

  @override
  Future<AwsCredentials> resolve() async => _creds;
}

/// Reads credentials from environment variables.
///
/// Variables read:
/// - `AWS_ACCESS_KEY_ID`
/// - `AWS_SECRET_ACCESS_KEY`
/// - `AWS_SESSION_TOKEN` (optional)
///
/// Pass a custom [env] map in tests to avoid touching [io.Platform.environment].
class EnvironmentCredentialsProvider implements AwsCredentialsProvider {
  final Map<String, String> _env;

  EnvironmentCredentialsProvider([Map<String, String>? env])
      : _env = env ?? io.Platform.environment;

  @override
  Future<AwsCredentials> resolve() async {
    final id = _env['AWS_ACCESS_KEY_ID'] ?? '';
    final secret = _env['AWS_SECRET_ACCESS_KEY'] ?? '';
    if (id.isEmpty || secret.isEmpty) {
      throw StateError(
          'AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set in the environment');
    }
    return AwsCredentials(
      accessKeyId: id,
      secretAccessKey: secret,
      sessionToken: _env['AWS_SESSION_TOKEN'],
    );
  }
}

/// [AuthOptions] subtype that triggers AWS SigV4 request signing.
///
/// ```dart
/// final cluster = Cluster.build()
///     .addContactPoint('localhost')
///     .port(8182)
///     .auth(SigV4Auth(
///       credentials: StaticCredentialsProvider(AwsCredentials(
///         accessKeyId: 'AKID',
///         secretAccessKey: 'SECRET',
///       )),
///       region: 'service-region',
///       service: 'service-name',
///     ))
///     .create();
/// ```
class SigV4Auth extends AuthOptions {
  final AwsCredentialsProvider credentials;
  final String region;

  /// Service name used in the SigV4 credential scope.  The value is defined by
  /// the SigV4-secured system being called.
  final String service;

  SigV4Auth({
    required this.credentials,
    required this.region,
    required this.service,
  });
}
