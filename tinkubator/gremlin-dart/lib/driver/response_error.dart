// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

class ResponseError implements Exception {
  final String message;
  final int? statusCode;
  final String? serverMessage;
  final String? exception;

  const ResponseError(
    this.message, {
    this.statusCode,
    this.serverMessage,
    this.exception,
  });

  @override
  String toString() {
    final parts = [message];
    if (statusCode != null) parts.add('status=$statusCode');
    if (serverMessage != null) parts.add('serverMessage=$serverMessage');
    if (exception != null) parts.add('exception=$exception');
    return 'ResponseError(${parts.join(', ')})';
  }
}
