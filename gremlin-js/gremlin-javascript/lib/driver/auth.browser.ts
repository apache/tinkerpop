/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

import { Buffer } from 'buffer';
import type { HttpRequest, RequestInterceptor } from './http-request.js';

export function basic(username: string, password: string): RequestInterceptor {
  return (request: HttpRequest) => {
    request.headers['authorization'] = 'Basic ' + Buffer.from(`${username}:${password}`).toString('base64');
  };
}

export interface AwsCredentials {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
}

export type AwsCredentialsProvider = () => AwsCredentials | Promise<AwsCredentials>;

export function sigv4(
  _region: string,
  _service: string,
  _credentialsProvider?: AwsCredentialsProvider,
): RequestInterceptor {
  throw new Error(
    'AWS SigV4 signing is not supported in the browser: it requires long-lived AWS credentials that ' +
    'must never be exposed to client-side code. Sign requests from a server-side proxy (or use ' +
    'Basic/token auth) for browser deployments instead.',
  );
}
