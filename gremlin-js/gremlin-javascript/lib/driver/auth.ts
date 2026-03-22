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
import type { HttpRequest, RequestInterceptor } from './connection.js';

export function basic(username: string, password: string): RequestInterceptor {
  return (request: HttpRequest) => {
    request.headers['authorization'] = 'Basic ' + Buffer.from(`${username}:${password}`).toString('base64');
    return request;
  };
}

export interface AwsCredentials {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
}

export type AwsCredentialsProvider = () => AwsCredentials | Promise<AwsCredentials>;

export function sigv4(region: string, service: string, credentialsProvider?: AwsCredentialsProvider): RequestInterceptor {
  let signer: any;
  let resolvedProvider: AwsCredentialsProvider;

  return async (request: HttpRequest) => {
    // Lazy-initialize signer and credentials provider on first use
    if (!signer) {
      const { SignatureV4 } = await import('@smithy/signature-v4');
      const { Hash } = await import('@smithy/hash-node');

      if (credentialsProvider) {
        resolvedProvider = credentialsProvider;
      } else {
        const { fromNodeProviderChain } = await import('@aws-sdk/credential-providers');
        const chain = fromNodeProviderChain({ clientConfig: { region } });
        resolvedProvider = () => chain();
      }

      signer = new SignatureV4({
        service,
        region,
        sha256: Hash.bind(null, 'sha256'),
        credentials: () => Promise.resolve(resolvedProvider()),
      });
    }

    const url = new URL(request.url);
    const signed = await signer.sign({
      method: request.method,
      protocol: url.protocol,
      hostname: url.hostname,
      port: url.port ? Number(url.port) : undefined,
      path: url.pathname + url.search,
      headers: {
        ...request.headers,
        host: url.host,
      },
      body: request.body,
    });

    request.headers = signed.headers;
    return request;
  };
}
