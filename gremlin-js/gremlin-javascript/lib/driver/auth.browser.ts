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

/**
 * Signs requests with AWS SigV4, e.g. for Amazon Neptune. Unlike the Node build, `credentialsProvider`
 * is required: there is no safe default credentials chain to fall back to in a browser (no
 * `~/.aws/credentials`, no environment, no instance metadata). Callers must supply their own
 * short-lived, narrowly-scoped credentials - e.g. from an STS `AssumeRole` call, a Cognito Identity
 * Pool, or a token-vending endpoint on their own backend - since anything reachable from this
 * function is visible to the browser it runs in for as long as that credential is valid.
 */
export function sigv4(region: string, service: string, credentialsProvider: AwsCredentialsProvider): RequestInterceptor {
  if (typeof credentialsProvider !== 'function') {
    throw new Error(
      'sigv4() requires a credentialsProvider in the browser: there is no default credentials chain ' +
      '(no ~/.aws/credentials, no environment, no instance metadata) to fall back to. Supply a function ' +
      'that returns your own short-lived, scoped credentials (e.g. from STS, Cognito, or your backend).',
    );
  }

  let signer: any;

  return async (request: HttpRequest) => {
    request.serializeBody();

    // Lazy-initialize the signer on first use.
    if (!signer) {
      const { SignatureV4 } = await import('@smithy/signature-v4');
      const { Sha256 } = await import('@aws-crypto/sha256-browser');

      signer = new SignatureV4({
        service,
        region,
        sha256: Sha256,
        credentials: () => Promise.resolve(credentialsProvider()),
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
        host: url.host,
      },
      body: request.body,
    });

    request.headers = { ...request.headers, ...signed.headers };
  };
}
