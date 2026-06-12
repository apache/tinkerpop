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
import { RequestMessage } from './request-message.js';

/**
 * Represents an HTTP request that is passed through interceptors before being sent to the server.
 * Interceptors may mutate any field. The {@link serializeBody} method serializes the body to JSON
 * if it is still a RequestMessage, or returns the existing bytes if already serialized.
 */
export class HttpRequest {
  url: string;
  method: string;
  headers: Record<string, string>;
  body: RequestMessage | Buffer | any;

  constructor(method: string, url: string, headers: Record<string, string>, body: any) {
    this.method = method;
    this.url = url;
    this.headers = headers;
    this.body = body;
  }

  /**
   * Serializes the request body to JSON if it is still a RequestMessage.
   * If body is already a Buffer, returns it as-is (idempotent).
   * Sets Content-Type and Content-Length headers on successful serialization.
   *
   * @returns The serialized body as a Buffer.
   * @throws Error if body is neither a RequestMessage nor a Buffer.
   */
  serializeBody(): Buffer {
    if (Buffer.isBuffer(this.body)) {
      return this.body;
    }

    if (this.body instanceof RequestMessage) {
      // RequestMessage.toJSON() produces the flattened wire object (standard + custom fields).
      const data = Buffer.from(JSON.stringify(this.body), 'utf-8');
      this.body = data;
      this.headers['Content-Type'] = 'application/json';
      this.headers['Content-Length'] = String(data.length);
      return data;
    }

    const typeName = this.body === null ? 'null' : typeof this.body;
    throw new Error(`unsupported body type: ${typeName}`);
  }
}

/**
 * A request interceptor receives an HttpRequest and mutates it in place.
 * Interceptors must not return a value.
 */
export type RequestInterceptor = (request: HttpRequest) => void | Promise<void>;
