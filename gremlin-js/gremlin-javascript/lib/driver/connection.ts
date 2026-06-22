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

/**
 * @author Jorge Bay Gondra
 */

import { Buffer } from 'buffer';
import { EventEmitter } from 'eventemitter3';
import type { Agent } from 'node:http';
import ioc, { createPreciseReader } from '../structure/io/binary/GraphBinary.js';
import GraphBinaryReader from '../structure/io/binary/internals/GraphBinaryReader.js';
import StreamReader from '../structure/io/binary/internals/StreamReader.js';
import * as utils from '../utils.js';
import ResultSet from './result-set.js';
import {RequestMessage} from "./request-message.js";
import { HttpRequest, RequestInterceptor } from './http-request.js';
import ResponseError from './response-error.js';
import { Traverser } from '../process/traversal.js';

const responseStatusCode = {
  success: 200,
  noContent: 204,
  partialContent: 206,
};

export type ConnectionOptions = {
  ca?: string[];
  cert?: string | string[] | Buffer;
  pfx?: string | Buffer;
  preciseNumbers?: boolean;
  pdtRegistry?: any;
  responseSerializer?: any;
  rejectUnauthorized?: boolean;
  traversalSource?: string;
  headers?: Record<string, string | string[]>;
  enableUserAgentOnConnect?: boolean;
  agent?: Agent;
  interceptors?: RequestInterceptor | RequestInterceptor[];
  /** An optional auth interceptor. As a convenience, this is always appended to the end of the
   *  interceptor list so it runs last, after any user interceptors have modified the request. */
  auth?: RequestInterceptor;
};

/**
 * Represents a single connection to a Gremlin Server.
 */
export default class Connection extends EventEmitter {
  private readonly _responseSerializer: any;

  isOpen = true;
  traversalSource: string;

  private readonly _enableUserAgentOnConnect: boolean;
  private readonly _interceptors: RequestInterceptor[];

  /**
   * Creates a new instance of {@link Connection}.
   * @param {String} url The resource uri.
   * @param {ConnectionOptions} [options] The connection options.
   */
  constructor(
    readonly url: string,
    readonly options: ConnectionOptions = {},
  ) {
    super();

    this._responseSerializer =
      options.responseSerializer || (options.preciseNumbers === true ? createPreciseReader() : new GraphBinaryReader(ioc));
    if (options.pdtRegistry) {
      this._responseSerializer.pdtRegistry = options.pdtRegistry;
    }
    this.traversalSource = options.traversalSource || 'g';
    this._enableUserAgentOnConnect = options.enableUserAgentOnConnect !== false;

    const interceptors = options.interceptors;
    if (typeof interceptors === 'function') {
      this._interceptors = [interceptors];
    } else if (Array.isArray(interceptors)) {
      this._interceptors = [...interceptors];
    } else if (interceptors === undefined || interceptors === null) {
      this._interceptors = [];
    } else {
      throw new TypeError('interceptors must be a function, array, or undefined');
    }

    // Auth interceptor is always last so it runs after user interceptors
    if (options.auth) {
      this._interceptors.push(options.auth);
    }
  }

  /**
   * Opens the connection, if its not already opened.
   * @returns {Promise}
   */
  async open() {
    // No-op for HTTP connections
    return Promise.resolve();
  }

  /**
   * Send a request and buffer the entire response. Returns a Promise<ResultSet>.
   */
  async submit(request: RequestMessage) {
    const response = await this.#makeHttpRequest(request);
    return this.#handleResponse(response);
  }

  /**
   * Send a request and stream the response incrementally.
   * Returns an AsyncGenerator that yields deserialized result items.
   * For bulked responses, yields Traverser objects.
   *
   * In the GraphBinary v4 streaming protocol, the server sends the status after all
   * result data. If the server encounters an error mid-traversal, values yielded before
   * the error are valid partial results. A ResponseError is thrown after the last value
   * has been yielded.
   *
   * @param {RequestMessage} request
   * @returns {AsyncGenerator<any>}
   */
  async *stream(request: RequestMessage): AsyncGenerator<any> {
    const abortController = new AbortController();

    let response: Response;
    try {
      response = await this.#makeHttpRequest(request, abortController.signal);
    } catch (e: any) {
      throw new Error(`Stream request failed: ${e.message}`, { cause: e });
    }

    if (!response.ok) {
      // For error responses, buffer and parse the error body
      const buffer = Buffer.from(await response.arrayBuffer());
      const errorMessage = `Server returned HTTP ${response.status}: ${response.statusText}`;
      const contentType = response.headers.get("Content-Type");
      const reader = this.#getReaderForContentType(contentType);

      try {
        if (reader) {
          const deserialized = await reader.readResponse(buffer);
          throw new ResponseError(errorMessage, {
            code: deserialized.status.code,
            message: deserialized.status.message || response.statusText,
            exception: deserialized.status.exception,
          });
        } else if (contentType === 'application/json') {
          const errorBody = JSON.parse(buffer.toString());
          throw new ResponseError(errorMessage, {
            code: response.status,
            message: errorBody.message || errorBody.error || response.statusText,
          });
        }
      } catch (err) {
        if (err instanceof ResponseError) throw err;
      }

      throw new ResponseError(errorMessage, {
        code: response.status,
        message: response.statusText,
      });
    }

    if (!response.body) {
      // 204 No Content — nothing to yield
      return;
    }

    const streamReader = StreamReader.fromReadableStream(response.body);

    let completed = false;
    try {
      yield* this._responseSerializer.readResponseStream(streamReader);
      completed = true;
    } finally {
      if (!completed) {
        // Consumer broke out early or an error occurred — abort to release the connection
        abortController.abort();
      }
    }
  }

  #getReaderForContentType(contentType: string | null) {
    if (!contentType) {
      return this._responseSerializer;
    }

    if (contentType === this._responseSerializer.mimeType) {
      return this._responseSerializer;
    }

    return null;
  }

  async #makeHttpRequest(request: RequestMessage, signal?: AbortSignal): Promise<Response> {
    const headers: Record<string, string> = {
      'Accept': this._responseSerializer.mimeType,
    };

    if (this._enableUserAgentOnConnect) {
      const userAgent = await utils.getUserAgent();
      if (userAgent !== undefined) {
        headers[utils.getUserAgentHeader()] = userAgent;
      }
    }

    if (this.options.headers) {
      Object.entries(this.options.headers).forEach(([key, value]) => {
        headers[key] = Array.isArray(value) ? value.join(', ') : value;
      });
    }

    const httpRequest = new HttpRequest('POST', this.url, headers, request);

    // Promote transactionId to HTTP header before interceptors run.
    // The field remains in the serialized body as well (dual transmission
    // per the HTTP transaction protocol specification).
    if (request instanceof RequestMessage) {
      const fields = request.getFields();
      if (fields.has('transactionId')) {
        httpRequest.headers['X-Transaction-Id'] = fields.get('transactionId');
      }
    }

    for (let i = 0; i < this._interceptors.length; i++) {
      try {
        await this._interceptors[i](httpRequest);
      } catch (e: any) {
        throw new Error(`Request interceptor at index ${i} failed: ${e.message}`, { cause: e });
      }
    }

    // Auto-serialize body to JSON after interceptors run (idempotent if already serialized)
    httpRequest.serializeBody();

    return fetch(httpRequest.url, {
      method: httpRequest.method,
      headers: httpRequest.headers,
      body: httpRequest.body,
      signal,
    });
  }

  async #handleResponse(response: Response) {
    const contentType = response.headers.get("Content-Type");
    const buffer = Buffer.from(await response.arrayBuffer());
    const reader = this.#getReaderForContentType(contentType);

    if (!response.ok) {
      const errorMessage = `Server returned HTTP ${response.status}: ${response.statusText}`;

      try {
        if (reader) {
          const deserialized = await reader.readResponse(buffer);
          throw new ResponseError(errorMessage, {
            code: deserialized.status.code,
            message: deserialized.status.message || response.statusText,
            exception: deserialized.status.exception,
          });
        } else if (contentType === 'application/json') {
          const errorBody = JSON.parse(buffer.toString());
          throw new ResponseError(errorMessage, {
            code: response.status,
            message: errorBody.message || errorBody.error || response.statusText,
          });
        }
      } catch (err) {
        if (err instanceof ResponseError) {
          throw err;
        }
      }
      
      throw new ResponseError(errorMessage, {
        code: response.status,
        message: response.statusText,
      });
    }

    if (!reader) {
      throw new Error(`Response Content-Type '${contentType}' does not match the configured response serializer (expected '${this._responseSerializer.mimeType}')`);
    }

    const deserialized = await reader.readResponse(buffer);
    
    if (deserialized.status.code && deserialized.status.code !== responseStatusCode.success && deserialized.status.code !== responseStatusCode.noContent && deserialized.status.code !== responseStatusCode.partialContent) {
      throw new ResponseError(
        `Server error: ${deserialized.status.message || 'Unknown error'} (${deserialized.status.code})`,
        {
          code: deserialized.status.code,
          message: deserialized.status.message || '',
          exception: deserialized.status.exception,
        }
      );
    }

    return new ResultSet(
      deserialized.result.bulked
        ? deserialized.result.data.map((item: { v: any; bulk: number }) => new Traverser(item.v, item.bulk))
        : deserialized.result.data,
      new Map(),
    );
  }

  /**
   * Closes the Connection.
   * @return {Promise}
   */
  close() {
    return Promise.resolve();
  }
}
