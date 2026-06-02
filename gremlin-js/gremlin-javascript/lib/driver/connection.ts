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
import StreamReader from '../structure/io/binary/internals/StreamReader.js';
import * as utils from '../utils.js';
import ResultSet from './result-set.js';
import {RequestMessage} from "./request-message.js";
import ResponseError from './response-error.js';
import { Traverser } from '../process/traversal.js';

const { graphBinaryReader, graphBinaryWriter } = ioc;

const responseStatusCode = {
  success: 200,
  noContent: 204,
  partialContent: 206,
};

export type HttpRequest = {
  url: string;
  method: string;
  headers: Record<string, string>;
  body: any;
};

export type RequestInterceptor = (request: HttpRequest) => HttpRequest | Promise<HttpRequest>;

export type ConnectionOptions = {
  ca?: string[];
  cert?: string | string[] | Buffer;
  pfx?: string | Buffer;
  preciseNumbers?: boolean;
  reader?: any;
  rejectUnauthorized?: boolean;
  traversalSource?: string;
  writer?: any | null;
  headers?: Record<string, string | string[]>;
  enableUserAgentOnConnect?: boolean;
  agent?: Agent;
  interceptors?: RequestInterceptor | RequestInterceptor[];
  /** Maximum time in milliseconds to wait for a server response before aborting the request. Undefined means no timeout. */
  requestTimeout?: number;
};

/**
 * Represents a single connection to a Gremlin Server.
 */
export default class Connection extends EventEmitter {
  private readonly _reader: any;
  private readonly _writer: any | null;

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

    this._reader = options.reader || (options.preciseNumbers === true ? createPreciseReader() : graphBinaryReader);
    this._writer = 'writer' in options ? options.writer : graphBinaryWriter;
    this.traversalSource = options.traversalSource || 'g';
    this._enableUserAgentOnConnect = options.enableUserAgentOnConnect !== false;

    const interceptors = options.interceptors;
    if (typeof interceptors === 'function') {
      this._interceptors = [interceptors];
    } else if (Array.isArray(interceptors)) {
      this._interceptors = interceptors;
    } else if (interceptors === undefined || interceptors === null) {
      this._interceptors = [];
    } else {
      throw new TypeError('interceptors must be a function, array, or undefined');
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
    const body = this._writer ? this._writer.writeRequest(request) : request;
    const response = await this.#makeHttpRequest(body);
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
    const body = this._writer ? this._writer.writeRequest(request) : request;
    const abortController = new AbortController();

    let response: Response;
    try {
      response = await this.#makeHttpRequest(body, abortController.signal);
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
      yield* this._reader.readResponseStream(streamReader);
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
      return this._reader;
    }

    if (contentType === this._reader.mimeType) {
      return this._reader;
    }

    return null;
  }

  async #makeHttpRequest(body: any, signal?: AbortSignal): Promise<Response> {
    const headers: Record<string, string> = {
      'Accept': this._reader.mimeType
    };

    if (this._writer) {
      headers['Content-Type'] = this._writer.mimeType;
    }

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

    let httpRequest: HttpRequest = {
      url: this.url,
      method: 'POST',
      headers,
      body,
    };

    // Promote transactionId to HTTP header before interceptors run.
    // The field remains in the serialized body as well (dual transmission
    // per the HTTP transaction protocol specification).
    if (body instanceof RequestMessage) {
      const fields = body.getFields();
      if (fields.has('transactionId')) {
        httpRequest.headers['X-Transaction-Id'] = fields.get('transactionId');
      }
    }

    for (let i = 0; i < this._interceptors.length; i++) {
      try {
        httpRequest = await this._interceptors[i](httpRequest);
      } catch (e: any) {
        throw new Error(`Request interceptor at index ${i} failed: ${e.message}`, { cause: e });
      }
    }

    let effectiveSignal = signal;
    const timeoutMs = this.options.requestTimeout;
    let timeoutId: ReturnType<typeof setTimeout> | undefined;
    let timeoutController: AbortController | undefined;

    if (timeoutMs !== undefined) {
      timeoutController = new AbortController();
      timeoutId = setTimeout(() => timeoutController!.abort(new DOMException('TimeoutError', 'TimeoutError')), timeoutMs);
      effectiveSignal = signal ? AbortSignal.any([signal, timeoutController.signal]) : timeoutController.signal;
    }

    try {
      const response = await fetch(httpRequest.url, {
        method: httpRequest.method,
        headers: httpRequest.headers,
        body: httpRequest.body,
        signal: effectiveSignal,
      });
      return response;
    } catch (err: any) {
      if (timeoutController?.signal.aborted) {
        const e = new ResponseError(
          `Request timed out after ${timeoutMs}ms - the server did not respond in time`,
          { code: 598, message: `Request timeout: ${timeoutMs}ms exceeded` },
        );
        e.cause = err;
        throw e;
      }
      const e = new ResponseError(
        'Connection to server closed unexpectedly. Ensure that the server is still reachable and the connection has not been closed by the server or a network device.',
        { code: 599, message: err.message || 'Connection failed' },
      );
      e.cause = err;
      throw e;
    } finally {
      if (timeoutId !== undefined) clearTimeout(timeoutId);
    }
  }

  async #handleResponse(response: Response) {
    let buffer: Buffer;
    try {
      buffer = Buffer.from(await response.arrayBuffer());
    } catch (err: any) {
      const e = new ResponseError(
        'Connection to server closed unexpectedly. Ensure that the server is still reachable and the connection has not been closed by the server or a network device.',
        { code: 599, message: err.message || 'Connection failed' },
      );
      e.cause = err;
      throw e;
    }
    const contentType = response.headers.get("Content-Type");
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
      throw new Error(`Response Content-Type '${contentType}' does not match the configured reader (expected '${this._reader.mimeType}')`);
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
