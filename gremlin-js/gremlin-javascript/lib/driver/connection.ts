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
import type { Dispatcher } from 'undici';
import ioc, { createPreciseReader } from '../structure/io/binary/GraphBinary.js';
import GraphBinaryReader from '../structure/io/binary/internals/GraphBinaryReader.js';
import StreamReader from '../structure/io/binary/internals/StreamReader.js';
import * as utils from '../utils.js';
import ResultSet from './result-set.js';
import {RequestMessage} from "./request-message.js";
import { HttpRequest, RequestInterceptor } from './http-request.js';
import ResponseError from './response-error.js';
import { Traverser } from '../process/traversal.js';
import { buildDispatcher } from './dispatcher.js';
import { Logger, LoggerCallback, normalizeLogger } from './logger.js';

const responseStatusCode = {
  success: 200,
  noContent: 204,
  partialContent: 206,
};

/**
 * Selects the content encoding requested from, and decoded for, the server. `'deflate'` (the
 * default) sends an `Accept-Encoding: deflate` header and decodes deflate responses; `'none'`
 * turns compression off.
 */
export type Compression = 'none' | 'deflate';

export type ConnectionOptions = {
  preciseNumbers?: boolean;
  pdtRegistry?: any;
  reader?: any;
  traversalSource?: string;
  enableUserAgentOnConnect?: boolean;
  /** Maximum number of concurrent connections per origin. Defaults to 128. */
  maxConnections?: number;
  /** Idle-read (body) timeout in milliseconds, applied to the default dispatcher. */
  readTimeoutMillis?: number;
  /** Maximum size of the response headers in bytes, applied to the default dispatcher. */
  maxResponseHeaderBytes?: number;
  /**
   * Idle time in milliseconds before TCP keep-alive probes begin on a connection. Defaults to
   * 30000 (30s) when unset. Set to `0` to disable keep-alive entirely.
   */
  keepAliveTimeMillis?: number;
  /** HTTP proxy URI. When set, requests are routed through an undici `ProxyAgent`. */
  proxy?: string;
  /** Response compression codec. Defaults to `'deflate'` (on). */
  compression?: Compression;
  /** Connection-level default that fills a request's `batchSize` when it is left unset. Defaults to 64. */
  batchSize?: number;
  /** Connection-level default for `bulkResults`, applied to every request unless overridden per-request. Defaults to `false`. */
  bulkResults?: boolean;
  /** An optional logger (a logger object or a callback). Logging is disabled when unset. */
  logger?: Logger;
  interceptors?: RequestInterceptor | RequestInterceptor[];
  /** An optional auth interceptor. As a convenience, this is always appended to the end of the
   *  interceptor list so it runs last, after any user interceptors have modified the request. */
  auth?: RequestInterceptor;
};

/** The default per-request batch size used when neither the request nor the connection sets one. */
export const DEFAULT_BATCH_SIZE = 64;

/**
 * Represents a single connection to a Gremlin Server.
 */
export default class Connection extends EventEmitter {
  private readonly _reader: any;

  isOpen = true;
  traversalSource: string;

  private readonly _enableUserAgentOnConnect: boolean;
  private readonly _interceptors: RequestInterceptor[];
  private readonly _dispatcher: Dispatcher | undefined;
  private readonly _compression: Compression;
  private readonly _batchSize: number;
  private readonly _bulkResults: boolean;
  private readonly _log: LoggerCallback;

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

    this._reader = options.reader || (options.preciseNumbers === true ? createPreciseReader() : new GraphBinaryReader(ioc));
    if (options.pdtRegistry) {
      this._reader.pdtRegistry = options.pdtRegistry;
    }
    this.traversalSource = options.traversalSource || 'g';
    this._enableUserAgentOnConnect = options.enableUserAgentOnConnect !== false;
    this._log = normalizeLogger(options.logger);

    if (options.compression === undefined) {
      this._compression = 'deflate';
    } else if (options.compression === 'none' || options.compression === 'deflate') {
      this._compression = options.compression;
    } else {
      throw new TypeError(`compression must be 'none' or 'deflate'`);
    }

    this._batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE;
    this._bulkResults = options.bulkResults ?? false;

    // The driver builds and owns the undici dispatcher from the discrete options.
    // TLS is configured through the Node/undici runtime (e.g. NODE_EXTRA_CA_CERTS),
    // not through a driver option.
    this._dispatcher = buildDispatcher({
      maxConnections: options.maxConnections,
      readTimeoutMillis: options.readTimeoutMillis,
      maxResponseHeaderBytes: options.maxResponseHeaderBytes,
      keepAliveTimeMillis: options.keepAliveTimeMillis,
      proxy: options.proxy,
    });

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

    this._log('debug', `Connection created for ${this.url}`);
  }

  /**
   * The connection-level default batch size, used to fill a request's `batchSize` when unset.
   */
  get batchSize(): number {
    return this._batchSize;
  }

  /**
   * The connection-level default for `bulkResults`, applied to every request unless overridden
   * per-request. Defaults to `false`.
   */
  get bulkResults(): boolean {
    return this._bulkResults;
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
      // 204 No Content - nothing to yield
      return;
    }

    const streamReader = StreamReader.fromReadableStream(response.body);

    let completed = false;
    try {
      yield* this._reader.readResponseStream(streamReader);
      completed = true;
    } finally {
      if (!completed) {
        // Consumer broke out early or an error occurred - abort to release the connection
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

  async #makeHttpRequest(request: RequestMessage, signal?: AbortSignal): Promise<Response> {
    const headers: Record<string, string> = {
      'Accept': this._reader.mimeType,
    };

    if (this._compression === 'deflate') {
      headers['Accept-Encoding'] = 'deflate';
    }

    if (this._enableUserAgentOnConnect) {
      const userAgent = await utils.getUserAgent();
      if (userAgent !== undefined) {
        headers[utils.getUserAgentHeader()] = userAgent;
      }
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

    this._log('debug', `Sending ${httpRequest.method} request to ${httpRequest.url}`);

    return fetch(httpRequest.url, {
      method: httpRequest.method,
      headers: httpRequest.headers,
      body: httpRequest.body,
      signal,
      // Node only: the undici dispatcher carries the connection-pool options. In the browser
      // it is undefined and the field is omitted, letting the user agent manage the transport.
      ...(this._dispatcher ? { dispatcher: this._dispatcher } : {}),
    } as RequestInit);
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
  async close() {
    this._log('debug', `Closing connection to ${this.url}`);
    await this._dispatcher?.close();
  }
}
