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
import ioc from '../structure/io/binary/GraphBinary.js';
import * as utils from '../utils.js';
import ResultSet from './result-set.js';
import {RequestMessage} from "./request-message.js";
import {Readable} from "stream";
import ResponseError from './response-error.js';
import { Traverser } from '../process/traversal.js';

const { DeferredPromise } = utils;
const { graphBinaryReader, graphBinaryWriter } = ioc;

const responseStatusCode = {
  success: 200,
  noContent: 204,
  partialContent: 206,
  authenticationChallenge: 407,
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
  reader?: any;
  rejectUnauthorized?: boolean;
  traversalSource?: string;
  writer?: any | null;
  headers?: Record<string, string | string[]>;
  enableUserAgentOnConnect?: boolean;
  agent?: Agent;
  interceptors?: RequestInterceptor | RequestInterceptor[];
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
   * @param {Object} [options] The connection options.
   * @param {Array} [options.ca] Trusted certificates.
   * @param {String|Array|Buffer} [options.cert] The certificate key.
   * @param {String|Buffer} [options.pfx] The private key, certificate, and CA certs.
   * @param {GraphBinaryReader} [options.reader] The reader to use.
   * @param {Boolean} [options.rejectUnauthorized] Determines whether to verify or not the server certificate.
   * @param {String} [options.traversalSource] The traversal source. Defaults to: 'g'.
   * @param {GraphBinaryWriter} [options.writer] The writer to use. Set to null to skip serialization.
   * @param {Object} [options.headers] An associative array containing the additional header key/values for the initial request.
   * @param {Boolean} [options.enableUserAgentOnConnect] Determines if a user agent will be sent during connection handshake. Defaults to: true
   * @param {http.Agent} [options.agent] The http.Agent implementation to use.
   * @param {RequestInterceptor|RequestInterceptor[]} [options.interceptors] One or more request interceptors to apply before each HTTP request.
   * @constructor
   */
  constructor(
    readonly url: string,
    readonly options: ConnectionOptions = {},
  ) {
    super();

    this._reader = options.reader || graphBinaryReader;
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

  /** @override */
  submit(request: RequestMessage) {
    // The user may not want the body to be serialized if they are using an interceptor.
    const body = this._writer ? this._writer.writeRequest(request) : request;
    
    return this.#makeHttpRequest(body)
        .then((response) => {
          return this.#handleResponse(response);
        });
  }

  /** @override */
  stream(request: RequestMessage): Readable {
    throw new Error('stream() is not yet implemented');
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

  async #makeHttpRequest(body: any): Promise<Response> {
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

    for (let i = 0; i < this._interceptors.length; i++) {
      try {
        httpRequest = await this._interceptors[i](httpRequest);
      } catch (e: any) {
        throw new Error(`Request interceptor at index ${i} failed: ${e.message}`, { cause: e });
      }
    }

    return fetch(httpRequest.url, {
      method: httpRequest.method,
      headers: httpRequest.headers,
      body: httpRequest.body,
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
          const deserialized = reader.readResponse(buffer);
          const attributes = new Map();
          if (deserialized.status.exception) {
            attributes.set('exceptions', deserialized.status.exception);
            attributes.set('stackTrace', deserialized.status.exception);
          }
          throw new ResponseError(errorMessage, {
            code: deserialized.status.code,
            message: deserialized.status.message || response.statusText,
            attributes: attributes,
          });
        } else if (contentType === 'application/json') {
          const errorBody = JSON.parse(buffer.toString());
          throw new ResponseError(errorMessage, {
            code: response.status,
            message: errorBody.message || errorBody.error || response.statusText,
            attributes: new Map(),
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
        attributes: new Map(),
      });
    }

    if (!reader) {
      throw new Error(`Response Content-Type '${contentType}' does not match the configured reader (expected '${this._reader.mimeType}')`);
    }

    const deserialized = reader.readResponse(buffer);
    
    if (deserialized.status.code && deserialized.status.code !== 200 && deserialized.status.code !== 204 && deserialized.status.code !== 206) {
      const attributes = new Map();
      if (deserialized.status.exception) {
        attributes.set('exceptions', deserialized.status.exception);
        attributes.set('stackTrace', deserialized.status.exception);
      }
      throw new ResponseError(
        `Server error: ${deserialized.status.message || 'Unknown error'} (${deserialized.status.code})`,
        {
          code: deserialized.status.code,
          message: deserialized.status.message || '',
          attributes: attributes,
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
