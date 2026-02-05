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
import * as serializer from '../structure/io/graph-serializer.js';
import * as utils from '../utils.js';
import ResultSet from './result-set.js';
import {RequestMessage} from "./request-message.js";
import {Readable} from "stream";
import ResponseError from './response-error.js';

const { DeferredPromise } = utils;
const { graphBinaryReader, graphBinaryWriter } = ioc;

const responseStatusCode = {
  success: 200,
  noContent: 204,
  partialContent: 206,
  authenticationChallenge: 407,
};

const defaultMimeType = 'application/vnd.gremlin-v4.0+json';
const graphSON2MimeType = 'application/vnd.gremlin-v2.0+json';
const graphBinaryMimeType = 'application/vnd.graphbinary-v1.0';

type MimeType = typeof defaultMimeType | typeof graphSON2MimeType | typeof graphBinaryMimeType;

export type ConnectionOptions = {
  ca?: string[];
  cert?: string | string[] | Buffer;
  mimeType?: MimeType;
  pfx?: string | Buffer;
  reader?: any;
  rejectUnauthorized?: boolean;
  traversalSource?: string;
  writer?: any;
  headers?: Record<string, string | string[]>;
  enableUserAgentOnConnect?: boolean;
  agent?: Agent;
};

/**
 * Represents a single connection to a Gremlin Server.
 */
export default class Connection extends EventEmitter {
  readonly mimeType: MimeType;

  private readonly _reader: any;
  private readonly _writer: any;

  isOpen = true;
  traversalSource: string;

  private readonly _enableUserAgentOnConnect: boolean;

  /**
   * Creates a new instance of {@link Connection}.
   * @param {String} url The resource uri.
   * @param {Object} [options] The connection options.
   * @param {Array} [options.ca] Trusted certificates.
   * @param {String|Array|Buffer} [options.cert] The certificate key.
   * @param {String} [options.mimeType] The mime type to use.
   * @param {String|Buffer} [options.pfx] The private key, certificate, and CA certs.
   * @param {GraphSONReader} [options.reader] The reader to use.
   * @param {Boolean} [options.rejectUnauthorized] Determines whether to verify or not the server certificate.
   * @param {String} [options.traversalSource] The traversal source. Defaults to: 'g'.
   * @param {GraphSONWriter} [options.writer] The writer to use.
   * @param {Object} [options.headers] An associative array containing the additional header key/values for the initial request.
   * @param {Boolean} [options.enableUserAgentOnConnect] Determines if a user agent will be sent during connection handshake. Defaults to: true
   * @param {http.Agent} [options.agent] The http.Agent implementation to use.
   * @constructor
   */
  constructor(
    readonly url: string,
    readonly options: ConnectionOptions = {},
  ) {
    super();

    /**
     * Gets the MIME type.
     * @type {String}
     */
    this.mimeType = options.mimeType || defaultMimeType;
    this._reader = options.reader || this.#getDefaultReader(this.mimeType);
    this._writer = options.writer || this.#getDefaultWriter(this.mimeType);
    this.traversalSource = options.traversalSource || 'g';
    this._enableUserAgentOnConnect = options.enableUserAgentOnConnect !== false;
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
    const request_buf = this._writer.writeRequest(request);
    
    return this.#makeHttpRequest(request_buf)
        .then((response) => {
          return this.#handleResponse(response);
        });
  }

  /** @override */
  stream(request: RequestMessage): Readable {
    throw new Error('stream() is not yet implemented');
  }

  #getDefaultReader(mimeType: MimeType) {
    if (mimeType === graphBinaryMimeType) {
      return graphBinaryReader;
    }

    return mimeType === graphSON2MimeType ? new serializer.GraphSON2Reader() : new serializer.GraphSONReader();
  }

  #getDefaultWriter(mimeType: MimeType) {
    if (mimeType === graphBinaryMimeType) {
      return graphBinaryWriter;
    }

    return mimeType === graphSON2MimeType ? new serializer.GraphSON2Writer() : new serializer.GraphSONWriter();
  }

  #getReaderForContentType(contentType: string | null) {
    if (!contentType) {
      return this._reader;
    }

    if (contentType === graphBinaryMimeType) {
      return graphBinaryReader;
    }

    if (contentType === graphSON2MimeType) {
      return new serializer.GraphSON2Reader();
    }

    if (contentType === defaultMimeType) {
      return new serializer.GraphSONReader();
    }

    return null;
  }

  async #makeHttpRequest(request_buf: Buffer): Promise<Response> {
    const headers: Record<string, string> = {
      'Content-Type': this.mimeType,
      'Accept': this.mimeType
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

    return fetch(this.url, {
      method: 'POST',
      headers,
      body: request_buf,
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
          throw new ResponseError(errorMessage, {
            code: deserialized.status.code,
            message: deserialized.status.message || response.statusText,
            attributes: deserialized.status.attributes || new Map(),
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
      throw new Error(`Unsupported Content-Type: ${contentType}`);
    }

    const deserialized = reader.readResponse(buffer);
    
    if (deserialized.status.code && deserialized.status.code !== 200 && deserialized.status.code !== 204 && deserialized.status.code !== 206) {
      throw new ResponseError(
        `Server returned status ${deserialized.status.code}: ${deserialized.status.message || 'Unknown error'}`,
        {
          code: deserialized.status.code,
          message: deserialized.status.message || '',
          attributes: deserialized.status.attributes || new Map(),
        }
      );
    }

    return new ResultSet(deserialized.result.data, deserialized.result.meta || new Map());
  }

  /**
   * Closes the Connection.
   * @return {Promise}
   */
  close() {
    return Promise.resolve();
  }
}
