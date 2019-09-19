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

'use strict';

/**
 * Represents an error obtained from the server.
 */
class ResponseError extends Error {
  constructor(message, responseStatus) {
    super(message);
    this.name = "ResponseError";

    /**
     * Gets the server status code.
     */
    this.statusCode = responseStatus.code;

    /**
     * Gets the server status message.
     */
    this.statusMessage = responseStatus.message;

    /**
     * Gets the server status attributes as a Map (may contain provider specific status information).
     */
    this.statusAttributes = responseStatus.attributes || {};
  }
}

module.exports = ResponseError;