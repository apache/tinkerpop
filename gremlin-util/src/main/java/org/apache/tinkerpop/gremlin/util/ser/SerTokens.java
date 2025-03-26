/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.util.ser;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class SerTokens {
    private SerTokens() {}

    public static final String TOKEN_RESULT = "result";
    public static final String TOKEN_STATUS = "status";
    public static final String TOKEN_CODE = "code";
    public static final String TOKEN_DATA = "data";
    public static final String TOKEN_EXCEPTION = "exception";
    public static final String TOKEN_MESSAGE = "message";
    public static final String TOKEN_GREMLIN = "gremlin";
    public static final String TOKEN_LANGUAGE = "language";
    public static final String TOKEN_BINDINGS = "bindings";
    public static final String TOKEN_G = "g";
    public static final String TOKEN_TIMEOUT_MS = "timeoutMs";
    public static final String TOKEN_MATERIALIZE_PROPERTIES = "materializeProperties";

    public static final String MIME_JSON = "application/json";
    public static final String MIME_GRAPHSON_V4 = "application/vnd.gremlin-v4.0+json";
    public static final String MIME_GRAPHSON_V4_UNTYPED = "application/vnd.gremlin-v4.0+json;types=false";
    public static final String MIME_GRAPHBINARY_V4 = "application/vnd.graphbinary-v4.0";
}
