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
package org.apache.tinkerpop.gremlin.driver.ser;

import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class SerTokens {
    private SerTokens() {}

    public static final String TOKEN_ATTRIBUTES = "attributes";
    public static final String TOKEN_RESULT = "result";
    public static final String TOKEN_STATUS = "status";
    public static final String TOKEN_DATA = "data";
    public static final String TOKEN_META = "meta";
    public static final String TOKEN_CODE = "code";
    public static final String TOKEN_REQUEST = "requestId";
    public static final String TOKEN_MESSAGE = "message";
    public static final String TOKEN_PROCESSOR = "processor";
    public static final String TOKEN_OP = "op";
    public static final String TOKEN_ARGS = "args";

    public static final String MIME_JSON = "application/json";
    public static final String MIME_GRAPHSON_V1D0 = "application/vnd.gremlin-v1.0+json";
    public static final String MIME_GRAPHSON_V2D0 = "application/vnd.gremlin-v2.0+json";
    public static final String MIME_GRAPHSON_V3D0 = "application/vnd.gremlin-v3.0+json";

    /**
     * @deprecated As of release 3.4.3, replaced by {@link #MIME_GRAPHBINARY_V1D0}.
     */
    @Deprecated
    public static final String MIME_GRYO_V1D0 = "application/vnd.gremlin-v1.0+gryo";

    /**
     * @deprecated As of release 3.4.3, replaced by {@link #MIME_GRAPHBINARY_V1D0}.
     */
    @Deprecated
    public static final String MIME_GRYO_V3D0 = "application/vnd.gremlin-v3.0+gryo";

    /**
     * @deprecated As of release 3.2.6, not directly replaced - supported through {@link HaltedTraverserStrategy}.
     */
    @Deprecated
    public static final String MIME_GRYO_LITE_V1D0 = "application/vnd.gremlin-v1.0+gryo-lite";
    public static final String MIME_GRAPHBINARY_V1D0 = "application/vnd.graphbinary-v1.0";
}
