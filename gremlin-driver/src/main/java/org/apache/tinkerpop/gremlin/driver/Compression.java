/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver;

/**
 * The wire compression algorithm that the driver negotiates with the server. {@link #DEFLATE} is
 * the only algorithm the server negotiates today; other algorithms are reserved for future server-side support.
 * <p>
 * The default is {@link #DEFLATE} (compression on); set {@link #NONE} to disable. Disabling can be worthwhile when
 * {@code deflate} adds CPU and latency with no wire-size benefit, such as on incompressible or tiny payloads.
 */
public enum Compression {

    /**
     * No compression. The driver does not request a compressed response from the server.
     */
    NONE,

    /**
     * Request {@code deflate} compression by sending an {@code Accept-Encoding: deflate} header. The server compresses
     * responses per GraphBinary chunk when this is negotiated.
     */
    DEFLATE
}
