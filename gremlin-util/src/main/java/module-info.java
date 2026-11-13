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

/**
 * The request and response messages of the Gremlin Server HTTP protocol and the {@code MessageSerializer}
 * implementations that encode them, shared by {@code gremlin-driver} and {@code gremlin-server}. The
 * {@code util.ser.binary} package is internal and not exported.
 */
module org.apache.tinkerpop.gremlin.util {
    requires transitive org.apache.tinkerpop.gremlin.core;
    // MessageSerializer reads and writes ByteBufs
    requires transitive io.netty.buffer;
    // ResponseStatus and ResponseMessage expose HttpResponseStatus
    requires transitive io.netty.codec.http;
    requires io.netty.common;

    exports org.apache.tinkerpop.gremlin.util.message;
    exports org.apache.tinkerpop.gremlin.util.ser;
}
