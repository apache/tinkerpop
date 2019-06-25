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
package org.apache.tinkerpop.gremlin.server;

import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.util.function.ThrowingConsumer;

/**
 * Interface for providing commands that websocket requests will respond to.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface OpProcessor extends AutoCloseable {

    /**
     * The name of the processor which requests must refer to "processor" field on a request.
     */
    public String getName();

    /**
     * Initialize the {@code OpProcessor} with settings from the server. This method should only be called once at
     * server startup by a single thread.
     */
    public default void init(final Settings settings) {
        // do nothing by default
    }

    /**
     * Given the context (which contains the RequestMessage), return back a Consumer function that will be
     * executed with the context.  A typical implementation will simply check the "op" field on the RequestMessage
     * and return the Consumer function for that particular operation.
     * @param ctx
     * @return
     */
    public ThrowingConsumer<Context> select(final Context ctx) throws OpProcessorException;
}
